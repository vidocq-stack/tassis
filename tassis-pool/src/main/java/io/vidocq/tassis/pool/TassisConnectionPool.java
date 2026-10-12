/*
 * Copyright (c) 2026 Yann Blazart, Antoine Sabot-Durand and the Vidocq contributors
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * or any later version, which is available at
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * It is also made available under the European Union Public Licence v. 1.2,
 * which is available at
 * https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
 *
 * SPDX-License-Identifier: EPL-2.0 OR EUPL-1.2 OR GPL-2.0-or-later
 */
package io.vidocq.tassis.pool;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.core.internal.pool.ConnectionSource;
import io.vidocq.tassis.core.internal.pool.PhysicalConnection;
import io.vidocq.tassis.core.internal.pool.Recycler;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link ConnectionFactory} that keeps the connections to the broker open and lends them: a
 * connection that the application closes goes back to the pool, and the next
 * {@code createConnection} or {@code createContext} reuses it instead of opening one. It is
 * worth it for a broker of the network, where a connection is a socket, a handshake and an
 * authentication. Jakarta Messaging does not define pooling; it leaves it to whoever hosts the
 * application.
 *
 * <pre>{@code
 * try (TassisConnectionPool pool = TassisConnectionPool.over(factory).maxSize(8).build()) {
 *     try (JMSContext context = pool.createContext()) {
 *         context.createProducer().send(queue, "hello");
 *     }
 * }
 * }</pre>
 *
 * <p><b>What a borrower gets</b> is a connection as a factory gives it: stopped, with no
 * session, no temporary destination, no exception listener and no client identifier. Closing it
 * closes its sessions, with what that means (§6.1.8: transactions rolled back, messages not
 * acknowledged delivered again), and deletes its temporary destinations.
 *
 * <p><b>Client identifiers.</b> A connection that is given one with {@code setClientID} stops
 * being lent: closing it really closes it, which frees the identifier (§6.1.2). It counts in
 * the size of the pool as long as it is open. A factory that assigns a client identifier
 * itself can only have one connection open, so a pool refuses it.
 *
 * <p><b>Credentials.</b> A borrower only gets a connection opened with the user name and the
 * password it gives. The pool keeps both in memory, for as long as it has such a connection.
 *
 * <p><b>How long a connection is kept.</b> One that nobody has used for {@code idleTimeout} is
 * closed, and so is one older than {@code maxLifetime}, once nobody is using it. A connection
 * that is lent for longer than {@code leakThreshold} is reported in the log, with the call that
 * borrowed it: it was probably never closed. The pool does not take it back.
 *
 * <p><b>Settings.</b> A pool starts from the settings its factory carries, the keys
 * {@code tassis.pool.*} or the URL parameters {@code pool.*}, and the builder changes them.
 *
 * <p>A pool is safe for use by several threads. Close it to close the connections it keeps.
 */
public final class TassisConnectionPool
        implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, AutoCloseable {

    /** How many connections to the broker a pool opens at most, unless told otherwise. */
    public static final int DEFAULT_MAX_SIZE = 10;

    /** How long a borrower waits for a connection of a pool that has none left, unless told otherwise. */
    public static final Duration DEFAULT_ACQUIRE_TIMEOUT = Duration.ofSeconds(5);

    /** How long a connection nobody uses is kept, unless told otherwise. */
    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMinutes(10);

    /** How long a connection is kept at all, unless told otherwise. */
    public static final Duration DEFAULT_MAX_LIFETIME = Duration.ofMinutes(30);

    private static final System.Logger LOG = System.getLogger(TassisConnectionPool.class.getName());

    /** Whom a connection to the broker was opened as. */
    private record Identity(String userName, String password) {
    }

    /** A connection to the broker, lent or waiting to be. Its times are guarded by the lock of the pool. */
    private static final class Entry {

        private final PhysicalConnection physical;
        private final Identity identity;
        private final long openedAt = System.nanoTime();
        private long idleSince;
        private long lentAt;
        // The call that borrowed it, kept only when the pool reports connections not given back.
        private Throwable borrower;
        private boolean reported;

        Entry(PhysicalConnection physical, Identity identity) {
            this.physical = physical;
            this.identity = identity;
        }

        PhysicalConnection physical() {
            return physical;
        }

        Identity identity() {
            return identity;
        }
    }

    /** What a borrower makes of the connection it is lent. */
    @FunctionalInterface
    private interface Lending<T> {

        T lend(PhysicalConnection physical, Recycler recycler) throws JMSException;
    }

    private final ConnectionSource source;
    private final int maxSize;
    private final long acquireTimeoutNanos;
    // Zero for never, all three.
    private final long idleTimeoutNanos;
    private final long maxLifetimeNanos;
    private final long leakThresholdNanos;

    // Guards everything below. Never held while a connection is opened or closed.
    private final ReentrantLock lock = new ReentrantLock();
    // A place or a connection is free, or the pool is closed.
    private final Condition changed = lock.newCondition();
    // The connections nobody is using, the last one given back first.
    private final Map<Identity, Deque<Entry>> idle = new HashMap<>();
    // The connections open or being opened, lent or not.
    private int size;
    // Out of the pool, yet to be closed by whoever leaves the lock next.
    private final List<Entry> doomed = new ArrayList<>();
    private boolean closed;
    // The connections that are lent.
    private final Set<Entry> lent = new HashSet<>();
    // Something the housekeeper watches has changed: a connection came back, went out, or the pool closed.
    private final Condition tick = lock.newCondition();
    // The thread that closes the connections kept too long; null while there is none to watch.
    private Thread housekeeper;

    private TassisConnectionPool(Builder builder) {
        this.source = builder.source;
        this.maxSize = builder.maxSize;
        this.acquireTimeoutNanos = builder.acquireTimeout.toNanos();
        this.idleTimeoutNanos = builder.idleTimeout.toNanos();
        this.maxLifetimeNanos = builder.maxLifetime.toNanos();
        this.leakThresholdNanos = builder.leakThreshold.toNanos();
    }

    /**
     * Starts the description of a pool whose connections are those of that factory: same broker,
     * same settings.
     *
     * @throws IllegalArgumentException if the factory assigns a client identifier
     *         ({@code withClientID}, {@code tassis.clientId}): only one of its connections can
     *         be open at a time
     */
    public static Builder over(TassisConnectionFactory factory) {
        ConnectionSource source = ConnectionSource.of(factory);
        if (source.assignsClientId()) {
            throw new IllegalArgumentException("a factory that assigns a client identifier cannot be pooled: "
                    + "only one of its connections can be open at a time (" + source + ")");
        }
        return new Builder(source);
    }

    /**
     * Whether the connections of that factory are worth a pool and can have one, for whoever
     * builds factories from settings and must decide by itself, as the CDI integration does: the
     * broker is one of the network, where a connection is a socket; the factory assigns no client
     * identifier; and its settings do not say {@code tassis.pool.enabled=false}.
     */
    public static boolean suits(TassisConnectionFactory factory) {
        ConnectionSource source = ConnectionSource.of(factory);
        return source.isRemote() && source.poolEnabled() && !source.assignsClientId();
    }

    /** The description of a pool, before it is built. */
    public static final class Builder {

        private final ConnectionSource source;
        private int maxSize;
        private Duration acquireTimeout;
        private Duration idleTimeout;
        private Duration maxLifetime;
        private Duration leakThreshold;

        /** Starts from what the factory carries: its settings {@code tassis.pool.*}, or the defaults. */
        private Builder(ConnectionSource source) {
            this.source = source;
            this.maxSize = source.poolMaxSize();
            this.acquireTimeout = source.poolAcquireTimeout();
            this.idleTimeout = source.poolIdleTimeout();
            this.maxLifetime = source.poolMaxLifetime();
            this.leakThreshold = source.poolLeakThreshold();
        }

        /**
         * How many connections to the broker the pool opens at most, lent or not, whatever the
         * credentials. The default is {@value TassisConnectionPool#DEFAULT_MAX_SIZE}.
         *
         * @throws IllegalArgumentException if it is less than one
         */
        public Builder maxSize(int maximum) {
            if (maximum < 1) {
                throw new IllegalArgumentException("the maximum size of a pool is at least 1, not " + maximum);
            }
            maxSize = maximum;
            return this;
        }

        /**
         * How long a borrower waits when every connection is lent, before it fails. The default
         * is five seconds.
         *
         * @throws IllegalArgumentException if it is zero or negative
         */
        public Builder acquireTimeout(Duration timeout) {
            if (Objects.requireNonNull(timeout, "timeout").isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("the time a borrower waits must be positive, not " + timeout);
            }
            acquireTimeout = timeout;
            return this;
        }

        /**
         * How long a connection that nobody uses is kept before it is closed; zero for as long as
         * the pool lasts. The default is ten minutes.
         *
         * @throws IllegalArgumentException if it is negative
         */
        public Builder idleTimeout(Duration timeout) {
            idleTimeout = notNegative(timeout, "the time an idle connection is kept");
            return this;
        }

        /**
         * How long a connection is kept at all: past that age it is closed as soon as nobody is
         * using it, never under a borrower. Zero for as long as the pool lasts. The default is
         * thirty minutes.
         *
         * @throws IllegalArgumentException if it is negative
         */
        public Builder maxLifetime(Duration lifetime) {
            maxLifetime = notNegative(lifetime, "the lifetime of a connection");
            return this;
        }

        /**
         * How long a connection may be lent before the pool reports it in the log, as a warning
         * with the call that borrowed it; zero, the default, for never. The connection stays with
         * its borrower.
         *
         * @throws IllegalArgumentException if it is negative
         */
        public Builder leakThreshold(Duration threshold) {
            leakThreshold = notNegative(threshold, "the time after which a lent connection is reported");
            return this;
        }

        private static Duration notNegative(Duration duration, String what) {
            if (Objects.requireNonNull(duration, "duration").isNegative()) {
                throw new IllegalArgumentException(what + " must not be negative: " + duration);
            }
            return duration;
        }

        /** Builds the pool. It opens its first connection when a first one is asked for. */
        public TassisConnectionPool build() {
            return new TassisConnectionPool(this);
        }
    }

    /** How many connections to the broker are open, lent or not. */
    public int size() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    /** How many connections to the broker are open and lent to nobody. */
    public int idle() {
        lock.lock();
        try {
            int count = 0;
            for (Deque<Entry> entries : idle.values()) {
                count += entries.size();
            }
            return count;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "pool of " + source;
    }

    // ---- borrowing ---------------------------------------------------------

    private <T> T borrow(String userName, String password, Lending<T> lending) throws JMSException {
        Entry entry = acquire(new Identity(userName, password));
        lock.lock();
        try {
            entry.lentAt = System.nanoTime();
            entry.reported = false;
            entry.borrower = leakThresholdNanos > 0 ? new Throwable("borrowed here") : null;
            lent.add(entry);
            tick.signalAll();
        } finally {
            lock.unlock();
        }
        try {
            return lending.lend(entry.physical(), reusable -> release(entry, reusable));
        } catch (JMSException | RuntimeException e) {
            // Nobody has the connection, so nobody will give it back.
            release(entry, true);
            throw e;
        }
    }

    /** A connection opened as that identity: one that waits, or a new one if there is room. */
    private Entry acquire(Identity identity) throws JMSException {
        long remaining = acquireTimeoutNanos;
        // Whatever the way out, what was taken out of the pool on the way is closed.
        List<Entry> toClose = List.of();
        try {
            lock.lock();
            try {
                while (true) {
                    if (closed) {
                        throw new IllegalStateException("the pool is closed");
                    }
                    Deque<Entry> waiting = idle.get(identity);
                    if (waiting != null && !waiting.isEmpty()) {
                        Entry kept = waiting.pollLast();
                        if (!kept.physical().isLost() && !tooOld(kept, System.nanoTime())) {
                            return kept;
                        }
                        // Lost and the pool not told yet, or past its lifetime: its place is free.
                        size--;
                        doomed.add(kept);
                        continue;
                    }
                    if (size < maxSize) {
                        // The housekeeper first: if it cannot start, no place is taken.
                        keepHouse();
                        // The place is taken now; the connection is opened once the lock is released.
                        size++;
                        break;
                    }
                    Entry other = anyIdle();
                    if (other != null) {
                        // Its place serves this identity: the size does not change.
                        doomed.add(other);
                        break;
                    }
                    if (remaining <= 0) {
                        throw new ResourceAllocationException("no connection came back to the pool within "
                                + Duration.ofNanos(acquireTimeoutNanos) + ": its " + maxSize + " are all in use");
                    }
                    try {
                        remaining = changed.awaitNanos(remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new JMSException("interrupted while waiting for a connection of the pool");
                    }
                }
            } finally {
                toClose = takeDoomed();
                lock.unlock();
            }
        } finally {
            close(toClose);
        }
        try {
            return new Entry(source.open(identity.userName(), identity.password()), identity);
        } catch (JMSException | RuntimeException e) {
            lock.lock();
            try {
                size--;
                changed.signalAll();
                tick.signalAll();
            } finally {
                lock.unlock();
            }
            throw e;
        }
    }

    /** Takes a waiting connection out, whatever its identity; null if none waits. Under the lock. */
    private Entry anyIdle() {
        Iterator<Deque<Entry>> all = idle.values().iterator();
        while (all.hasNext()) {
            Deque<Entry> entries = all.next();
            if (entries.isEmpty()) {
                all.remove();
            } else {
                // The one that has waited longest.
                return entries.pollFirst();
            }
        }
        return null;
    }

    /** The application closed the connection it was lent. */
    private void release(Entry entry, boolean reusable) {
        List<Entry> toClose;
        lock.lock();
        try {
            lent.remove(entry);
            entry.borrower = null;
            if (!reusable) {
                // Closed already, by the connection that had it.
                size--;
            } else if (closed || tooOld(entry, System.nanoTime())) {
                size--;
                doomed.add(entry);
            } else {
                entry.idleSince = System.nanoTime();
                idle.computeIfAbsent(entry.identity(), identity -> new ArrayDeque<>()).addLast(entry);
                // Under the lock, so that no borrower has taken the connection yet: it will ask
                // to be told itself, and whoever asks last is told. A connection lost already
                // tells at once, on this thread, which holds the lock twice then.
                entry.physical().onLost(() -> lostWhileIdle(entry));
            }
            changed.signalAll();
            tick.signalAll();
            toClose = takeDoomed();
        } finally {
            lock.unlock();
        }
        close(toClose);
    }

    // ---- housekeeping ------------------------------------------------------

    /** Whether the connection has lived as long as one may. Under the lock. */
    private boolean tooOld(Entry entry, long now) {
        return maxLifetimeNanos > 0 && now - entry.openedAt >= maxLifetimeNanos;
    }

    /** Starts the housekeeper if there is something for it to watch and it is not running. Under the lock. */
    private void keepHouse() {
        if (housekeeper == null && (idleTimeoutNanos > 0 || maxLifetimeNanos > 0 || leakThresholdNanos > 0)) {
            // Kept only once started: a thread that cannot start must not pass for a housekeeper.
            Thread thread = Thread.ofVirtual().name("tassis-pool-housekeeper").unstarted(this::housekeeping);
            thread.start();
            housekeeper = thread;
        }
    }

    /**
     * Closes the idle connections kept too long and reports the lent ones kept too long, then
     * sleeps until the next of them is due. Ends when the pool is closed or has no connection
     * left; the next connection opened starts another.
     */
    private void housekeeping() {
        try {
            watch();
        } finally {
            // However it ends, the next connection opened must be able to start another.
            lock.lock();
            try {
                if (housekeeper == Thread.currentThread()) {
                    housekeeper = null;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void watch() {
        while (true) {
            List<Entry> toClose;
            List<Throwable> toReport = new ArrayList<>();
            lock.lock();
            try {
                if (closed || size == 0) {
                    return;
                }
                long now = System.nanoTime();
                // How long until something is due; Long.MAX_VALUE for nothing.
                long wait = Long.MAX_VALUE;
                for (Deque<Entry> entries : idle.values()) {
                    for (Iterator<Entry> all = entries.iterator(); all.hasNext();) {
                        Entry entry = all.next();
                        long due = Long.MAX_VALUE;
                        if (idleTimeoutNanos > 0) {
                            due = idleTimeoutNanos - (now - entry.idleSince);
                        }
                        if (maxLifetimeNanos > 0) {
                            due = Math.min(due, maxLifetimeNanos - (now - entry.openedAt));
                        }
                        if (due <= 0) {
                            all.remove();
                            size--;
                            doomed.add(entry);
                        } else {
                            wait = Math.min(wait, due);
                        }
                    }
                }
                if (leakThresholdNanos > 0) {
                    for (Entry entry : lent) {
                        if (entry.reported) {
                            continue;
                        }
                        long due = leakThresholdNanos - (now - entry.lentAt);
                        if (due <= 0) {
                            entry.reported = true;
                            toReport.add(entry.borrower);
                        } else {
                            wait = Math.min(wait, due);
                        }
                    }
                }
                toClose = takeDoomed();
                if (toClose.isEmpty() && toReport.isEmpty()) {
                    try {
                        if (wait == Long.MAX_VALUE) {
                            tick.await();
                        } else {
                            tick.awaitNanos(wait);
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                    continue;
                }
                // Places are free.
                changed.signalAll();
            } finally {
                lock.unlock();
            }
            close(toClose);
            for (Throwable borrower : toReport) {
                try {
                    LOG.log(System.Logger.Level.WARNING, "a connection of the " + this
                            + " has been lent for more than " + Duration.ofNanos(leakThresholdNanos)
                            + " and is not given back: was it closed?", borrower);
                } catch (RuntimeException unreported) {
                    // A log that fails must not stop the housekeeping.
                }
            }
        }
    }

    /** A connection is lost. If a borrower has it, the borrower was told too and will not give it back. */
    private void lostWhileIdle(Entry entry) {
        List<Entry> toClose = List.of();
        lock.lock();
        try {
            Deque<Entry> waiting = idle.get(entry.identity());
            if (waiting != null && waiting.remove(entry)) {
                size--;
                doomed.add(entry);
                changed.signalAll();
                tick.signalAll();
            }
            if (lock.getHoldCount() == 1) {
                toClose = takeDoomed();
            }
        } finally {
            lock.unlock();
        }
        close(toClose);
    }

    /** What whoever leaves the lock must close. Under the lock. */
    private List<Entry> takeDoomed() {
        if (doomed.isEmpty()) {
            return List.of();
        }
        List<Entry> taken = List.copyOf(doomed);
        doomed.clear();
        return taken;
    }

    private static void close(List<Entry> entries) {
        for (Entry entry : entries) {
            try {
                entry.physical().close();
            } catch (RuntimeException alreadyBroken) {
                // The others must be closed all the same, and nobody is waiting for this one.
            }
        }
    }

    /**
     * Closes the connections that nobody is using and refuses to lend any from now on: a
     * borrower that waits fails. A connection that is lent stays usable, and is closed when the
     * application closes it. Closing a closed pool does nothing.
     */
    @Override
    public void close() {
        List<Entry> toClose;
        lock.lock();
        try {
            closed = true;
            for (Deque<Entry> entries : idle.values()) {
                size -= entries.size();
                doomed.addAll(entries);
            }
            idle.clear();
            changed.signalAll();
            tick.signalAll();
            toClose = takeDoomed();
        } finally {
            lock.unlock();
        }
        close(toClose);
    }

    // ---- the classic API ---------------------------------------------------

    /**
     * @throws ResourceAllocationException if every connection is lent and none comes back in time
     * @throws IllegalStateException if the pool is closed
     */
    @Override
    public Connection createConnection() throws JMSException {
        return borrow(null, null, PhysicalConnection::connection);
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return borrow(userName, password, PhysicalConnection::connection);
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return borrow(null, null, PhysicalConnection::queueConnection);
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return borrow(userName, password, PhysicalConnection::queueConnection);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return borrow(null, null, PhysicalConnection::topicConnection);
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return borrow(userName, password, PhysicalConnection::topicConnection);
    }

    // ---- the simplified API ------------------------------------------------

    /** The contexts created from the one returned share its connection, given back with the last of them. */
    private JMSContext context(String userName, String password, int sessionMode) {
        ConnectionSource.checkSessionMode(sessionMode);
        try {
            return borrow(userName, password, (physical, recycler) -> physical.context(sessionMode, recycler));
        } catch (JMSException e) {
            throw ConnectionSource.unchecked(e);
        }
    }

    @Override
    public JMSContext createContext() {
        return context(null, null, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        return context(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        return context(userName, password, sessionMode);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return context(null, null, sessionMode);
    }
}
