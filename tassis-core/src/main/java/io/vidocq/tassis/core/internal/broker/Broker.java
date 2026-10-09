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
package io.vidocq.tassis.core.internal.broker;

import io.vidocq.tassis.core.internal.message.TassisMessage;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * The in-VM broker: named queues holding the messages sent to them until a
 * consumer takes them (Jakarta Messaging 3.1 §4.1).
 *
 * <p>All broker state, and the delivery-related state of its clients (connection
 * started, consumer closed), is guarded by one lock. Whatever changes that state
 * signals {@link #changed}, which is the only thing a waiting receive listens to.
 */
public final class Broker {

    /** Timeout value of {@link #receive} that never expires. */
    public static final long FOREVER = Long.MAX_VALUE;

    private static final ConcurrentHashMap<String, Broker> BROKERS = new ConcurrentHashMap<>();

    /** A consumer's attachment to a queue. */
    public static final class QueueConsumer {

        private final String queue;
        private final BooleanSupplier started;
        private boolean closed;
        private boolean detached;
        // A count, not a flag: a misused consumer may be in several receives at once.
        private int receiving;

        private QueueConsumer(String queue, BooleanSupplier started) {
            this.queue = queue;
            this.started = started;
        }
    }

    private record Stored(TassisMessage message, long sequence) {

        /** Highest priority first, then the order the messages were sent in (§3.4.10, §4.1). */
        static final Comparator<Stored> DELIVERY_ORDER = Comparator
                .comparingInt((Stored stored) -> -stored.message.getJMSPriority())
                .thenComparingLong(Stored::sequence);
    }

    private final String name;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<String, TreeSet<Stored>> queues = new HashMap<>();
    private final Map<String, Integer> consumerCounts = new HashMap<>();
    private final Set<String> temporaryQueues = new HashSet<>();
    private final AtomicLong messageIds = new AtomicLong();
    private final AtomicLong temporaryQueueIds = new AtomicLong();
    private long sequence;

    private Broker(String name) {
        this.name = name;
    }

    /** Returns the broker of that name in this JVM, creating it on first use. */
    public static Broker named(String name) {
        return BROKERS.computeIfAbsent(name, Broker::new);
    }

    public String nextMessageId() {
        return "ID:" + name + "-" + messageIds.incrementAndGet();
    }

    /** Creates a temporary queue and returns its name. */
    public String createTemporaryQueue() {
        String queue = "temporary:" + name + "-" + temporaryQueueIds.incrementAndGet();
        lock.lock();
        try {
            temporaryQueues.add(queue);
            return queue;
        } finally {
            lock.unlock();
        }
    }

    /** Returns whether the temporary queue of that name still exists. */
    public boolean hasTemporaryQueue(String queue) {
        lock.lock();
        try {
            return temporaryQueues.contains(queue);
        } finally {
            lock.unlock();
        }
    }

    /** Runs a change to delivery-related client state, then wakes the waiting receives. */
    public void update(Runnable change) {
        lock.lock();
        try {
            change.run();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds {@code message}, which the broker now owns, to the queue.
     *
     * @param temporary whether {@code queue} names a temporary queue, which must then still exist
     */
    public void send(String queue, boolean temporary, TassisMessage message) throws InvalidDestinationException {
        lock.lock();
        try {
            checkExists(queue, temporary);
            queues.computeIfAbsent(queue, created -> new TreeSet<>(Stored.DELIVERY_ORDER))
                    .add(new Stored(message, sequence++));
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Attaches a consumer to a queue.
     *
     * @param temporary whether {@code queue} names a temporary queue, which must then still exist
     * @param started tells whether the consumer's connection is started; read under the broker lock
     */
    public QueueConsumer attach(String queue, boolean temporary, BooleanSupplier started)
            throws InvalidDestinationException {
        lock.lock();
        try {
            checkExists(queue, temporary);
            consumerCounts.merge(queue, 1, Integer::sum);
            return new QueueConsumer(queue, started);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the consumer; returns once its receive in progress, if any, has completed (§8.8).
     * Every caller waits for that, including one that finds the consumer already closing.
     */
    public void detach(QueueConsumer consumer) {
        lock.lock();
        try {
            consumer.closed = true;
            changed.signalAll();
            while (consumer.receiving > 0) {
                changed.awaitUninterruptibly();
            }
            if (!consumer.detached) {
                consumer.detached = true;
                consumerCounts.computeIfPresent(consumer.queue, (queue, count) -> count == 1 ? null : count - 1);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Takes the next message of the consumer's queue, in read-only state.
     *
     * @param timeoutNanos how long to wait for one: 0 not at all, {@link #FOREVER} without limit
     * @return the message, or {@code null} if the timeout expires or the consumer is closed
     */
    public TassisMessage receive(QueueConsumer consumer, long timeoutNanos) throws JMSException {
        // The timeout runs from the call, not from the moment the lock is obtained.
        long start = System.nanoTime();
        lock.lock();
        try {
            consumer.receiving++;
            while (!consumer.closed) {
                long now = System.currentTimeMillis();
                long nextDeliveryTime = Long.MAX_VALUE;
                // A stopped connection delivers nothing, but its receive timers keep running (§6.1.5).
                if (consumer.started.getAsBoolean()) {
                    Iterator<Stored> stored = messagesOf(consumer.queue).iterator();
                    while (stored.hasNext()) {
                        TassisMessage message = stored.next().message;
                        if (isExpired(message, now)) {
                            stored.remove();
                        } else if (message.getJMSDeliveryTime() > now) {
                            nextDeliveryTime = Math.min(nextDeliveryTime, message.getJMSDeliveryTime());
                        } else {
                            message.markReceived();
                            stored.remove();
                            return message;
                        }
                    }
                }
                long wait = timeoutNanos == FOREVER ? FOREVER : timeoutNanos - (System.nanoTime() - start);
                if (wait <= 0) {
                    return null;
                }
                if (nextDeliveryTime != Long.MAX_VALUE) {
                    wait = Math.min(wait, TimeUnit.MILLISECONDS.toNanos(nextDeliveryTime - now));
                }
                if (wait == FOREVER) {
                    changed.await();
                } else {
                    changed.awaitNanos(wait);
                }
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JMSException("interrupted while waiting for a message");
        } finally {
            consumer.receiving--;
            changed.signalAll();
            lock.unlock();
        }
    }

    /** Returns copies of the messages a consumer could take from the queue right now, in delivery order. */
    public List<TassisMessage> browse(String queue) {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            List<TassisMessage> messages = new ArrayList<>();
            for (Stored stored : messagesOf(queue)) {
                TassisMessage message = stored.message;
                // §7.9: a browser must not return a message before its delivery time.
                if (!isExpired(message, now) && message.getJMSDeliveryTime() <= now) {
                    TassisMessage copy = message.copy();
                    copy.markReceived();
                    messages.add(copy);
                }
            }
            return messages;
        } finally {
            lock.unlock();
        }
    }

    /** Returns how many consumers are attached to the queue. */
    public int consumerCount(String queue) {
        lock.lock();
        try {
            return consumerCounts.getOrDefault(queue, 0);
        } finally {
            lock.unlock();
        }
    }

    /** Deletes a temporary queue and its messages; fails while a consumer is attached to it (§6.2.2). */
    public void deleteTemporaryQueue(String queue) throws JMSException {
        lock.lock();
        try {
            if (consumerCounts.containsKey(queue)) {
                throw new JMSException("queue " + queue + " still has consumers");
            }
            temporaryQueues.remove(queue);
            queues.remove(queue);
        } finally {
            lock.unlock();
        }
    }

    /** Deletes a temporary queue whose connection is closing, whatever is attached to it. */
    public void dropTemporaryQueue(String queue) {
        lock.lock();
        try {
            temporaryQueues.remove(queue);
            queues.remove(queue);
        } finally {
            lock.unlock();
        }
    }

    private void checkExists(String queue, boolean temporary) throws InvalidDestinationException {
        if (temporary && !temporaryQueues.contains(queue)) {
            throw new InvalidDestinationException("temporary queue " + queue + " has been deleted");
        }
    }

    /** The stored messages of a queue, for reading: a queue nobody sent to has none. */
    private Set<Stored> messagesOf(String queue) {
        Set<Stored> messages = queues.get(queue);
        return messages == null ? Set.of() : messages;
    }

    private static boolean isExpired(TassisMessage message, long now) {
        return message.getJMSExpiration() != 0 && message.getJMSExpiration() <= now;
    }
}
