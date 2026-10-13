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
import io.vidocq.tassis.core.internal.selector.Selector;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Outgoing;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Subscribing;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The in-VM broker (Jakarta Messaging 3.1 chapter 4).
 *
 * <p>Messages wait in a {@link Storage}: one per queue, and one per topic subscription. A
 * queue's storage is fed by the sends to the queue; a subscription's storage is fed by the
 * publications to its topic. From there on the two behave alike: consumers attached to a
 * storage compete for its messages.
 *
 * <p>A client reaches the broker through a {@link BrokerConnection}, obtained from
 * {@link #connect}: the broker reads nothing from its clients. What it keeps for each of them
 * is in a {@link LocalConnection}.
 *
 * <p>All broker state, that of its connections included (started, consumers, unsettled
 * deliveries), is guarded by one lock. A receive that waits does so on the condition of its
 * queue or subscription ({@link Storage#changed}), and whatever changes what it waits for
 * signals that condition and no other: a message that arrives or comes back
 * ({@link Storage#add}), its consumer closing or its listener changing, its connection
 * starting. What happens to another queue wakes nobody here (ROADMAP M12-2).
 */
public final class Broker {

    private static final long FOREVER = BrokerConnection.FOREVER;

    /** For a receive that does not depend on the consumer listening; no period has this number. */
    private static final long NO_PERIOD = -1;

    private static final ConcurrentHashMap<String, Broker> BROKERS = new ConcurrentHashMap<>();

    /** A consumer's attachment to a queue or to a subscription. */
    static final class Consumer implements BrokerConnection.Consumer {

        private final LocalConnection connection;
        private final Storage storage;
        private final Subscription subscription;
        // A queue consumer's own filter; a subscription filters when the message is published.
        private final Selector selector;
        // The selector's view of the queue, or null when the consumer looks at the whole storage.
        private final View view;
        private boolean listening;
        // Changes each time the consumer stops listening: see BrokerConnection.Consumer.listen.
        private long period;
        private boolean closed;
        private boolean detached;
        // A count, not a flag: a misused consumer may be in several receives at once.
        private int receiving;

        private Consumer(LocalConnection connection, Storage storage, Subscription subscription, Selector selector,
                View view) {
            this.connection = connection;
            this.storage = storage;
            this.subscription = subscription;
            this.selector = selector;
            this.view = view;
        }

        @Override
        public BrokerConnection.Delivery receive(long timeoutNanos, boolean settleOnDelivery) throws JMSException {
            return connection.broker.receive(this, timeoutNanos, settleOnDelivery, NO_PERIOD);
        }

        @Override
        public BrokerConnection.Delivery take(long ofPeriod) throws JMSException {
            return connection.broker.receive(this, FOREVER, false, ofPeriod);
        }

        @Override
        public long listen(boolean nowListening) {
            return connection.broker.listen(this, nowListening);
        }

        @Override
        public void close() {
            connection.broker.detach(this);
        }
    }

    /** A message handed to a consumer. */
    static final class Delivery implements BrokerConnection.Delivery {

        private final Stored stored;
        private final TassisMessage message;
        private final int deliveryCount;

        private Delivery(Stored stored, TassisMessage message) {
            this.stored = stored;
            this.message = message;
            this.deliveryCount = stored.deliveryCount;
        }

        @Override
        public int deliveryCount() {
            return deliveryCount;
        }

        @Override
        public TassisMessage message() {
            return message;
        }
    }

    /** The messages waiting for the consumers of one queue or of one subscription. */
    private static final class Storage {

        // What the receives of its consumers wait on, and whoever waits for one of them to end.
        // A condition of the broker lock.
        final Condition changed;
        // The messages a consumer may take now, in delivery order.
        final TreeSet<Stored> messages = new TreeSet<>(Stored.DELIVERY_ORDER);
        // Those whose delivery time has not come (§7.9), the soonest first: kept aside, so that
        // no receive looks at them, and moved among the others when their time comes (release).
        final TreeSet<Stored> delayed = new TreeSet<>(Stored.DUE_ORDER);
        // One view per selector in use by the queue's consumers: see View.
        final Map<Selector, View> views = new HashMap<>();
        int consumers;
        // Set when the queue or subscription is deleted: a message given back then goes nowhere.
        boolean deleted;
        // The earliest expiration among the messages, or Long.MAX_VALUE: when to sweep next.
        long nextExpiration = Long.MAX_VALUE;

        Storage(Condition changed) {
            this.changed = changed;
        }

        /**
         * Removes the messages that have expired (§3.4.9). Done on the whole storage, since a
         * consumer that reads through a view never meets the messages its selector rejects.
         */
        void sweepExpired(long now) {
            if (now < nextExpiration) {
                return;
            }
            List<Stored> expired = new ArrayList<>();
            long next = Long.MAX_VALUE;
            for (TreeSet<Stored> held : List.of(messages, delayed)) {
                for (Stored stored : held) {
                    long expiration = stored.message.getJMSExpiration();
                    if (expiration != 0 && expiration <= now) {
                        expired.add(stored);
                    } else if (expiration != 0) {
                        next = Math.min(next, expiration);
                    }
                }
            }
            expired.forEach(this::remove);
            nextExpiration = next;
        }

        /**
         * Moves among the messages a consumer may take those whose delivery time has come
         * (§7.9), each to the place its priority and its sending give it.
         *
         * @return when the next one is due, or {@code Long.MAX_VALUE} if none is delayed
         */
        long release(long now) {
            while (!delayed.isEmpty() && delayed.first().message.getJMSDeliveryTime() <= now) {
                place(delayed.pollFirst());
            }
            return delayed.isEmpty() ? Long.MAX_VALUE : delayed.first().message.getJMSDeliveryTime();
        }

        private void place(Stored stored) {
            messages.add(stored);
            views.forEach((selector, view) -> {
                if (stored.selectedBy(selector)) {
                    view.messages.add(stored);
                }
            });
        }

        /** @param due whether the delivery time of the message has come: see {@link Broker#isDue} */
        void add(Stored stored, boolean due) {
            long expiration = stored.message.getJMSExpiration();
            if (expiration != 0) {
                nextExpiration = Math.min(nextExpiration, expiration);
            }
            if (due) {
                place(stored);
            } else {
                delayed.add(stored);
            }
            // All of them, not one: the first to wake may not be able to take it (its selector,
            // its connection stopped, its listener gone), or be no receive at all but whoever
            // waits for one to end (detach). A delayed message wakes them too: they then wait
            // no longer than until it is due.
            changed.signalAll();
        }

        void remove(Stored stored) {
            messages.remove(stored);
            delayed.remove(stored);
            for (View view : views.values()) {
                view.messages.remove(stored);
            }
        }
    }

    /**
     * The messages of a queue that one selector accepts, in delivery order. It exists while a
     * consumer of the queue uses that selector, and is kept up to date as messages come and go,
     * so that a message is evaluated once per selector and a receive need not scan the messages
     * its selector rejects. A selector that looks at the delivery count gets no view: its answer
     * changes from one delivery to the next.
     */
    private static final class View {

        final TreeSet<Stored> messages = new TreeSet<>(Stored.DELIVERY_ORDER);
        int consumers;
    }

    private static final class Stored {

        /** Highest priority first, then the order the messages were sent in (§3.4.10, §4.1). */
        static final Comparator<Stored> DELIVERY_ORDER = Comparator
                .comparingInt((Stored stored) -> -stored.message.getJMSPriority())
                .thenComparingLong(stored -> stored.sequence);

        /** The soonest due first (§7.9); the delivery order tells apart those due together. */
        static final Comparator<Stored> DUE_ORDER = Comparator
                .comparingLong((Stored stored) -> stored.message.getJMSDeliveryTime())
                .thenComparing(DELIVERY_ORDER);

        final Storage storage;
        // Shared by every subscription the message was published to: it is never modified.
        final TassisMessage message;
        // Kept across redeliveries, so that a message given back returns to its place.
        final long sequence;
        int deliveryCount;
        // What each selector answered for this message, which never changes: asked once.
        private Map<Selector, Boolean> selections;

        Stored(Storage storage, TassisMessage message, long sequence) {
            this.storage = storage;
            this.message = message;
            this.sequence = sequence;
        }

        boolean selectedBy(Selector selector) {
            if (selections == null) {
                selections = new HashMap<>();
            }
            return selections.computeIfAbsent(selector, asked -> asked.matches(message, 1));
        }
    }

    private static final class Subscription {

        final Subscribing request;
        // The client identifier the name is scoped by, or null if the subscriber had none.
        final String clientId;
        // Who subscribed, to be compared with who publishes (§8.3.1).
        final Object subscriber;
        final Storage storage;

        Subscription(Subscribing request, LocalConnection connection, Condition changed) {
            this.storage = new Storage(changed);
            this.request = request;
            this.clientId = request.global() ? null : connection.clientId;
            this.subscriber = connection.identity();
        }
    }

    /** Durable and shared non-durable subscriptions are two separate namespaces (§8.3). */
    private record SubscriptionName(boolean durable, String clientId, String name) {
    }

    private final String name;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Storage> queues = new HashMap<>();
    private final Set<String> temporaryQueues = new HashSet<>();
    private final Set<String> temporaryTopics = new HashSet<>();
    private final Map<String, List<Subscription>> subscriptionsByTopic = new HashMap<>();
    private final Map<SubscriptionName, Subscription> namedSubscriptions = new HashMap<>();
    // Who created each temporary destination: it goes when its connection does.
    private final Map<String, LocalConnection> temporaryOwners = new HashMap<>();
    private final Set<String> clientIds = new HashSet<>();
    private final AtomicLong messageIds = new AtomicLong();
    private final AtomicLong temporaryIds = new AtomicLong();
    private long sequence;
    // How many times a receive came back from waiting, for a message or for nothing.
    private long awakenings;
    // A time the clock has shown: never ahead of it. See isDue.
    private long clock;
    // How many messages the receives have looked at to find the one to take.
    private long examined;
    // How many receives are waiting right now.
    private int waiting;
    private final Users users;

    private Broker(String name) {
        this.name = name;
        this.users = new Users(name);
    }

    /** The user registry, which guards itself: it is neither read nor written under the broker lock. */
    public Users users() {
        return users;
    }

    /** The name this broker is known by in its JVM. */
    public String name() {
        return name;
    }

    /** Returns the broker of that name in this JVM, creating it on first use. */
    public static Broker named(String name) {
        return BROKERS.computeIfAbsent(name, Broker::new);
    }

    /**
     * Opens a connection for a client, once the broker has accepted who it says it is (§6.1.1).
     *
     * @param userName the user name, or {@code null} for a connection without credentials
     * @throws JMSSecurityException if the broker is secured and rejects the credentials
     */
    public BrokerConnection connect(String userName, String password) throws JMSSecurityException {
        // Before anything is kept for the client: a rejected one leaves no trace.
        users.check(userName, password);
        return new LocalConnection(this);
    }

    /** Wakes the receives of the connection's consumers, for what changed for all of them. Under the lock. */
    private static void wake(LocalConnection connection) {
        for (Consumer consumer : connection.consumers) {
            consumer.storage.changed.signalAll();
        }
    }

    String nextMessageId() {
        return "ID:" + name + "-" + messageIds.incrementAndGet();
    }

    private static void checkOpen(LocalConnection connection) throws IllegalStateException {
        if (connection.closed) {
            throw new IllegalStateException("the connection is closed");
        }
    }

    // ---- connections -----------------------------------------------------

    /** Reserves a client identifier for the connection, until it closes (§6.1.2). */
    void claimClientId(LocalConnection connection, String clientId) throws JMSException {
        lock.lock();
        try {
            checkOpen(connection);
            if (connection.clientId != null) {
                throw new IllegalStateException("the client identifier is already set to " + connection.clientId);
            }
            if (!clientIds.add(clientId)) {
                throw new InvalidClientIDException("client identifier " + clientId + " is in use by another connection");
            }
            connection.clientId = clientId;
        } finally {
            lock.unlock();
        }
    }

    boolean start(LocalConnection connection) {
        lock.lock();
        try {
            if (connection.closed || connection.started) {
                return false;
            }
            connection.started = true;
            wake(connection);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Flipped under the lock every delivery holds: once this returns, no receive hands out a message. */
    void stop(LocalConnection connection) {
        lock.lock();
        try {
            connection.started = false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Ends a connection, taking back what it leaves behind. The order matters: its consumers
     * first, so that a non-durable subscription ends before its unsettled messages look for a
     * place to return to.
     */
    void close(LocalConnection connection) {
        lock.lock();
        try {
            if (connection.closed) {
                return;
            }
            connection.closed = true;
            connection.started = false;
            // Each detach wakes the receives of its consumer; what is given back, those of its queue.
            for (Consumer consumer : List.copyOf(connection.consumers)) {
                detach(consumer);
            }
            for (Delivery delivery : connection.unsettled) {
                giveBack(delivery);
            }
            connection.unsettled.clear();
            for (String queue : List.copyOf(connection.temporaryQueues)) {
                dropTemporaryQueue(queue);
            }
            for (String topic : List.copyOf(connection.temporaryTopics)) {
                dropTemporaryTopic(topic);
            }
            if (connection.clientId != null) {
                clientIds.remove(connection.clientId);
            }
        } finally {
            lock.unlock();
        }
    }

    // ---- temporary destinations ------------------------------------------

    /** Creates a temporary queue or topic that lasts as long as the connection, and returns its name. */
    String createTemporary(LocalConnection connection, boolean topic) throws IllegalStateException {
        String created = (topic ? "temporary-topic:" : "temporary-queue:") + name + "-" + temporaryIds.incrementAndGet();
        lock.lock();
        try {
            checkOpen(connection);
            (topic ? temporaryTopics : temporaryQueues).add(created);
            (topic ? connection.temporaryTopics : connection.temporaryQueues).add(created);
            temporaryOwners.put(created, connection);
            return created;
        } finally {
            lock.unlock();
        }
    }

    /** Returns whether the temporary queue of that name still exists. */
    boolean hasTemporaryQueue(String queue) {
        lock.lock();
        try {
            return temporaryQueues.contains(queue);
        } finally {
            lock.unlock();
        }
    }

    /** Returns whether the temporary topic of that name still exists. */
    boolean hasTemporaryTopic(String topic) {
        lock.lock();
        try {
            return temporaryTopics.contains(topic);
        } finally {
            lock.unlock();
        }
    }

    /** Deletes a temporary queue and its messages; fails while a consumer is attached to it (§6.2.2). */
    void deleteTemporaryQueue(String queue) throws JMSException {
        lock.lock();
        try {
            Storage storage = queues.get(queue);
            if (storage != null && storage.consumers > 0) {
                throw new JMSException("queue " + queue + " still has consumers");
            }
            dropTemporaryQueue(queue);
        } finally {
            lock.unlock();
        }
    }

    /** Deletes a temporary queue, whatever is attached to it. Under the lock. */
    private void dropTemporaryQueue(String queue) {
        temporaryQueues.remove(queue);
        LocalConnection owner = temporaryOwners.remove(queue);
        if (owner != null) {
            owner.temporaryQueues.remove(queue);
        }
        Storage storage = queues.remove(queue);
        if (storage != null) {
            storage.deleted = true;
        }
    }

    /**
     * Deletes a temporary topic; fails while a consumer is subscribed to it (§6.2.2). Its
     * subscriptions without a consumer, which can only be durable ones, go with it.
     */
    void deleteTemporaryTopic(String topic) throws JMSException {
        lock.lock();
        try {
            for (Subscription subscription : subscriptionsByTopic.getOrDefault(topic, List.of())) {
                if (subscription.storage.consumers > 0) {
                    throw new JMSException("topic " + topic + " still has subscribers");
                }
            }
            dropTemporaryTopic(topic);
        } finally {
            lock.unlock();
        }
    }

    /** Deletes a temporary topic with its subscriptions, whatever is attached to them. Under the lock. */
    private void dropTemporaryTopic(String topic) {
        temporaryTopics.remove(topic);
        LocalConnection owner = temporaryOwners.remove(topic);
        if (owner != null) {
            owner.temporaryTopics.remove(topic);
        }
        for (Subscription subscription : List.copyOf(subscriptionsByTopic.getOrDefault(topic, List.of()))) {
            remove(subscription);
        }
    }

    // ---- sending ---------------------------------------------------------

    /**
     * Adds messages, which the broker now owns, to their destinations, and settles deliveries:
     * all of it, or nothing if one of the messages names a temporary destination that no longer
     * exists. This is a commit (§6.2.7), and a send outside a transaction is a commit of one
     * message. A message sent to a queue waits there; a message published to a topic is added
     * to each of its subscriptions, and to none if it has none (§4.2.3).
     */
    void commit(LocalConnection connection, Collection<Outgoing> messages,
            Collection<? extends BrokerConnection.Delivery> consumed) throws JMSException {
        lock.lock();
        try {
            checkOpen(connection);
            for (Outgoing outgoing : messages) {
                checkExists(outgoing.destination(), outgoing.topic(), outgoing.temporary());
            }
            Object publisher = connection.identity();
            for (Outgoing outgoing : messages) {
                if (!outgoing.topic()) {
                    store(storageOf(outgoing.destination()), outgoing.message());
                    continue;
                }
                for (Subscription subscription : subscriptionsByTopic.getOrDefault(outgoing.destination(), List.of())) {
                    // §8.3.1: a noLocal subscription ignores what its own connection publishes.
                    boolean local = Objects.equals(subscription.subscriber, publisher);
                    Selector selector = subscription.request.selector();
                    // §4.2.2: a message the selector rejects is never part of the subscription.
                    boolean selected = selector == null || selector.matches(outgoing.message(), 1);
                    if (selected && !(subscription.request.noLocal() && local)) {
                        store(subscription.storage, outgoing.message());
                    }
                }
            }
            for (BrokerConnection.Delivery delivery : consumed) {
                connection.unsettled.remove(delivery);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Whether the delivery time of a message has come (§7.9). Under the lock. Nearly every
     * message is sent without a delay and is due the moment it arrives: the time a receive last
     * read is enough to tell, and the clock is only read for a message that is ahead of it.
     */
    private boolean isDue(TassisMessage message) {
        long time = message.getJMSDeliveryTime();
        if (time > clock) {
            clock = Math.max(clock, System.currentTimeMillis());
        }
        return time <= clock;
    }

    private void store(Storage storage, TassisMessage message) {
        storage.add(new Stored(storage, message, sequence++), isDue(message));
    }

    // ---- settling --------------------------------------------------------

    /** Returns false if the delivery was no longer the connection's to settle. */
    boolean settle(LocalConnection connection, BrokerConnection.Delivery delivery) {
        lock.lock();
        try {
            return connection.unsettled.remove(delivery);
        } finally {
            lock.unlock();
        }
    }

    void settle(LocalConnection connection, Collection<? extends BrokerConnection.Delivery> deliveries) {
        lock.lock();
        try {
            for (BrokerConnection.Delivery delivery : deliveries) {
                connection.unsettled.remove(delivery);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Puts back a message that was taken from the broker but never reached the application,
     * as if it had not been taken: this delivery does not count.
     */
    void returnUndelivered(LocalConnection connection, BrokerConnection.Delivery returned) {
        lock.lock();
        try {
            // Not the connection's any more: settled, or given back already, by it or by its closing.
            if (connection.unsettled.remove(returned)) {
                Delivery delivery = (Delivery) returned;
                delivery.stored.deliveryCount--;
                giveBack(delivery);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Puts back messages that were delivered and not settled. Their next delivery will be
     * marked as a redelivery. A message of a deleted queue or subscription is dropped.
     */
    void redeliver(LocalConnection connection, Collection<? extends BrokerConnection.Delivery> deliveries) {
        lock.lock();
        try {
            for (BrokerConnection.Delivery delivery : deliveries) {
                if (connection.unsettled.remove(delivery)) {
                    giveBack((Delivery) delivery);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns a message to the place its sequence number gives it, which wakes whoever waits for
     * one there. Under the lock.
     */
    private static void giveBack(Delivery delivery) {
        Storage storage = delivery.stored.storage;
        if (!storage.deleted) {
            // Delivered once already: its time has come.
            storage.add(delivery.stored, true);
        }
    }

    // ---- consuming -------------------------------------------------------

    /** Attaches a consumer to a queue: see {@link BrokerConnection#attach}. */
    Consumer attach(LocalConnection connection, String queue, boolean temporary, Selector selector)
            throws JMSException {
        lock.lock();
        try {
            checkOpen(connection);
            checkExists(queue, false, temporary);
            Storage storage = storageOf(queue);
            storage.consumers++;
            View view = null;
            if (selector != null && !selector.referencesDeliveryCount()) {
                view = storage.views.get(selector);
                if (view == null) {
                    // The one scan a selector costs: afterwards the view follows the queue.
                    view = new View();
                    for (Stored stored : storage.messages) {
                        if (stored.selectedBy(selector)) {
                            view.messages.add(stored);
                        }
                    }
                    storage.views.put(selector, view);
                }
                view.consumers++;
            }
            Consumer consumer = new Consumer(connection, storage, null, selector, view);
            connection.consumers.add(consumer);
            return consumer;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Attaches a consumer to a topic subscription, creating, reusing or replacing the
     * subscription as §8.3 prescribes for its kind.
     */
    Consumer subscribe(LocalConnection connection, Subscribing request) throws JMSException {
        lock.lock();
        try {
            checkOpen(connection);
            checkExists(request.topic(), true, request.temporary());
            Subscription subscription =
                    request.name() == null ? add(connection, request) : named(connection, request);
            subscription.storage.consumers++;
            Consumer consumer = new Consumer(connection, subscription.storage, subscription, null, null);
            connection.consumers.add(consumer);
            return consumer;
        } finally {
            lock.unlock();
        }
    }

    private Subscription named(LocalConnection connection, Subscribing request) throws JMSException {
        Subscription existing =
                namedSubscriptions.get(nameOf(request.global() ? null : connection.clientId, request));
        if (existing == null) {
            return add(connection, request);
        }
        boolean active = existing.storage.consumers > 0;
        if (existing.request.shared() != request.shared()) {
            throw new JMSException("a " + (existing.request.shared() ? "shared" : "unshared")
                    + " durable subscription named " + request.name() + " already exists");
        }
        if (!request.shared() && active) {
            throw new JMSException("durable subscription " + request.name() + " already has an active consumer");
        }
        if (existing.request.describesTheSameAs(request)) {
            return existing;
        }
        if (active) {
            throw new JMSException("subscription " + request.name()
                    + " has active consumers and cannot be changed to another topic or selector");
        }
        // Same name, another topic or selector: the old subscription is deleted and a new one starts.
        remove(existing);
        return add(connection, request);
    }

    private Subscription add(LocalConnection connection, Subscribing request) {
        Subscription subscription = new Subscription(request, connection, lock.newCondition());
        subscriptionsByTopic.computeIfAbsent(request.topic(), topic -> new ArrayList<>()).add(subscription);
        if (request.name() != null) {
            namedSubscriptions.put(nameOf(subscription.clientId, request), subscription);
        }
        return subscription;
    }

    private void remove(Subscription subscription) {
        Subscribing request = subscription.request;
        subscription.storage.deleted = true;
        subscription.storage.messages.clear();
        subscription.storage.delayed.clear();
        List<Subscription> ofTopic = subscriptionsByTopic.get(request.topic());
        ofTopic.remove(subscription);
        if (ofTopic.isEmpty()) {
            subscriptionsByTopic.remove(request.topic());
        }
        if (request.name() != null) {
            namedSubscriptions.remove(nameOf(subscription.clientId, request));
        }
    }

    private static SubscriptionName nameOf(String clientId, Subscribing request) {
        return new SubscriptionName(request.durable(), clientId, request.name());
    }

    /** The topic of a durable subscription of the connection's client identifier, or null. */
    String durableSubscriptionTopic(LocalConnection connection, String name, boolean global) {
        lock.lock();
        try {
            Subscription subscription =
                    namedSubscriptions.get(new SubscriptionName(true, global ? null : connection.clientId, name));
            return subscription == null ? null : subscription.request.topic();
        } finally {
            lock.unlock();
        }
    }

    /** Deletes a durable subscription; fails while it has an active consumer (§8.3.5). */
    void unsubscribe(LocalConnection connection, String name, boolean global) throws JMSException {
        lock.lock();
        try {
            checkOpen(connection);
            Subscription subscription =
                    namedSubscriptions.get(new SubscriptionName(true, global ? null : connection.clientId, name));
            if (subscription == null) {
                throw new InvalidDestinationException("there is no durable subscription named " + name);
            }
            if (subscription.storage.consumers > 0) {
                throw new JMSException("durable subscription " + name + " has an active consumer");
            }
            remove(subscription);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the consumer; returns once its receive in progress, if any, has completed (§8.8).
     * Every caller waits for that, including one that finds the consumer already closing.
     */
    void detach(Consumer consumer) {
        lock.lock();
        try {
            consumer.closed = true;
            consumer.storage.changed.signalAll();
            while (consumer.receiving > 0) {
                consumer.storage.changed.awaitUninterruptibly();
            }
            if (!consumer.detached) {
                consumer.detached = true;
                consumer.connection.consumers.remove(consumer);
                consumer.storage.consumers--;
                if (consumer.view != null && --consumer.view.consumers == 0) {
                    consumer.storage.views.remove(consumer.selector);
                }
                Subscription subscription = consumer.subscription;
                // A non-durable subscription ends with its last consumer (§8.3.1, §8.3.2).
                if (subscription != null && !subscription.request.durable() && consumer.storage.consumers == 0
                        && !subscription.storage.deleted) {
                    remove(subscription);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Says whether the consumer has a listener: what {@link BrokerConnection.Consumer#take} waits on. */
    long listen(Consumer consumer, boolean listening) {
        lock.lock();
        try {
            if (consumer.listening && !listening) {
                consumer.period++;
                // The one change of it a take waits for: it only ever waits while listening.
                consumer.storage.changed.signalAll();
            }
            consumer.listening = listening;
            return consumer.period;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Takes the next message the consumer is attached to.
     *
     * @param timeoutNanos how long to wait for one: 0 not at all, {@link #FOREVER} without limit
     * @param settleOnDelivery whether the delivery is settled at once; otherwise the broker keeps
     *        it among the unsettled deliveries of the connection
     * @param ofPeriod the period of listening the caller serves, to give up as soon as it is
     *        over, or {@link #NO_PERIOD} for a receive that does not depend on a listener
     * @return the delivery, or {@code null} if the timeout expires, the consumer is closed or
     *         the period is over
     */
    private Delivery receive(Consumer consumer, long timeoutNanos, boolean settleOnDelivery, long ofPeriod)
            throws JMSException {
        // The timeout runs from the call, not from the moment the lock is obtained.
        long start = System.nanoTime();
        lock.lock();
        try {
            consumer.receiving++;
            while (!consumer.closed
                    && (ofPeriod == NO_PERIOD || consumer.listening && consumer.period == ofPeriod)) {
                long now = System.currentTimeMillis();
                clock = Math.max(clock, now);
                long nextDeliveryTime = Long.MAX_VALUE;
                // A stopped connection delivers nothing, but its receive timers keep running (§6.1.5).
                if (consumer.connection.started) {
                    // Through its view a consumer only meets the messages its selector accepts.
                    TreeSet<Stored> candidates =
                            consumer.view != null ? consumer.view.messages : consumer.storage.messages;
                    consumer.storage.sweepExpired(now);
                    // Whatever is not due is aside: the wait below ends when the next of it is.
                    nextDeliveryTime = consumer.storage.release(now);
                    Stored taken = null;
                    List<Stored> gone = new ArrayList<>();
                    for (Stored next : candidates) {
                        examined++;
                        TassisMessage message = next.message;
                        if (noLongerSelected(consumer.subscription, next)) {
                            gone.add(next);
                        } else if (consumer.view == null && consumer.selector != null
                                && !consumer.selector.matches(message, next.deliveryCount + 1)) {
                            // A selector on the delivery count, checked each time. Not for this
                            // consumer: the message stays where it is.
                            continue;
                        } else {
                            taken = next;
                            break;
                        }
                    }
                    gone.forEach(consumer.storage::remove);
                    if (taken != null) {
                        // Each delivery gets its own copy: the stored message may be delivered again.
                        TassisMessage delivered = taken.message.copy();
                        delivered.markDelivered(++taken.deliveryCount);
                        consumer.storage.remove(taken);
                        Delivery delivery = new Delivery(taken, delivered);
                        if (!settleOnDelivery) {
                            consumer.connection.unsettled.add(delivery);
                        }
                        return delivery;
                    }
                }
                long wait = timeoutNanos == FOREVER ? FOREVER : timeoutNanos - (System.nanoTime() - start);
                if (wait <= 0) {
                    return null;
                }
                if (nextDeliveryTime != Long.MAX_VALUE) {
                    wait = Math.min(wait, TimeUnit.MILLISECONDS.toNanos(nextDeliveryTime - now));
                }
                waiting++;
                try {
                    if (wait == FOREVER) {
                        consumer.storage.changed.await();
                    } else {
                        consumer.storage.changed.awaitNanos(wait);
                    }
                } finally {
                    waiting--;
                }
                awakenings++;
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JMSException("interrupted while waiting for a message");
        } finally {
            consumer.receiving--;
            if (consumer.closed) {
                // Whoever closes the consumer waits for this (detach), and nobody else does.
                consumer.storage.changed.signalAll();
            }
            lock.unlock();
        }
    }

    /**
     * Returns copies of the messages a consumer with that selector could take from the queue
     * right now, in delivery order.
     */
    List<TassisMessage> browse(String queue, Selector selector) {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            List<TassisMessage> messages = new ArrayList<>();
            Storage storage = queues.get(queue);
            if (storage == null) {
                return messages;
            }
            // §7.9: a browser must not return a message before its delivery time, and those are aside.
            storage.release(now);
            for (Stored stored : storage.messages) {
                TassisMessage message = stored.message;
                if (!isExpired(message, now)
                        && (selector == null || selector.matches(message, stored.deliveryCount + 1))) {
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

    /**
     * Returns how many times a receive stopped waiting, to take a message or to find that it
     * had been woken for nothing: what tells a wake-up nobody needed.
     */
    long awakenings() {
        lock.lock();
        try {
            return awakenings;
        } finally {
            lock.unlock();
        }
    }

    /** Returns how many messages the receives have looked at so far, taken or passed over. */
    long examined() {
        lock.lock();
        try {
            return examined;
        } finally {
            lock.unlock();
        }
    }

    /** Returns how many receives are waiting for a message at this moment. */
    int waiting() {
        lock.lock();
        try {
            return waiting;
        } finally {
            lock.unlock();
        }
    }

    /** Returns how many consumers are attached to the queue. */
    public int consumerCount(String queue) {
        lock.lock();
        try {
            Storage storage = queues.get(queue);
            return storage == null ? 0 : storage.consumers;
        } finally {
            lock.unlock();
        }
    }

    /** Returns how many messages the queue holds, delayed ones included and delivered ones excluded. */
    public int messageCount(String queue) {
        lock.lock();
        try {
            Storage storage = queues.get(queue);
            return storage == null ? 0 : storage.messages.size() + storage.delayed.size();
        } finally {
            lock.unlock();
        }
    }

    /** Returns how many subscriptions the topic has. */
    public int subscriptionCount(String topic) {
        lock.lock();
        try {
            return subscriptionsByTopic.getOrDefault(topic, List.of()).size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * A subscription selects its messages when they are published, which settles the matter
     * unless its selector looks at the delivery count: a message given back may then no longer
     * belong to the subscription, and leaves it.
     */
    private static boolean noLongerSelected(Subscription subscription, Stored stored) {
        if (subscription == null || stored.deliveryCount == 0) {
            return false;
        }
        Selector selector = subscription.request.selector();
        return selector != null && selector.referencesDeliveryCount()
                && !selector.matches(stored.message, stored.deliveryCount + 1);
    }

    private void checkExists(String destination, boolean topic, boolean temporary)
            throws InvalidDestinationException {
        if (temporary && !(topic ? temporaryTopics : temporaryQueues).contains(destination)) {
            throw new InvalidDestinationException("temporary destination " + destination + " has been deleted");
        }
    }

    private Storage storageOf(String queue) {
        return queues.computeIfAbsent(queue, created -> new Storage(lock.newCondition()));
    }

    private static boolean isExpired(TassisMessage message, long now) {
        return message.getJMSExpiration() != 0 && message.getJMSExpiration() <= now;
    }
}
