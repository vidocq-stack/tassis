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
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import java.util.function.BooleanSupplier;

/**
 * The in-VM broker (Jakarta Messaging 3.1 chapter 4).
 *
 * <p>Messages wait in a {@link Storage}: one per queue, and one per topic subscription. A
 * queue's storage is fed by the sends to the queue; a subscription's storage is fed by the
 * publications to its topic. From there on the two behave alike: consumers attached to a
 * storage compete for its messages.
 *
 * <p>All broker state, and the delivery-related state of its clients (connection started,
 * consumer closed), is guarded by one lock. Whatever changes that state signals
 * {@link #changed}, which is the only thing a waiting receive listens to.
 */
public final class Broker {

    /** Timeout value of {@link #receive} that never expires. */
    public static final long FOREVER = Long.MAX_VALUE;

    private static final ConcurrentHashMap<String, Broker> BROKERS = new ConcurrentHashMap<>();

    /**
     * A message on its way to a destination.
     *
     * @param publisher identifies who sends it, for the {@code noLocal} subscriptions of a topic
     */
    public record Outgoing(String destination, boolean topic, boolean temporary, Object publisher,
            TassisMessage message) {
    }

    /**
     * What a consumer asks of a topic (§8.3).
     *
     * @param name the subscription name, or {@code null} for an unshared non-durable subscription
     * @param clientId the client identifier the name is scoped by, or {@code null} if unset
     * @param subscriber identifies who subscribes, to be compared with {@link Outgoing#publisher}
     */
    public record Subscribing(String topic, boolean temporary, String name, boolean durable, boolean shared,
            String clientId, boolean noLocal, Object subscriber) {

        /** Whether the two requests describe the same subscription (§8.3.3, §8.3.4). */
        private boolean describesTheSameAs(Subscribing other) {
            return topic.equals(other.topic) && noLocal == other.noLocal;
        }
    }

    /** A consumer's attachment to a queue or to a subscription. */
    public static final class Consumer {

        private final Storage storage;
        private final Subscription subscription;
        private final BooleanSupplier started;
        private boolean closed;
        private boolean detached;
        // A count, not a flag: a misused consumer may be in several receives at once.
        private int receiving;

        private Consumer(Storage storage, Subscription subscription, BooleanSupplier started) {
            this.storage = storage;
            this.subscription = subscription;
            this.started = started;
        }
    }

    /** A message handed to a consumer, which the consumer's session either settles or gives back. */
    public static final class Delivery {

        private final Stored stored;
        private final TassisMessage message;

        private Delivery(Stored stored, TassisMessage message) {
            this.stored = stored;
            this.message = message;
        }

        /** The copy of the message made for this delivery. */
        public TassisMessage message() {
            return message;
        }
    }

    /** The messages waiting for the consumers of one queue or of one subscription. */
    private static final class Storage {

        final TreeSet<Stored> messages = new TreeSet<>(Stored.DELIVERY_ORDER);
        int consumers;
        // Set when the queue or subscription is deleted: a message given back then goes nowhere.
        boolean deleted;
    }

    private static final class Stored {

        /** Highest priority first, then the order the messages were sent in (§3.4.10, §4.1). */
        static final Comparator<Stored> DELIVERY_ORDER = Comparator
                .comparingInt((Stored stored) -> -stored.message.getJMSPriority())
                .thenComparingLong(stored -> stored.sequence);

        final Storage storage;
        // Shared by every subscription the message was published to: it is never modified.
        final TassisMessage message;
        // Kept across redeliveries, so that a message given back returns to its place.
        final long sequence;
        int deliveryCount;

        Stored(Storage storage, TassisMessage message, long sequence) {
            this.storage = storage;
            this.message = message;
            this.sequence = sequence;
        }
    }

    private static final class Subscription {

        final Subscribing request;
        final Storage storage = new Storage();

        Subscription(Subscribing request) {
            this.request = request;
        }
    }

    /** Durable and shared non-durable subscriptions are two separate namespaces (§8.3). */
    private record SubscriptionName(boolean durable, String clientId, String name) {
    }

    private final String name;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<String, Storage> queues = new HashMap<>();
    private final Set<String> temporaryQueues = new HashSet<>();
    private final Set<String> temporaryTopics = new HashSet<>();
    private final Map<String, List<Subscription>> subscriptionsByTopic = new HashMap<>();
    private final Map<SubscriptionName, Subscription> namedSubscriptions = new HashMap<>();
    private final Set<String> clientIds = new HashSet<>();
    private final AtomicLong messageIds = new AtomicLong();
    private final AtomicLong temporaryIds = new AtomicLong();
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

    // ---- client identifiers ----------------------------------------------

    /** Reserves a client identifier for one connection (§6.1.2). */
    public void claimClientId(String clientId) throws InvalidClientIDException {
        lock.lock();
        try {
            if (!clientIds.add(clientId)) {
                throw new InvalidClientIDException("client identifier " + clientId + " is in use by another connection");
            }
        } finally {
            lock.unlock();
        }
    }

    public void releaseClientId(String clientId) {
        lock.lock();
        try {
            clientIds.remove(clientId);
        } finally {
            lock.unlock();
        }
    }

    // ---- temporary destinations ------------------------------------------

    /** Creates a temporary queue and returns its name. */
    public String createTemporaryQueue() {
        return createTemporary(temporaryQueues, "temporary-queue:");
    }

    /** Creates a temporary topic and returns its name. */
    public String createTemporaryTopic() {
        return createTemporary(temporaryTopics, "temporary-topic:");
    }

    private String createTemporary(Set<String> names, String prefix) {
        String created = prefix + name + "-" + temporaryIds.incrementAndGet();
        lock.lock();
        try {
            names.add(created);
            return created;
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

    /** Returns whether the temporary topic of that name still exists. */
    public boolean hasTemporaryTopic(String topic) {
        lock.lock();
        try {
            return temporaryTopics.contains(topic);
        } finally {
            lock.unlock();
        }
    }

    /** Deletes a temporary queue and its messages; fails while a consumer is attached to it (§6.2.2). */
    public void deleteTemporaryQueue(String queue) throws JMSException {
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

    /** Deletes a temporary queue whose connection is closing, whatever is attached to it. */
    public void dropTemporaryQueue(String queue) {
        lock.lock();
        try {
            temporaryQueues.remove(queue);
            Storage storage = queues.remove(queue);
            if (storage != null) {
                storage.deleted = true;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Deletes a temporary topic; fails while a consumer is subscribed to it (§6.2.2). Its
     * subscriptions without a consumer, which can only be durable ones, go with it.
     */
    public void deleteTemporaryTopic(String topic) throws JMSException {
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

    /** Deletes a temporary topic whose connection is closing, with its subscriptions. */
    public void dropTemporaryTopic(String topic) {
        lock.lock();
        try {
            temporaryTopics.remove(topic);
            for (Subscription subscription : List.copyOf(subscriptionsByTopic.getOrDefault(topic, List.of()))) {
                remove(subscription);
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    // ---- sending ---------------------------------------------------------

    /**
     * Adds messages, which the broker now owns, to their destinations: all of them, or none if
     * one names a temporary destination that no longer exists. A message sent to a queue waits
     * there; a message published to a topic is added to each of its subscriptions, and to none
     * if it has none (§4.2.3).
     */
    public void send(Collection<Outgoing> messages) throws InvalidDestinationException {
        lock.lock();
        try {
            for (Outgoing outgoing : messages) {
                checkExists(outgoing.destination(), outgoing.topic(), outgoing.temporary());
            }
            for (Outgoing outgoing : messages) {
                if (!outgoing.topic()) {
                    store(storageOf(outgoing.destination()), outgoing.message());
                    continue;
                }
                for (Subscription subscription : subscriptionsByTopic.getOrDefault(outgoing.destination(), List.of())) {
                    // §8.3.1: a noLocal subscription ignores what its own connection publishes.
                    boolean local = Objects.equals(subscription.request.subscriber(), outgoing.publisher());
                    if (!(subscription.request.noLocal() && local)) {
                        store(subscription.storage, outgoing.message());
                    }
                }
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void store(Storage storage, TassisMessage message) {
        storage.messages.add(new Stored(storage, message, sequence++));
    }

    /**
     * Puts back messages that were delivered and not settled. Their next delivery will be
     * marked as a redelivery. A message of a deleted queue or subscription is dropped.
     */
    public void redeliver(Collection<Delivery> deliveries) {
        lock.lock();
        try {
            for (Delivery delivery : deliveries) {
                Storage storage = delivery.stored.storage;
                if (!storage.deleted) {
                    storage.messages.add(delivery.stored);
                }
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    // ---- consuming -------------------------------------------------------

    /**
     * Attaches a consumer to a queue.
     *
     * @param temporary whether {@code queue} names a temporary queue, which must then still exist
     * @param started tells whether the consumer's connection is started; read under the broker lock
     */
    public Consumer attach(String queue, boolean temporary, BooleanSupplier started)
            throws InvalidDestinationException {
        lock.lock();
        try {
            checkExists(queue, false, temporary);
            Storage storage = storageOf(queue);
            storage.consumers++;
            return new Consumer(storage, null, started);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Attaches a consumer to a topic subscription, creating, reusing or replacing the
     * subscription as §8.3 prescribes for its kind.
     *
     * @param started tells whether the consumer's connection is started; read under the broker lock
     */
    public Consumer subscribe(Subscribing request, BooleanSupplier started) throws JMSException {
        lock.lock();
        try {
            checkExists(request.topic(), true, request.temporary());
            Subscription subscription = request.name() == null ? add(request) : named(request);
            subscription.storage.consumers++;
            return new Consumer(subscription.storage, subscription, started);
        } finally {
            lock.unlock();
        }
    }

    private Subscription named(Subscribing request) throws JMSException {
        Subscription existing = namedSubscriptions.get(nameOf(request));
        if (existing == null) {
            return add(request);
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
                    + " has active consumers and cannot be changed to another topic");
        }
        // Same name, another topic: the old subscription is deleted and a new one starts.
        remove(existing);
        return add(request);
    }

    private Subscription add(Subscribing request) {
        Subscription subscription = new Subscription(request);
        subscriptionsByTopic.computeIfAbsent(request.topic(), topic -> new ArrayList<>()).add(subscription);
        if (request.name() != null) {
            namedSubscriptions.put(nameOf(request), subscription);
        }
        return subscription;
    }

    private void remove(Subscription subscription) {
        Subscribing request = subscription.request;
        subscription.storage.deleted = true;
        subscription.storage.messages.clear();
        List<Subscription> ofTopic = subscriptionsByTopic.get(request.topic());
        ofTopic.remove(subscription);
        if (ofTopic.isEmpty()) {
            subscriptionsByTopic.remove(request.topic());
        }
        if (request.name() != null) {
            namedSubscriptions.remove(nameOf(request));
        }
    }

    private static SubscriptionName nameOf(Subscribing request) {
        return new SubscriptionName(request.durable(), request.clientId(), request.name());
    }

    /** Deletes a durable subscription; fails while it has an active consumer (§8.3.5). */
    public void unsubscribe(String clientId, String name) throws JMSException {
        lock.lock();
        try {
            Subscription subscription = namedSubscriptions.get(new SubscriptionName(true, clientId, name));
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
    public void detach(Consumer consumer) {
        lock.lock();
        try {
            consumer.closed = true;
            changed.signalAll();
            while (consumer.receiving > 0) {
                changed.awaitUninterruptibly();
            }
            if (!consumer.detached) {
                consumer.detached = true;
                consumer.storage.consumers--;
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

    /**
     * Takes the next message the consumer is attached to. The caller owns the delivery from
     * then on: the message is gone unless it is handed back to {@link #redeliver}.
     *
     * @param timeoutNanos how long to wait for one: 0 not at all, {@link #FOREVER} without limit
     * @return the delivery, or {@code null} if the timeout expires or the consumer is closed
     */
    public Delivery receive(Consumer consumer, long timeoutNanos) throws JMSException {
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
                    Iterator<Stored> stored = consumer.storage.messages.iterator();
                    while (stored.hasNext()) {
                        Stored next = stored.next();
                        TassisMessage message = next.message;
                        if (isExpired(message, now)) {
                            stored.remove();
                        } else if (message.getJMSDeliveryTime() > now) {
                            nextDeliveryTime = Math.min(nextDeliveryTime, message.getJMSDeliveryTime());
                        } else {
                            // Each delivery gets its own copy: the stored message may be delivered again.
                            TassisMessage delivered = message.copy();
                            delivered.markDelivered(++next.deliveryCount);
                            stored.remove();
                            return new Delivery(next, delivered);
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
            Storage storage = queues.get(queue);
            if (storage == null) {
                return messages;
            }
            for (Stored stored : storage.messages) {
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
            Storage storage = queues.get(queue);
            return storage == null ? 0 : storage.consumers;
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

    private void checkExists(String destination, boolean topic, boolean temporary)
            throws InvalidDestinationException {
        if (temporary && !(topic ? temporaryTopics : temporaryQueues).contains(destination)) {
            throw new InvalidDestinationException("temporary destination " + destination + " has been deleted");
        }
    }

    private Storage storageOf(String queue) {
        return queues.computeIfAbsent(queue, created -> new Storage());
    }

    private static boolean isExpired(TassisMessage message, long now) {
        return message.getJMSExpiration() != 0 && message.getJMSExpiration() <= now;
    }
}
