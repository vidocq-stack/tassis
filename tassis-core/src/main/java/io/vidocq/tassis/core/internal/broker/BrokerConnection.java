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
import jakarta.jms.JMSException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * What a client connection asks of its broker, and all it asks: the client objects know the
 * broker through this interface only. The broker of the JVM implements it today; a broker
 * reached through the network will be another implementation (ROADMAP M11.3).
 *
 * <p>The contract is written for a broker that may be far away, and that must cope with a
 * client that disappears:
 *
 * <ul>
 * <li>the broker reads nothing from the client. What it needs to know (the connection is
 * started, a consumer listens) it is told;
 * <li>the broker keeps every delivery that is not settled. The client settles a delivery
 * ({@link #settle}, {@link #commit}) or gives it back ({@link #redeliver},
 * {@link #returnUndelivered}); {@link #close()} gives back whatever is left;
 * <li>the broker knows who the connection is: nothing in a request says who sends it.
 * </ul>
 */
public interface BrokerConnection {

    /** The timeout of a receive that waits as long as it takes. */
    long FOREVER = Long.MAX_VALUE;

    /** A message on its way to a destination. */
    record Outgoing(String destination, boolean topic, boolean temporary, TassisMessage message) {
    }

    /**
     * What a consumer asks of a topic (§8.3).
     *
     * @param name the subscription name, or {@code null} for an unshared non-durable subscription
     * @param selector what the subscription keeps of the topic's messages, or {@code null} for all
     */
    record Subscribing(String topic, boolean temporary, String name, boolean durable, boolean shared,
            boolean noLocal, Selector selector) {

        /** Whether the two requests describe the same subscription (§8.3.3, §8.3.4). */
        boolean describesTheSameAs(Subscribing other) {
            return topic.equals(other.topic) && noLocal == other.noLocal
                    && Objects.equals(selector, other.selector);
        }
    }

    /** A message handed to a consumer. Unless it was settled on delivery, the broker still holds it. */
    interface Delivery {

        /** How many times the message has been delivered, this delivery included. */
        int deliveryCount();

        /** The copy of the message made for this delivery. */
        TassisMessage message();
    }

    /** A consumer's attachment to a queue or to a subscription. */
    interface Consumer {

        /**
         * Takes the next message.
         *
         * @param timeoutNanos how long to wait for one: 0 not at all, {@link #FOREVER} without limit
         * @param settleOnDelivery whether the delivery is settled as it is made, for a caller that
         *        will neither acknowledge it nor give it back
         * @return the delivery, or {@code null} if the timeout expires or the consumer is closed
         */
        Delivery receive(long timeoutNanos, boolean settleOnDelivery) throws JMSException;

        /**
         * Waits for the next message for as long as the consumer {@linkplain #listen listens}
         * without interruption since {@code period} began.
         *
         * @param period what {@link #listen} returned to whoever started waiting for messages
         * @return the delivery, not settled, or {@code null} if that period is over or the
         *         consumer is closed: in both cases there is nothing more to wait for
         */
        Delivery take(long period) throws JMSException;

        /**
         * Says whether the consumer has a listener to hand messages to; it has none at first.
         *
         * @return the period of listening this opens or continues: a number that changes each
         *         time the consumer stops listening, so that whoever waited for the listener of
         *         one period is not mistaken for who waits for the listener of the next
         */
        long listen(boolean listening);

        /** Closes the consumer; returns once its receive in progress, if any, has completed (§8.8). */
        void close();
    }

    String nextMessageId();

    /** Reserves a client identifier for this connection, until it closes (§6.1.2). */
    void claimClientId(String clientId) throws JMSException;

    /**
     * Starts the delivery of messages to the consumers of the connection (§6.1.4).
     *
     * @return whether that changed anything: false if it was started already, or is closed
     */
    boolean start();

    /** Pauses delivery: once this returns, no receive hands out a message (§6.1.5). */
    void stop();

    boolean isStarted();

    /** Creates a temporary queue, which lasts until it is deleted or the connection closes; returns its name. */
    String createTemporaryQueue() throws JMSException;

    /** Creates a temporary topic, which lasts until it is deleted or the connection closes; returns its name. */
    String createTemporaryTopic() throws JMSException;

    /** Returns whether the temporary queue of that name still exists, whichever connection created it. */
    boolean hasTemporaryQueue(String queue);

    /** Returns whether the temporary topic of that name still exists, whichever connection created it. */
    boolean hasTemporaryTopic(String topic);

    /** Deletes a temporary queue and its messages; fails while a consumer is attached to it (§6.2.2). */
    void deleteTemporaryQueue(String queue) throws JMSException;

    /** Deletes a temporary topic; fails while a consumer is subscribed to it (§6.2.2). */
    void deleteTemporaryTopic(String topic) throws JMSException;

    /** Sends a message, which the broker now owns, outside any transaction. */
    void send(Outgoing message) throws JMSException;

    /**
     * Commits a transaction: sends the messages and settles the deliveries, all of it or, if one
     * of the messages cannot be sent, none of it (§6.2.7).
     */
    void commit(Collection<Outgoing> sends, Collection<? extends Delivery> consumed) throws JMSException;

    /**
     * Attaches a consumer to a queue.
     *
     * @param temporary whether {@code queue} names a temporary queue, which must then still exist
     * @param selector the messages the consumer takes, or {@code null} for all; the others stay
     *        on the queue for other consumers (§4.1.2)
     */
    Consumer attach(String queue, boolean temporary, Selector selector) throws JMSException;

    /**
     * Attaches a consumer to a topic subscription, creating, reusing or replacing the
     * subscription as §8.3 prescribes for its kind.
     */
    Consumer subscribe(Subscribing request) throws JMSException;

    /** Deletes a durable subscription of this connection's client identifier (§8.3.5). */
    void unsubscribe(String name) throws JMSException;

    /**
     * Settles a delivery: the message is consumed for good.
     *
     * @return false if the delivery was no longer this connection's to settle, because it was
     *         settled or given back already, by the client or by the closing of the connection
     */
    boolean settle(Delivery delivery);

    /** Settles deliveries; those that are no longer this connection's are skipped. */
    void settle(Collection<? extends Delivery> deliveries);

    /**
     * Gives back deliveries the application has seen: their next delivery will be marked as a
     * redelivery. A message of a deleted queue or subscription is dropped. Deliveries that are
     * no longer this connection's are skipped.
     */
    void redeliver(Collection<? extends Delivery> deliveries);

    /**
     * Gives back a delivery that never reached the application, as if it had not been made:
     * it does not count. Does nothing if the delivery is no longer this connection's.
     */
    void returnUndelivered(Delivery delivery);

    /**
     * Returns copies of the messages a consumer with that selector could take from the queue
     * right now, in delivery order.
     */
    List<TassisMessage> browse(String queue, Selector selector);

    /**
     * Ends the connection. Its consumers are closed, its unsettled deliveries given back as
     * {@link #redeliver} does, its temporary destinations deleted and its client identifier
     * released. A client that closes in good order has little left for this to do; a client
     * that vanished has all of it. Closing a closed connection does nothing.
     */
    void close();
}
