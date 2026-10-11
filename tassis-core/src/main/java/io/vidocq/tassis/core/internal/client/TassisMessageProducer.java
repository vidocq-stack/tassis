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
package io.vidocq.tassis.core.internal.client;

import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import io.vidocq.tassis.core.internal.message.Messages;
import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;

/**
 * Sends messages to a queue or publishes them to a topic (Jakarta Messaging 3.1
 * §7). Created with a destination, it sends there only; created without one,
 * each send names its destination.
 */
final class TassisMessageProducer implements QueueSender, TopicPublisher, SessionChild {

    private final TassisSession session;
    private final Destination destination;
    private volatile boolean closed;
    private boolean disableMessageId;
    private boolean disableMessageTimestamp;
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;

    TassisMessageProducer(TassisSession session, Destination destination) {
        this.session = session;
        this.destination = destination;
    }

    private void checkOpen() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("the producer is closed");
        }
    }

    // The two hints are recorded and ignored: every message gets an ID and a timestamp (§3.4.3, §3.4.4).

    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        checkOpen();
        disableMessageId = value;
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkOpen();
        return disableMessageId;
    }

    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        checkOpen();
        disableMessageTimestamp = value;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkOpen();
        return disableMessageTimestamp;
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkOpen();
        checkDeliveryMode(deliveryMode);
        this.deliveryMode = deliveryMode;
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        checkOpen();
        return deliveryMode;
    }

    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        checkOpen();
        checkPriority(defaultPriority);
        priority = defaultPriority;
    }

    @Override
    public int getPriority() throws JMSException {
        checkOpen();
        return priority;
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        checkOpen();
        this.timeToLive = timeToLive;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        checkOpen();
        return timeToLive;
    }

    @Override
    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        checkOpen();
        this.deliveryDelay = deliveryDelay;
    }

    @Override
    public long getDeliveryDelay() throws JMSException {
        checkOpen();
        return deliveryDelay;
    }

    @Override
    public Destination getDestination() throws JMSException {
        checkOpen();
        return destination;
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkOpen();
        return destination instanceof Queue queue ? queue : null;
    }

    @Override
    public Topic getTopic() throws JMSException {
        checkOpen();
        return destination instanceof Topic topic ? topic : null;
    }

    /** Returns once the asynchronous sends in progress have completed (§7.3.4). */
    @Override
    public void close() throws JMSException {
        session.awaitCompletions("close");
        shut();
    }

    @Override
    public void shut() {
        closed = true;
        session.forget(this);
    }

    @Override
    public void send(Message message) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkOpen();
        if (destination == null) {
            throw new UnsupportedOperationException("this producer has no destination: name one on each send");
        }
        deliver(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        checkOpen();
        if (this.destination != null) {
            throw new UnsupportedOperationException("this producer is bound to " + this.destination);
        }
        deliver(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        send((Destination) queue, message);
    }

    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        send((Destination) queue, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Message message) throws JMSException {
        send(message);
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        send((Destination) topic, message);
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        send((Destination) topic, message, deliveryMode, priority, timeToLive);
    }

    private void deliver(Destination target, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        if (message == null) {
            throw new MessageFormatException("the message is null");
        }
        Destinations.Address address = Destinations.resolve(target);
        checkDeliveryMode(deliveryMode);
        checkPriority(priority);
        long now = System.currentTimeMillis();
        // The headers are set on the client's own message too (§3.4), then the broker gets a copy (§3.9).
        message.setJMSDestination(target);
        message.setJMSDeliveryMode(deliveryMode);
        message.setJMSPriority(priority);
        message.setJMSTimestamp(now);
        message.setJMSExpiration(timeToLive == 0 ? 0 : now + timeToLive);
        message.setJMSDeliveryTime(now + deliveryDelay);
        message.setJMSMessageID(session.link().nextMessageId());
        session.send(new BrokerConnection.Outgoing(address.name(), address.topic(), address.temporary(),
                Messages.snapshot(message)));
    }

    private static void checkDeliveryMode(int deliveryMode) throws JMSException {
        if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new JMSException("unknown delivery mode " + deliveryMode);
        }
    }

    private static void checkPriority(int priority) throws JMSException {
        if (priority < 0 || priority > 9) {
            throw new JMSException("priority " + priority + " is outside 0..9");
        }
    }

    // ---- asynchronous send (§7.3) --------------------------------------

    /*
     * A send to the in-VM broker is complete when it returns, so the whole of it is done in the
     * calling thread, where any failure is thrown (§7.3.2). What is asynchronous is the
     * notification, which the session makes from a thread of its own (§7.3.8).
     */

    @Override
    public void send(Message message, CompletionListener completionListener) throws JMSException {
        requireListener(completionListener);
        send(message);
        session.completed(() -> completionListener.onCompletion(message));
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive,
            CompletionListener completionListener) throws JMSException {
        requireListener(completionListener);
        send(message, deliveryMode, priority, timeToLive);
        session.completed(() -> completionListener.onCompletion(message));
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener completionListener)
            throws JMSException {
        requireListener(completionListener);
        send(destination, message);
        session.completed(() -> completionListener.onCompletion(message));
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive,
            CompletionListener completionListener) throws JMSException {
        requireListener(completionListener);
        send(destination, message, deliveryMode, priority, timeToLive);
        session.completed(() -> completionListener.onCompletion(message));
    }

    private static void requireListener(CompletionListener completionListener) {
        if (completionListener == null) {
            throw new IllegalArgumentException("the completion listener is null");
        }
    }
}
