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

import io.vidocq.tassis.core.internal.broker.Broker;
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

/**
 * Sends messages to a queue (Jakarta Messaging 3.1 §7). Created with a
 * destination, it sends there only; created without one, each send names its
 * destination.
 */
final class TassisMessageProducer implements QueueSender, SessionChild {

    private final TassisSession session;
    private final Queue queue;
    private volatile boolean closed;
    private boolean disableMessageId;
    private boolean disableMessageTimestamp;
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;

    TassisMessageProducer(TassisSession session, Queue queue) {
        this.session = session;
        this.queue = queue;
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
        return queue;
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkOpen();
        return queue;
    }

    @Override
    public void close() {
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
        if (queue == null) {
            throw new UnsupportedOperationException("this producer has no destination: name one on each send");
        }
        deliver(queue, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        checkOpen();
        if (queue != null) {
            throw new UnsupportedOperationException("this producer is bound to " + queue);
        }
        deliver(Destinations.queue(destination), message, deliveryMode, priority, timeToLive);
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

    private void deliver(Queue target, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        if (message == null) {
            throw new MessageFormatException("the message is null");
        }
        Destinations.queue(target);
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
        message.setJMSMessageID(session.broker().nextMessageId());
        session.send(new Broker.Outgoing(Destinations.name(target), Destinations.isTemporary(target),
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

    // ---- asynchronous send: ROADMAP M6 ---------------------------------

    @Override
    public void send(Message message, CompletionListener completionListener) {
        throw new UnsupportedOperationException("asynchronous send: see ROADMAP M6");
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive,
            CompletionListener completionListener) {
        throw new UnsupportedOperationException("asynchronous send: see ROADMAP M6");
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener completionListener) {
        throw new UnsupportedOperationException("asynchronous send: see ROADMAP M6");
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive,
            CompletionListener completionListener) {
        throw new UnsupportedOperationException("asynchronous send: see ROADMAP M6");
    }
}
