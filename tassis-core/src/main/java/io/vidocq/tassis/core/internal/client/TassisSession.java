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

import io.vidocq.tassis.core.TassisQueue;
import io.vidocq.tassis.core.internal.broker.Broker;
import io.vidocq.tassis.core.internal.message.TassisBytesMessage;
import io.vidocq.tassis.core.internal.message.TassisMapMessage;
import io.vidocq.tassis.core.internal.message.TassisMessage;
import io.vidocq.tassis.core.internal.message.TassisObjectMessage;
import io.vidocq.tassis.core.internal.message.TassisStreamMessage;
import io.vidocq.tassis.core.internal.message.TassisTextMessage;
import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.QueueReceiver;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;
import jakarta.jms.TransactionRolledBackException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A single-threaded context for producing and consuming messages (Jakarta
 * Messaging 3.1 §6.2).
 *
 * <p>The session owns every message its consumers took and that is not settled yet: the
 * messages of the current transaction, or the unacknowledged ones of a client-acknowledged
 * session. It settles them (commit, acknowledge) or gives them back to the broker (rollback,
 * recover, close). An automatically acknowledged session settles on delivery and owns nothing.
 */
public final class TassisSession implements QueueSession {

    private final TassisConnection connection;
    private final int acknowledgeMode;
    private final boolean transacted;
    private final List<SessionChild> children = new CopyOnWriteArrayList<>();
    // Guards the two lists below, which the session's thread and a concurrent close() both touch.
    private final ReentrantLock work = new ReentrantLock();
    private final List<Broker.Delivery> unsettled = new ArrayList<>();
    private final List<Broker.Outgoing> uncommittedSends = new ArrayList<>();
    private volatile boolean closed;

    TassisSession(TassisConnection connection, int acknowledgeMode) {
        this.connection = connection;
        this.acknowledgeMode = acknowledgeMode;
        this.transacted = acknowledgeMode == SESSION_TRANSACTED;
    }

    /** Sends now, or with the current transaction's commit (§6.2.7). */
    void send(Broker.Outgoing outgoing) throws JMSException {
        if (!transacted) {
            broker().send(List.of(outgoing));
            return;
        }
        work.lock();
        try {
            checkOpen();
            uncommittedSends.add(outgoing);
        } finally {
            work.unlock();
        }
    }

    /**
     * Takes responsibility for a delivery one of this session's consumers obtained, and returns
     * the message to hand to the application: {@code null} if there is no delivery, or if the
     * session was closed in the meantime, in which case the message goes back to its queue.
     */
    Message delivered(Broker.Delivery delivery) {
        if (delivery == null) {
            return null;
        }
        if (!transacted && acknowledgeMode != CLIENT_ACKNOWLEDGE) {
            return delivery.message();
        }
        work.lock();
        try {
            if (closed) {
                broker().redeliver(List.of(delivery));
                return null;
            }
            if (acknowledgeMode == CLIENT_ACKNOWLEDGE) {
                delivery.message().onAcknowledge(this::acknowledgeAll);
            }
            unsettled.add(delivery);
        } finally {
            work.unlock();
        }
        return delivery.message();
    }

    /** {@code Message.acknowledge()}: acknowledges everything this session has consumed (§6.2.10). */
    private void acknowledgeAll() throws IllegalStateException {
        work.lock();
        try {
            checkOpen();
            unsettled.clear();
        } finally {
            work.unlock();
        }
    }

    /** Gives every unsettled message back to the broker, to be redelivered (§6.2.11). */
    private void giveBackUnsettled() {
        broker().redeliver(unsettled);
        unsettled.clear();
    }

    TassisConnection connection() {
        return connection;
    }

    Broker broker() {
        return connection.broker();
    }

    void forget(SessionChild child) {
        children.remove(child);
    }

    void checkOpen() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("the session is closed");
        }
    }

    // ---- messages ------------------------------------------------------

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkOpen();
        return new TassisBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        checkOpen();
        return new TassisMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        checkOpen();
        return new TassisMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkOpen();
        return new TassisObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        ObjectMessage message = createObjectMessage();
        message.setObject(object);
        return message;
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        checkOpen();
        return new TassisStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkOpen();
        return new TassisTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        TextMessage message = createTextMessage();
        message.setText(text);
        return message;
    }

    // ---- acknowledgement and transactions ------------------------------

    @Override
    public boolean getTransacted() throws JMSException {
        checkOpen();
        return transacted;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        checkOpen();
        return acknowledgeMode;
    }

    @Override
    public void commit() throws JMSException {
        checkOpen();
        if (!transacted) {
            throw new IllegalStateException("commit() on a session that is not transacted");
        }
        work.lock();
        try {
            checkOpen();
            try {
                broker().send(uncommittedSends);
            } catch (JMSException e) {
                // Nothing was sent: the whole transaction is rolled back.
                uncommittedSends.clear();
                giveBackUnsettled();
                TransactionRolledBackException rolledBack =
                        new TransactionRolledBackException("the transaction was rolled back: " + e.getMessage());
                rolledBack.setLinkedException(e);
                rolledBack.initCause(e);
                throw rolledBack;
            }
            uncommittedSends.clear();
            unsettled.clear();
        } finally {
            work.unlock();
        }
    }

    @Override
    public void rollback() throws JMSException {
        checkOpen();
        if (!transacted) {
            throw new IllegalStateException("rollback() on a session that is not transacted");
        }
        work.lock();
        try {
            checkOpen();
            uncommittedSends.clear();
            giveBackUnsettled();
        } finally {
            work.unlock();
        }
    }

    /** Only a client-acknowledged session has unacknowledged messages to redeliver. */
    @Override
    public void recover() throws JMSException {
        checkOpen();
        if (transacted) {
            throw new IllegalStateException("recover() on a transacted session");
        }
        work.lock();
        try {
            checkOpen();
            giveBackUnsettled();
        } finally {
            work.unlock();
        }
    }

    /**
     * May be called from any thread (§2.14), and concurrently: every caller goes through the
     * children, whose own close is idempotent and returns only once they are quiet.
     */
    @Override
    public void close() {
        closed = true;
        for (SessionChild child : children) {
            child.close();
        }
        // The transaction in progress is rolled back, and closing acknowledges nothing (§6.1.8).
        work.lock();
        try {
            uncommittedSends.clear();
            giveBackUnsettled();
        } finally {
            work.unlock();
        }
        connection.forget(this);
    }

    // ---- producers and consumers ---------------------------------------

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkOpen();
        return register(new TassisMessageProducer(this, destination == null ? null : Destinations.queue(destination)));
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return (QueueSender) createProducer(queue);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination, null);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        checkOpen();
        Queue queue = Destinations.queue(destination);
        checkNoSelector(messageSelector);
        if (queue instanceof TassisTemporaryQueue temporary && temporary.owner() != connection) {
            throw new JMSException("temporary queue " + temporary + " belongs to another connection");
        }
        return register(new TassisMessageConsumer(this, queue));
    }

    /** {@code noLocal} only applies to topics (§8.3.1): it is ignored on a queue. */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal)
            throws JMSException {
        return createConsumer(destination, messageSelector);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return (QueueReceiver) createConsumer(queue, null);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return (QueueReceiver) createConsumer(queue, messageSelector);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return createBrowser(queue, null);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        checkOpen();
        Queue browsed = Destinations.queue(queue);
        checkNoSelector(messageSelector);
        return register(new TassisQueueBrowser(this, browsed));
    }

    /**
     * Makes the session responsible for closing {@code child}. A concurrent close either sees the
     * child in the list, or has already raised the flag checked here: never neither.
     */
    private <T extends SessionChild> T register(T child) throws IllegalStateException {
        children.add(child);
        if (closed) {
            child.close();
            throw new IllegalStateException("the session is closed");
        }
        return child;
    }

    private static void checkNoSelector(String messageSelector) {
        if (messageSelector != null && !messageSelector.isBlank()) {
            throw new UnsupportedOperationException("message selectors: see ROADMAP M5");
        }
    }

    // ---- destinations --------------------------------------------------

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        checkOpen();
        if (queueName == null || queueName.isEmpty()) {
            throw new JMSException("a queue name must not be null or empty");
        }
        return new TassisQueue(queueName);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        checkOpen();
        return connection.createTemporaryQueue();
    }

    // ---- publish/subscribe: ROADMAP M4 ---------------------------------

    @Override
    public Topic createTopic(String topicName) {
        throw new UnsupportedOperationException("createTopic: see ROADMAP M4");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        throw new UnsupportedOperationException("createTemporaryTopic: see ROADMAP M4");
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        throw new UnsupportedOperationException("createSharedConsumer: see ROADMAP M4");
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        throw new UnsupportedOperationException("createSharedConsumer: see ROADMAP M4");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) {
        throw new UnsupportedOperationException("createDurableSubscriber: see ROADMAP M4");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector,
            boolean noLocal) {
        throw new UnsupportedOperationException("createDurableSubscriber: see ROADMAP M4");
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) {
        throw new UnsupportedOperationException("createDurableConsumer: see ROADMAP M4");
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector,
            boolean noLocal) {
        throw new UnsupportedOperationException("createDurableConsumer: see ROADMAP M4");
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) {
        throw new UnsupportedOperationException("createSharedDurableConsumer: see ROADMAP M4");
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        throw new UnsupportedOperationException("createSharedDurableConsumer: see ROADMAP M4");
    }

    @Override
    public void unsubscribe(String name) {
        throw new UnsupportedOperationException("unsubscribe: see ROADMAP M4");
    }

    // ---- application server facilities: optional (§11) -----------------

    @Override
    public MessageListener getMessageListener() {
        throw new UnsupportedOperationException("Session.getMessageListener: optional (§11), not on the ROADMAP");
    }

    @Override
    public void setMessageListener(MessageListener listener) {
        throw new UnsupportedOperationException("Session.setMessageListener: optional (§11), not on the ROADMAP");
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException("Session.run: optional (§11), not on the ROADMAP");
    }
}
