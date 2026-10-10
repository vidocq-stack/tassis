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
import io.vidocq.tassis.core.TassisTopic;
import io.vidocq.tassis.core.internal.message.TassisBytesMessage;
import io.vidocq.tassis.core.internal.message.TassisMapMessage;
import io.vidocq.tassis.core.internal.message.TassisMessage;
import io.vidocq.tassis.core.internal.message.TassisObjectMessage;
import io.vidocq.tassis.core.internal.message.TassisStreamMessage;
import io.vidocq.tassis.core.internal.message.TassisTextMessage;
import jakarta.jms.BytesMessage;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The simplified API's context (Jakarta Messaging 3.1 §2.8): a connection and a
 * session in one object. Contexts created from one another share the connection,
 * which is closed with the last of them (§6.1.8 of the simplified API,
 * {@code JMSContext.close}).
 *
 * <p>The session is created with the first thing that needs one, so that
 * {@code setClientID} can still be called on a new context (§6.1.2).
 */
public final class TassisContext implements JMSContext {

    private final TassisConnection connection;
    // How many open contexts use the connection.
    private final AtomicInteger contexts;
    private final int sessionMode;
    // Guards 'session' and 'closing'. Never held while waiting for anything: a callback of this
    // context may need it, and closing the session waits for the callbacks.
    private final ReentrantLock lifecycle = new ReentrantLock();
    private TassisSession session;
    // A close is under way: no session is created any more, the existing one stays usable.
    private boolean closing;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean autoStart = true;

    /** A context on a connection of its own. */
    public TassisContext(TassisConnection connection, int sessionMode) {
        this(connection, new AtomicInteger(1), sessionMode);
    }

    /** @param contexts the count of the connection's open contexts, this one already included */
    private TassisContext(TassisConnection connection, AtomicInteger contexts, int sessionMode) {
        this.connection = connection;
        this.contexts = contexts;
        this.sessionMode = sessionMode;
    }

    /** Checks a session mode before anything is allocated for it. */
    public static void checkSessionMode(int sessionMode) {
        switch (sessionMode) {
            case AUTO_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE, SESSION_TRANSACTED -> {
            }
            default -> throw new JMSRuntimeException("unknown session mode " + sessionMode);
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateRuntimeException("the context is closed");
        }
    }

    /**
     * The context's session, created on first use. While the context is being closed its session
     * stays available to the callbacks still running (JMSContext.close), but none is created.
     */
    TassisSession session() {
        lifecycle.lock();
        try {
            checkOpen();
            if (session == null) {
                if (closing) {
                    throw new IllegalStateRuntimeException("the context is being closed");
                }
                session = (TassisSession) Unchecked.get(() -> connection.createSession(sessionMode));
            }
            return session;
        } finally {
            lifecycle.unlock();
        }
    }

    // ---- the connection half -----------------------------------------------

    @Override
    public JMSContext createContext(int sessionMode) {
        checkOpen();
        checkSessionMode(sessionMode);
        // Counted in one step with the check that the connection still has an open context:
        // otherwise a concurrent close of the last one would leave this context on a closed connection.
        if (contexts.updateAndGet(open -> open == 0 ? 0 : open + 1) == 0) {
            throw new IllegalStateRuntimeException("the context is closed");
        }
        return new TassisContext(connection, contexts, sessionMode);
    }

    @Override
    public String getClientID() {
        checkOpen();
        return Unchecked.get(connection::getClientID);
    }

    @Override
    public void setClientID(String clientID) {
        checkOpen();
        Unchecked.run(() -> connection.setClientID(clientID));
    }

    @Override
    public ConnectionMetaData getMetaData() {
        checkOpen();
        return Unchecked.get(connection::getMetaData);
    }

    @Override
    public ExceptionListener getExceptionListener() {
        checkOpen();
        return Unchecked.get(connection::getExceptionListener);
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        checkOpen();
        Unchecked.run(() -> connection.setExceptionListener(listener));
    }

    @Override
    public void start() {
        checkOpen();
        Unchecked.run(connection::start);
    }

    @Override
    public void stop() {
        checkOpen();
        Unchecked.run(connection::stop);
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        checkOpen();
        this.autoStart = autoStart;
    }

    @Override
    public boolean getAutoStart() {
        checkOpen();
        return autoStart;
    }

    /**
     * Closes the session, then the connection if this was its last open context. A callback
     * cannot close the context it runs on: the context then stays open.
     */
    @Override
    public void close() {
        TassisSession toClose;
        lifecycle.lock();
        try {
            if (closed.get()) {
                return;
            }
            closing = true;
            toClose = session;
        } finally {
            lifecycle.unlock();
        }
        try {
            // Waits for the callbacks in progress, which may be using this context: no lock is held.
            if (toClose != null) {
                Unchecked.run(toClose::close);
            }
        } catch (RuntimeException refused) {
            lifecycle.lock();
            try {
                closing = false;
            } finally {
                lifecycle.unlock();
            }
            throw refused;
        }
        if (closed.compareAndSet(false, true) && contexts.decrementAndGet() == 0) {
            Unchecked.run(connection::close);
        }
    }

    // ---- messages --------------------------------------------------------

    @Override
    public BytesMessage createBytesMessage() {
        checkOpen();
        return new TassisBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        checkOpen();
        return new TassisMapMessage();
    }

    @Override
    public Message createMessage() {
        checkOpen();
        return new TassisMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        checkOpen();
        return new TassisObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        ObjectMessage message = createObjectMessage();
        Unchecked.run(() -> message.setObject(object));
        return message;
    }

    @Override
    public StreamMessage createStreamMessage() {
        checkOpen();
        return new TassisStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() {
        checkOpen();
        return new TassisTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) {
        TextMessage message = createTextMessage();
        Unchecked.run(() -> message.setText(text));
        return message;
    }

    // ---- the session half ------------------------------------------------

    @Override
    public boolean getTransacted() {
        checkOpen();
        return sessionMode == SESSION_TRANSACTED;
    }

    @Override
    public int getSessionMode() {
        checkOpen();
        return sessionMode;
    }

    @Override
    public void commit() {
        Unchecked.run(session()::commit);
    }

    @Override
    public void rollback() {
        Unchecked.run(session()::rollback);
    }

    @Override
    public void recover() {
        Unchecked.run(session()::recover);
    }

    @Override
    public void acknowledge() {
        Unchecked.run(session()::acknowledge);
    }

    @Override
    public JMSProducer createProducer() {
        checkOpen();
        return new TassisJMSProducer(this);
    }

    /** Wraps a consumer of the session and, unless told otherwise, starts the connection for it. */
    private JMSConsumer consumer(Unchecked.Call<MessageConsumer> creation) {
        TassisJMSConsumer consumer = new TassisJMSConsumer((TassisMessageConsumer) Unchecked.get(creation));
        if (autoStart) {
            Unchecked.run(connection::start);
        }
        return consumer;
    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        return consumer(() -> session().createConsumer(destination));
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        return consumer(() -> session().createConsumer(destination, messageSelector));
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        return consumer(() -> session().createConsumer(destination, messageSelector, noLocal));
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return consumer(() -> session().createDurableConsumer(topic, name));
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        return consumer(() -> session().createDurableConsumer(topic, name, messageSelector, noLocal));
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return consumer(() -> session().createSharedDurableConsumer(topic, name));
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        return consumer(() -> session().createSharedDurableConsumer(topic, name, messageSelector));
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return consumer(() -> session().createSharedConsumer(topic, sharedSubscriptionName));
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        return consumer(() -> session().createSharedConsumer(topic, sharedSubscriptionName, messageSelector));
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return Unchecked.get(() -> session().createBrowser(queue));
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        return Unchecked.get(() -> session().createBrowser(queue, messageSelector));
    }

    @Override
    public Queue createQueue(String queueName) {
        checkOpen();
        if (queueName == null || queueName.isEmpty()) {
            throw new JMSRuntimeException("a queue name must not be null or empty");
        }
        return new TassisQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) {
        checkOpen();
        if (topicName == null || topicName.isEmpty()) {
            throw new JMSRuntimeException("a topic name must not be null or empty");
        }
        return new TassisTopic(topicName);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        return Unchecked.get(session()::createTemporaryQueue);
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        return Unchecked.get(session()::createTemporaryTopic);
    }

    @Override
    public void unsubscribe(String name) {
        Unchecked.run(() -> session().unsubscribe(name));
    }
}
