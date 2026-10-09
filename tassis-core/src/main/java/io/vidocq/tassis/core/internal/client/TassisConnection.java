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
import jakarta.jms.ConnectionConsumer;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueSession;
import jakarta.jms.ServerSessionPool;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A client's active connection to the in-VM broker (Jakarta Messaging 3.1 §6.1).
 * It is created stopped: its consumers receive nothing until {@link #start()}.
 */
public final class TassisConnection implements QueueConnection {

    private final Broker broker;
    private final List<TassisSession> sessions = new CopyOnWriteArrayList<>();
    private final List<TassisTemporaryQueue> temporaryQueues = new CopyOnWriteArrayList<>();
    // Written under the broker lock, which is where delivery reads them.
    private volatile boolean started;
    private volatile boolean closed;
    private volatile String clientId;
    private volatile ExceptionListener exceptionListener;

    public TassisConnection(Broker broker) {
        this.broker = broker;
    }

    Broker broker() {
        return broker;
    }

    boolean isStarted() {
        return started;
    }

    void forget(TassisSession session) {
        sessions.remove(session);
    }

    TassisTemporaryQueue createTemporaryQueue() throws IllegalStateException {
        TassisTemporaryQueue queue = new TassisTemporaryQueue(this, broker.createTemporaryQueue());
        temporaryQueues.add(queue);
        // Same add-then-check as for sessions: a concurrent close must not miss this queue.
        if (closed) {
            queue.dispose();
            throw new IllegalStateException("the connection is closed");
        }
        return queue;
    }

    void forget(TassisTemporaryQueue queue) {
        temporaryQueues.remove(queue);
    }

    private void checkOpen() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("the connection is closed");
        }
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return openSession(transacted ? Session.SESSION_TRANSACTED : acknowledgeMode);
    }

    @Override
    public Session createSession(int sessionMode) throws JMSException {
        return openSession(sessionMode);
    }

    @Override
    public Session createSession() throws JMSException {
        return openSession(Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return openSession(transacted ? Session.SESSION_TRANSACTED : acknowledgeMode);
    }

    private TassisSession openSession(int sessionMode) throws JMSException {
        checkOpen();
        switch (sessionMode) {
            case Session.AUTO_ACKNOWLEDGE, Session.DUPS_OK_ACKNOWLEDGE -> {
            }
            case Session.CLIENT_ACKNOWLEDGE, Session.SESSION_TRANSACTED ->
                throw new UnsupportedOperationException(
                        "client-acknowledged and transacted sessions: see ROADMAP M3");
            default -> throw new JMSException("unknown session mode " + sessionMode);
        }
        TassisSession session = new TassisSession(this, sessionMode);
        sessions.add(session);
        // A concurrent close either sees the session in the list, or has raised the flag checked here.
        if (closed) {
            session.close();
            throw new IllegalStateException("the connection is closed");
        }
        return session;
    }

    @Override
    public String getClientID() throws JMSException {
        checkOpen();
        return clientId;
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        checkOpen();
        clientId = clientID;
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkOpen();
        return TassisConnectionMetaData.INSTANCE;
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkOpen();
        return exceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkOpen();
        exceptionListener = listener;
    }

    @Override
    public void start() throws JMSException {
        checkOpen();
        broker.update(() -> started = true);
    }

    /** Returns once delivery is paused: the flag is flipped under the lock every delivery holds. */
    @Override
    public void stop() throws JMSException {
        checkOpen();
        broker.update(() -> started = false);
    }

    /**
     * Closing a closed connection does nothing (§6.1.8). Concurrent callers all go through the
     * sessions, so each returns only once the pending receives have completed.
     */
    @Override
    public void close() {
        broker.update(() -> {
            closed = true;
            started = false;
        });
        for (TassisSession session : sessions) {
            session.close();
        }
        for (TassisTemporaryQueue queue : temporaryQueues) {
            queue.dispose();
            temporaryQueues.remove(queue);
        }
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException("createConnectionConsumer: optional (§11), not on the ROADMAP");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException("createConnectionConsumer: optional (§11), not on the ROADMAP");
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException("createSharedConnectionConsumer: optional (§11), not on the ROADMAP");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException("createDurableConnectionConsumer: optional (§11), not on the ROADMAP");
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException(
                "createSharedDurableConnectionConsumer: optional (§11), not on the ROADMAP");
    }
}
