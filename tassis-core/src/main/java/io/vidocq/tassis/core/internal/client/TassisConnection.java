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
import jakarta.jms.ConnectionConsumer;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueSession;
import jakarta.jms.ServerSessionPool;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicSession;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A client's active connection to a broker (Jakarta Messaging 3.1 §6.1), which it knows through
 * a {@link BrokerConnection} only.
 * It is created stopped: its consumers receive nothing until {@link #start()}.
 */
public final class TassisConnection implements QueueConnection, TopicConnection {

    private final BrokerConnection link;
    private final Domain domain;
    private final int maxListenerDeliveries;
    private final List<TassisSession> sessions = new CopyOnWriteArrayList<>();
    private volatile boolean closed;
    private volatile String clientId;
    // Whether anything was done with the connection: after that, setClientID is too late (§6.1.2).
    private volatile boolean used;
    private volatile ExceptionListener exceptionListener;
    // Serialises the calls to setClientID: the identifier is set once. The broker gives it back
    // when the connection closes, and refuses to give one to a closed connection.
    private final ReentrantLock identity = new ReentrantLock();

    /**
     * @param configuredClientId the client identifier assigned by the connection factory, or
     *        {@code null} to leave it to {@link #setClientID}
     */
    public TassisConnection(BrokerConnection link, Domain domain, String configuredClientId,
            int maxListenerDeliveries) throws JMSException {
        this.link = link;
        this.domain = domain;
        this.maxListenerDeliveries = maxListenerDeliveries;
        link.onLost(this::lost);
        if (configuredClientId != null) {
            link.claimClientId(configuredClientId);
            clientId = configuredClientId;
            used = true;
        }
    }

    /**
     * The connection to the broker is lost: the application is told, if it asked to be (§6.1.7).
     * Called by the transport, on a thread of its own.
     */
    private void lost(JMSException problem) {
        ExceptionListener listener = exceptionListener;
        if (listener != null && !closed) {
            listener.onException(problem);
        }
    }

    /** The broker, as this connection knows it. */
    BrokerConnection link() {
        return link;
    }

    boolean isStarted() {
        return link.isStarted();
    }

    /** How many times a message is delivered to a listener that keeps failing before it is dropped. */
    int maxListenerDeliveries() {
        return maxListenerDeliveries;
    }

    /** The client identifier, or {@code null} if none is set. */
    String clientId() {
        return clientId;
    }

    void forget(TassisSession session) {
        sessions.remove(session);
    }

    /** A temporary destination lasts as long as its connection: the broker deletes it with it (§6.2.2). */
    TassisTemporaryQueue createTemporaryQueue() throws JMSException {
        return new TassisTemporaryQueue(this, link.createTemporaryQueue());
    }

    TassisTemporaryTopic createTemporaryTopic() throws JMSException {
        return new TassisTemporaryTopic(this, link.createTemporaryTopic());
    }

    private void checkOpen() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("the connection is closed");
        }
    }

    /** For every method that counts as "an action on the connection" in the sense of §6.1.2. */
    private void use() throws IllegalStateException {
        checkOpen();
        used = true;
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return openSession(sessionMode(transacted, acknowledgeMode), domain);
    }

    @Override
    public Session createSession(int sessionMode) throws JMSException {
        return openSession(sessionMode, domain);
    }

    @Override
    public Session createSession() throws JMSException {
        return openSession(Session.AUTO_ACKNOWLEDGE, domain);
    }

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return openSession(sessionMode(transacted, acknowledgeMode), Domain.QUEUE);
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return openSession(sessionMode(transacted, acknowledgeMode), Domain.TOPIC);
    }

    /** With {@code transacted} set, the acknowledgement mode is ignored (Java SE rule of createSession). */
    private static int sessionMode(boolean transacted, int acknowledgeMode) throws JMSException {
        if (transacted) {
            return Session.SESSION_TRANSACTED;
        }
        if (acknowledgeMode == Session.SESSION_TRANSACTED) {
            throw new JMSException("SESSION_TRANSACTED is not an acknowledgement mode of a non-transacted session");
        }
        return acknowledgeMode;
    }

    private TassisSession openSession(int sessionMode, Domain sessionDomain) throws JMSException {
        use();
        switch (sessionMode) {
            case Session.AUTO_ACKNOWLEDGE, Session.DUPS_OK_ACKNOWLEDGE, Session.CLIENT_ACKNOWLEDGE,
                    Session.SESSION_TRANSACTED -> {
            }
            default -> throw new JMSException("unknown session mode " + sessionMode);
        }
        TassisSession session = new TassisSession(this, sessionMode, sessionDomain);
        sessions.add(session);
        // A concurrent close either sees the session in the list, or has raised the flag checked here.
        if (closed) {
            session.shut();
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
        identity.lock();
        try {
            checkOpen();
            if (clientId != null) {
                throw new IllegalStateException("the client identifier is already set to " + clientId);
            }
            if (used) {
                throw new IllegalStateException("the client identifier must be set before the connection is used");
            }
            if (clientID == null || clientID.isEmpty()) {
                throw new InvalidClientIDException("a client identifier must not be null or empty");
            }
            link.claimClientId(clientID);
            clientId = clientID;
            used = true;
        } finally {
            identity.unlock();
        }
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        use();
        return TassisConnectionMetaData.INSTANCE;
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        use();
        return exceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        use();
        exceptionListener = listener;
    }

    @Override
    public void start() throws JMSException {
        use();
        // Only when delivery really resumes: start on a started connection must do nothing, or
        // listeners of two sessions calling it would wait for each other's turn.
        if (link.start()) {
            for (TassisSession session : sessions) {
                session.resumeListeners();
            }
        }
    }

    /**
     * Returns once delivery is paused (§6.1.5): no receive will return a message, which is what
     * the broker guarantees of a stopped connection, and no listener is running or will be
     * called. A listener cannot stop the connection it is called by.
     */
    @Override
    public void stop() throws JMSException {
        use();
        notFromACallback("stop", false);
        link.stop();
        for (TassisSession session : sessions) {
            session.awaitListenerInProgress();
        }
    }

    /** Of the two behaviours §6.1 permits for stop and close in a callback, Tassis fails. */
    private void notFromACallback(String method, boolean completionListenersToo) throws IllegalStateException {
        for (TassisSession session : sessions) {
            if (session.inOwnListener()) {
                throw new IllegalStateException(method + " called by a message listener on its own connection");
            }
            if (completionListenersToo && session.inOwnCompletion()) {
                throw new IllegalStateException(method + " called by a completion listener on its own connection");
            }
        }
    }

    /**
     * Closing a closed connection does nothing (§6.1.8). Concurrent callers all go through the
     * sessions, so each returns only once the pending receives have completed.
     */
    @Override
    public void close() throws JMSException {
        notFromACallback("close", true);
        closed = true;
        link.stop();
        for (TassisSession session : sessions) {
            session.shut();
        }
        // The sessions have given back what they held; the broker takes back the rest: the
        // temporary destinations and the client identifier.
        link.close();
    }

    // ---- connection consumers: optional (§11), and not for a QueueConnection (§9.2) ----

    private UnsupportedOperationException optional(String method) throws IllegalStateException {
        if (domain == Domain.QUEUE && !method.equals("createConnectionConsumer")) {
            throw new IllegalStateException(method + " is not available on a QueueConnection");
        }
        return new UnsupportedOperationException(method + ": optional (§11), not on the ROADMAP");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw optional("createConnectionConsumer");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw optional("createConnectionConsumer");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw optional("createConnectionConsumer");
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw optional("createSharedConnectionConsumer");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw optional("createDurableConnectionConsumer");
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw optional("createSharedDurableConnectionConsumer");
    }
}
