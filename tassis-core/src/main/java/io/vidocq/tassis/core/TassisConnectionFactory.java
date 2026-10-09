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
package io.vidocq.tassis.core;

import io.vidocq.tassis.core.internal.broker.Broker;
import io.vidocq.tassis.core.internal.client.Domain;
import io.vidocq.tassis.core.internal.client.TassisConnection;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;
import java.util.Objects;

/**
 * The Tassis {@link ConnectionFactory}: the administered object a client uses to
 * reach an in-VM broker (Jakarta Messaging 3.1 §5.3). A Java SE client
 * instantiates it directly; no JNDI lookup is involved.
 *
 * <p>A broker is identified by its name within the JVM: factories created with
 * the same name connect to the same broker, which is created on first use.
 *
 * <p>The in-VM broker has no user registry: the methods taking a user name and a
 * password accept any (§6.1.1 leaves authentication to the provider).
 */
public final class TassisConnectionFactory
        implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {

    /** Name of the broker the no-argument constructor connects to. */
    public static final String DEFAULT_BROKER = "default";

    private final String brokerName;
    private final String clientId;

    public TassisConnectionFactory() {
        this(DEFAULT_BROKER);
    }

    public TassisConnectionFactory(String brokerName) {
        this(brokerName, null);
    }

    private TassisConnectionFactory(String brokerName, String clientId) {
        this.brokerName = Objects.requireNonNull(brokerName, "brokerName");
        this.clientId = clientId;
    }

    /**
     * Returns a factory for the same broker whose connections are created with this client
     * identifier, the preferred way to assign one (§6.1.2). Such a connection rejects
     * {@code setClientID}, and only one of them can be open at a time.
     */
    public TassisConnectionFactory withClientID(String clientId) {
        return new TassisConnectionFactory(brokerName, Objects.requireNonNull(clientId, "clientId"));
    }

    private TassisConnection connect(Domain domain) throws JMSException {
        return new TassisConnection(Broker.named(brokerName), domain, clientId);
    }

    @Override
    public Connection createConnection() throws JMSException {
        return connect(Domain.UNIFIED);
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return connect(Domain.UNIFIED);
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return connect(Domain.QUEUE);
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return connect(Domain.QUEUE);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return connect(Domain.TOPIC);
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return connect(Domain.TOPIC);
    }

    @Override
    public JMSContext createContext() {
        throw new UnsupportedOperationException("createContext(): see ROADMAP M7");
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        throw new UnsupportedOperationException("createContext(userName, password): see ROADMAP M7");
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        throw new UnsupportedOperationException("createContext(userName, password, sessionMode): see ROADMAP M7");
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        throw new UnsupportedOperationException("createContext(sessionMode): see ROADMAP M7");
    }
}
