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
import io.vidocq.tassis.core.internal.client.TassisConnection;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import java.util.Objects;

/**
 * The Tassis {@link ConnectionFactory}: the administered object a client uses to
 * reach an in-VM broker (Jakarta Messaging 3.1 §5.3). A Java SE client
 * instantiates it directly; no JNDI lookup is involved.
 *
 * <p>A broker is identified by its name within the JVM: factories created with
 * the same name connect to the same broker, which is created on first use.
 */
public final class TassisConnectionFactory implements ConnectionFactory, QueueConnectionFactory {

    /** Name of the broker the no-argument constructor connects to. */
    public static final String DEFAULT_BROKER = "default";

    private final String brokerName;

    public TassisConnectionFactory() {
        this(DEFAULT_BROKER);
    }

    public TassisConnectionFactory(String brokerName) {
        this.brokerName = Objects.requireNonNull(brokerName, "brokerName");
    }

    @Override
    public Connection createConnection() {
        return new TassisConnection(Broker.named(brokerName));
    }

    /**
     * The in-VM broker has no user registry: any user name and password are
     * accepted (§6.1.1 leaves authentication to the provider).
     */
    @Override
    public Connection createConnection(String userName, String password) {
        return createConnection();
    }

    @Override
    public QueueConnection createQueueConnection() {
        return new TassisConnection(Broker.named(brokerName));
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) {
        return createQueueConnection();
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
