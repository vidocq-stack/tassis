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
package io.vidocq.tassis.tck;

import com.sun.ts.lib.porting.TSJMSObjectsInterface;
import io.vidocq.tassis.core.TassisBroker;
import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.core.TassisQueue;
import io.vidocq.tassis.core.TassisTopic;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnectionFactory;

/**
 * The TCK's porting class: hands the test suite the administered objects it asks
 * for by name. Selected by the property {@code porting.ts.jmsObjects.class.1} of the
 * harness. Every factory connects to the default in-VM broker, directly or, with the system
 * property {@code tassis.tck.transport=amqp}, through a server and a socket. It is secured with the user of the
 * TCK's configuration, and a destination is whatever name the TCK asks for, since Tassis
 * creates destinations on first use.
 */
public final class TassisJmsObjects implements TSJMSObjectsInterface {

    /** The factory the TCK expects to come with a client identifier, for durable subscriptions. */
    private static final String DURABLE_FACTORY = "DURABLE_SUB_CONNECTION_FACTORY";

    static {
        // The TCK creates most of its connections without credentials, some with the user of its
        // configuration, and expects ("invalid", "invalid") to be refused (§6.1.1).
        TassisBroker.named(TassisConnectionFactory.DEFAULT_BROKER)
                .addUser(System.getProperty("user", "j2ee"), System.getProperty("password", "j2ee"))
                .allowAnonymous(true);
    }

    /**
     * The URL of the broker when the TCK is run through the network, {@code null} when it uses
     * the broker of its JVM directly. The server lasts as long as the JVM of the tests.
     */
    private static final String SERVER_URL = "amqp".equals(System.getProperty("tassis.tck.transport"))
            ? "tassis:amqp://localhost:"
                    + TassisServer.forBroker(TassisConnectionFactory.DEFAULT_BROKER).port(0).start().port()
            : null;

    private static TassisConnectionFactory factory(String name) {
        TassisConnectionFactory factory =
                SERVER_URL == null ? new TassisConnectionFactory() : TassisConnectionFactory.fromUrl(SERVER_URL);
        return DURABLE_FACTORY.equals(name) ? factory.withClientID("cts") : factory;
    }

    @Override
    public Queue getQueue(String name) {
        return new TassisQueue(name);
    }

    @Override
    public Topic getTopic(String name) {
        return new TassisTopic(name);
    }

    @Override
    public TopicConnectionFactory getTopicConnectionFactory(String name) {
        return factory(name);
    }

    @Override
    public QueueConnectionFactory getQueueConnectionFactory(String name) {
        return factory(name);
    }

    @Override
    public ConnectionFactory getConnectionFactory(String name) {
        return factory(name);
    }
}
