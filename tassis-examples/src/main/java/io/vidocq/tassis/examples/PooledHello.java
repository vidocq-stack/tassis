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
package io.vidocq.tassis.examples;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.pool.TassisConnectionPool;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.JMSContext;
import jakarta.jms.Queue;

/**
 * Reaches a broker of the network through a pool: the contexts come and go, the connection to
 * the broker stays. A pool is a connection factory, so nothing else changes in the code that
 * sends and receives.
 */
public final class PooledHello {

    private PooledHello() {
    }

    public static void main(String[] args) {
        try (TassisServer server = TassisServer.forBroker("orders").port(0).start()) {
            System.out.println(sendAndReceive(server.port()));
        }
    }

    static String sendAndReceive(int port) {
        TassisConnectionFactory factory = TassisConnectionFactory.fromUrl("tassis:amqp://localhost:" + port);

        // Closing the pool closes the connections it keeps.
        try (TassisConnectionPool pool = TassisConnectionPool.over(factory).maxSize(4).build()) {
            for (int i = 1; i <= 3; i++) {
                // Closing a context gives its connection back to the pool: one socket for the three.
                try (JMSContext context = pool.createContext()) {
                    Queue queue = context.createQueue("greetings");
                    context.createProducer().send(queue, "hello " + i);
                }
            }
            try (JMSContext context = pool.createContext()) {
                Queue queue = context.createQueue("greetings");
                String first = context.createConsumer(queue).receiveBody(String.class, 5_000);
                return first + ", over " + pool.size() + " connection";
            }
        }
    }
}
