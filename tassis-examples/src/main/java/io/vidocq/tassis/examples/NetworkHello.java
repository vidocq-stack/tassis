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
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.ConnectionFactory;

/**
 * Opens a broker to the network and reaches it through the network: what two processes do,
 * here in one. The process that hosts the broker starts a {@code TassisServer}; the other one
 * only changes the URL of its connection factory.
 */
public final class NetworkHello {

    private NetworkHello() {
    }

    public static void main(String[] args) {
        // Port 0: any free port. A real server listens on a known one, 5672 by default.
        try (TassisServer server = TassisServer.forBroker("orders").port(0).start()) {
            System.out.println(sendAndReceive(server.port()));
        }
    }

    static String sendAndReceive(int port) {
        ConnectionFactory factory = TassisConnectionFactory.fromUrl("tassis:amqp://localhost:" + port);

        return SimplifiedHello.sendAndReceive(factory);
    }
}
