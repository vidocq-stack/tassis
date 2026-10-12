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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.JMSException;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The examples quoted by the documentation do what the documentation says. */
class ExamplesTest {

    private final String broker = "examples-" + UUID.randomUUID();

    @Test
    void simplifiedExampleReceivesWhatItSent() {
        assertEquals("hello", SimplifiedHello.sendAndReceive(new TassisConnectionFactory(broker)));
    }

    @Test
    void classicExampleReceivesWhatItSent() throws JMSException {
        assertEquals("hello", ClassicHello.sendAndReceive(new TassisConnectionFactory(broker)));
    }

    @Test
    void networkExampleReceivesWhatItSentThroughAServer() {
        try (TassisServer server = TassisServer.forBroker(broker).port(0).start()) {
            assertEquals("hello", NetworkHello.sendAndReceive(server.port()));
        }
    }

    @Test
    void pooledExampleReceivesWhatItSentOverOneConnection() {
        try (TassisServer server = TassisServer.forBroker(broker).port(0).start()) {
            assertEquals("hello 1, over 1 connection", PooledHello.sendAndReceive(server.port()));
        }
    }

    @Test
    void exampleRunsOnAFactoryConfiguredBySettings() {
        TassisConnectionFactory configured =
                TassisConnectionFactory.from(Map.of("tassis.broker", broker, "tassis.clientId", "reader"));

        assertEquals("tassis:vm:" + broker + "?clientId=reader", configured.toUrl());
        assertEquals("hello", SimplifiedHello.sendAndReceive(configured));
    }
}
