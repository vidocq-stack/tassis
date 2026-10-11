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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.vidocq.tassis.core.TassisQueue;
import io.vidocq.tassis.core.internal.broker.Broker;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * A session may be closed from another thread than the one using it (Jakarta
 * Messaging 3.1 §2.14): whatever the interleaving, a closed connection must
 * leave nothing attached to the broker.
 */
class CloseRaceTest {

    @Test
    void closingAConnectionWhileItCreatesConsumersLeavesNoneAttached() throws Exception {
        Broker broker = Broker.named("test-" + UUID.randomUUID());
        TassisQueue queue = new TassisQueue("orders");

        for (int round = 0; round < 2_000; round++) {
            TassisConnection connection = new TassisConnection(broker.connect(null, null), Domain.UNIFIED, null, 10);
            Session session = connection.createSession();
            CountDownLatch go = new CountDownLatch(1);
            Thread creator = Thread.startVirtualThread(() -> {
                try {
                    go.await();
                    while (true) {
                        session.createConsumer(queue);
                    }
                } catch (JMSException | InterruptedException closed) {
                    // The session was closed under us: the expected way out.
                }
            });
            go.countDown();
            connection.close();
            creator.join();

            assertEquals(0, broker.consumerCount("orders"), "round " + round);
        }
    }
}
