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
package io.vidocq.tassis.cdi.moduleit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.cdi.TassisCdi;
import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.vauban.core.container.VaubanContainer;
import jakarta.jms.JMSContext;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@code JMSContext} injection (Jakarta Messaging 3.1 §12.4) for an application on the module
 * path whose beans were processed at build time: the annotations of the injection points must
 * reach the producer although no package is opened to the container.
 */
class JmsContextInjectionModulePathTest {

    private final String broker = "module-it-" + UUID.randomUUID();

    @BeforeEach
    void configure() {
        System.setProperty("tassis.broker", broker);
        System.setProperty("tassis.factory.audit.broker", broker + "-audit");
    }

    @AfterEach
    void unconfigure() {
        System.clearProperty("tassis.broker");
        System.clearProperty("tassis.factory.audit.broker");
    }

    @Test
    void applicationBeanInjectsItsContextsOnTheModulePath() {
        assertTrue(OrderDesk.class.getModule().isNamed(), "the test must run on the module path");
        assertTrue(!OrderDesk.class.getModule().isOpen(OrderDesk.class.getPackageName(),
                VaubanContainer.class.getModule()), "nothing is opened to the container");

        var builder = VaubanContainer.builder();
        TassisCdi.BEANS.forEach(builder::addBeanClass);
        try (VaubanContainer container = builder.addBeanClass(OrderDesk.class).build()) {
            OrderDesk desk = container.select(OrderDesk.class);
            AtomicReference<String> order = new AtomicReference<>();
            AtomicReference<Integer> auditMode = new AtomicReference<>();

            container.requestContext().runInScope(() -> {
                desk.place("42");
                order.set(desk.nextOrder());
                auditMode.set(desk.auditSessionMode());
            });

            assertEquals("42", order.get());
            assertEquals(JMSContext.CLIENT_ACKNOWLEDGE, auditMode.get(), "@JMSSessionMode was read");
        }

        try (JMSContext outside = new TassisConnectionFactory(broker + "-audit").createContext()) {
            assertEquals("placed 42",
                    outside.createConsumer(outside.createQueue("trail")).receiveBody(String.class, 5_000),
                    "@JMSConnectionFactory(\"audit\") was read: the trail is on the audit broker");
        }
    }
}
