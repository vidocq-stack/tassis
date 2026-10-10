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
package io.vidocq.tassis.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vidocq.tassis.cdi.app.Audit;
import io.vidocq.tassis.cdi.app.Billing;
import io.vidocq.tassis.cdi.app.Factories;
import io.vidocq.tassis.cdi.app.Orders;
import io.vidocq.tassis.cdi.app.TwoDefaultFactories;
import io.vidocq.tassis.core.TassisBroker;
import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.vauban.core.container.VaubanContainer;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.Queue;
import jakarta.jms.TemporaryQueue;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Injection of {@code JMSContext} (Jakarta Messaging 3.1 §12.4): the annotations of the
 * injection point (§12.4.3), the request scope of the injected context (§12.4.4) and what an
 * injected context refuses to do (§12.4.5).
 *
 * <p>The application beans are in a package of their own, {@code app}: it is the only one opened
 * to the container, which reaches those test beans reflectively. The beans of the integration
 * itself are in a package that is not opened, so these tests also show that the generated code
 * is enough for them.
 */
class JmsContextInjectionTest {

    private static final long SOON = 5_000;

    // ---- fixture -----------------------------------------------------------

    private final String broker = "cdi-" + UUID.randomUUID();
    private final String auditBroker = broker + "-audit";

    @BeforeEach
    void configure() {
        System.setProperty("tassis.broker", broker);
        System.setProperty("tassis.factory.audit.broker", auditBroker);
    }

    @AfterEach
    void unconfigure() {
        System.clearProperty("tassis.broker");
        System.clearProperty("tassis.factory.audit.broker");
    }

    private static VaubanContainer container(Class<?>... applicationBeans) {
        var builder = VaubanContainer.builder();
        for (Class<?> bean : TassisCdi.BEANS) {
            builder.addBeanClass(bean);
        }
        for (Class<?> bean : applicationBeans) {
            builder.addBeanClass(bean);
        }
        return builder.build();
    }

    /** What a broker holds on a queue, read from outside CDI. */
    private static String received(String brokerName, String queueName) {
        try (JMSContext outside = new TassisConnectionFactory(brokerName).createContext()) {
            return outside.createConsumer(outside.createQueue(queueName)).receiveBody(String.class, 200);
        }
    }

    // ---- §12.4.3 and §12.4.4 -----------------------------------------------

    @Test
    void injectedContextSendsAndReceivesWithinARequest() {
        try (VaubanContainer container = container(Orders.class)) {
            Orders orders = container.select(Orders.class);
            AtomicReference<String> body = new AtomicReference<>();

            container.requestContext().runInScope(() -> {
                Queue queue = orders.context.createQueue("orders");
                orders.context.createProducer().send(queue, "hello");
                body.set(orders.context.createConsumer(queue).receiveBody(String.class, SOON));
            });

            assertEquals("hello", body.get());
        }
    }

    @Test
    void aSecuredBrokerChecksTheCredentialsOfTheInjectionPoint() {
        // §12.4.3: @JMSPasswordCredential gives the credentials; §6.1.1: the broker checks them.
        TassisBroker.named(broker).addUser("user", "secret");
        try (VaubanContainer container = container(Orders.class)) {
            Orders orders = container.select(Orders.class);

            container.requestContext().runInScope(() -> {
                assertNotNull(orders.withCredentials.createQueue("orders"));
                orders.withCredentials.createProducer().send(orders.withCredentials.createQueue("orders"), "paid");

                assertThrows(JMSSecurityRuntimeException.class, () -> orders.context.createProducer(),
                        "an injection point without credentials connects without any");
            });
        }
        TassisBroker.named(broker).allowAnonymous(true);
        assertEquals("paid", received(broker, "orders"));
    }

    @Test
    void injectionsWithIdenticalAnnotationsShareOneContextInARequest() {
        try (VaubanContainer container = container(Orders.class, Billing.class)) {
            Orders orders = container.select(Orders.class);
            Billing billing = container.select(Billing.class);
            AtomicReference<String> body = new AtomicReference<>();

            container.requestContext().runInScope(() -> {
                // A temporary queue is consumed by the connection that created it and no other.
                TemporaryQueue temporary = orders.context.createTemporaryQueue();
                orders.sameAnnotations.createProducer().send(temporary, "one connection");
                body.set(billing.context.createConsumer(temporary).receiveBody(String.class, SOON));

                assertThrows(JMSRuntimeException.class, () -> orders.clientAcknowledged.createConsumer(temporary),
                        "other annotations mean another context, hence another connection");
            });

            assertEquals("one connection", body.get());
        }
    }

    @Test
    void contextIsClosedWhenItsRequestEndsAndANewRequestGetsANewOne() {
        try (VaubanContainer container = container(Orders.class)) {
            Orders orders = container.select(Orders.class);
            AtomicReference<TemporaryQueue> ofTheFirstRequest = new AtomicReference<>();

            container.requestContext().runInScope(
                    () -> ofTheFirstRequest.set(orders.context.createTemporaryQueue()));

            container.requestContext().runInScope(() -> assertThrows(JMSRuntimeException.class,
                    () -> orders.context.createProducer().send(ofTheFirstRequest.get(), "too late"),
                    "the connection of the first request was closed with it, and its temporary queue deleted"));
        }
    }

    @Test
    void injectedContextNeedsAnActiveRequest() {
        try (VaubanContainer container = container(Orders.class)) {
            Orders orders = container.select(Orders.class);

            assertNotNull(orders.context, "injection itself needs no request");
            assertThrows(ContextNotActiveException.class, () -> orders.context.createQueue("orders"));
        }
    }

    @Test
    void sessionModeComesFromTheAnnotation() {
        try (VaubanContainer container = container(Orders.class)) {
            Orders orders = container.select(Orders.class);

            container.requestContext().runInScope(() -> {
                assertEquals(JMSContext.AUTO_ACKNOWLEDGE, orders.context.getSessionMode());
                assertEquals(JMSContext.CLIENT_ACKNOWLEDGE, orders.clientAcknowledged.getSessionMode());
                assertEquals(JMSContext.AUTO_ACKNOWLEDGE, orders.withCredentials.getSessionMode());
            });
        }
    }

    // ---- which connection factory ------------------------------------------

    @Test
    void factoriesComeFromTheSettingsWhenTheApplicationSuppliesNone() {
        try (VaubanContainer container = container(Orders.class, Audit.class)) {
            Orders orders = container.select(Orders.class);
            Audit audit = container.select(Audit.class);

            container.requestContext().runInScope(() -> {
                orders.context.createProducer().send(orders.context.createQueue("log"), "to the default broker");
                audit.context.createProducer().send(audit.context.createQueue("log"), "to the audit broker");
            });

            assertEquals("to the default broker", received(broker, "log"));
            assertEquals("to the audit broker", received(auditBroker, "log"));
        }
    }

    @Test
    void factoriesSuppliedAsBeansTakePrecedenceOverTheSettings() {
        Factories.defaultBroker = broker + "-bean";
        Factories.auditBroker = auditBroker + "-bean";
        try (VaubanContainer container = container(Orders.class, Audit.class, Factories.class)) {
            Orders orders = container.select(Orders.class);
            Audit audit = container.select(Audit.class);

            container.requestContext().runInScope(() -> {
                orders.context.createProducer().send(orders.context.createQueue("log"), "default");
                audit.context.createProducer().send(audit.context.createQueue("log"), "audit");
            });

            assertEquals("default", received(broker + "-bean", "log"));
            assertEquals("audit", received(auditBroker + "-bean", "log"));
            assertNull(received(broker, "log"), "the settings were not used");
        }
    }

    @Test
    void twoFactoriesForOneNameAreAnError() {
        try (VaubanContainer container = container(Orders.class, TwoDefaultFactories.class)) {
            Orders orders = container.select(Orders.class);
            AtomicReference<JMSRuntimeException> failure = new AtomicReference<>();

            container.requestContext().runInScope(() -> failure.set(
                    assertThrows(JMSRuntimeException.class, () -> orders.context.createQueue("any"))));

            assertEquals("several ConnectionFactory beans match the default connection factory",
                    failure.get().getMessage());
        }
    }

    // ---- §12.4.5 -----------------------------------------------------------

    @Test
    void injectedContextRefusesWhatWouldChangeItForTheOtherBeans() {
        try (VaubanContainer container = container(Orders.class)) {
            Orders orders = container.select(Orders.class);

            container.requestContext().runInScope(() -> {
                JMSContext context = orders.context;
                List<Runnable> refused = List.of(context::close, context::commit, context::rollback,
                        context::recover, context::acknowledge, context::start, context::stop,
                        () -> context.setAutoStart(false), () -> context.setClientID("id"),
                        () -> context.setExceptionListener(exception -> { }));
                for (Runnable call : refused) {
                    assertThrows(IllegalStateRuntimeException.class, call::run);
                }
                assertNotNull(context.createTextMessage("still usable"));
            });
        }
    }
}
