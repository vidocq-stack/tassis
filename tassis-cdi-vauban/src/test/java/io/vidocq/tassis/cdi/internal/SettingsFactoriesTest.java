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
package io.vidocq.tassis.cdi.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.pool.TassisConnectionPool;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateException;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Which factory the settings give: a pool for a broker of the network, where a connection is a
 * socket, and the plain factory otherwise. No connection is made here: a pool opens none until
 * one is asked for.
 */
class SettingsFactoriesTest {

    private static final List<String> KEYS = List.of("tassis.broker", "tassis.server", "tassis.clientId",
            "tassis.pool.enabled", "tassis.pool.maxSize", "tassis.factory.audit.server",
            "tassis.factory.audit.pool.enabled");

    private final SettingsFactories factories = new SettingsFactories();

    @AfterEach
    void unconfigure() {
        factories.close();
        KEYS.forEach(System::clearProperty);
    }

    @Test
    void aBrokerOfTheJvmIsReachedWithoutAPool() {
        System.setProperty("tassis.broker", "orders");

        ConnectionFactory factory = factories.named(null);

        assertEquals("tassis:vm:orders", assertInstanceOf(TassisConnectionFactory.class, factory).toUrl());
    }

    @Test
    void aBrokerOfTheNetworkIsReachedThroughAPool() {
        System.setProperty("tassis.server", "amqp://broker.example");
        System.setProperty("tassis.pool.maxSize", "3");

        ConnectionFactory factory = factories.named(null);

        assertEquals("pool of tassis:amqp://broker.example:5672?pool.maxSize=3",
                assertInstanceOf(TassisConnectionPool.class, factory).toString());
    }

    @Test
    void theSettingsMayRefuseThePool() {
        System.setProperty("tassis.server", "amqp://broker.example");
        System.setProperty("tassis.pool.enabled", "false");

        assertInstanceOf(TassisConnectionFactory.class, factories.named(null));
    }

    @Test
    void aFactoryThatAssignsAClientIdentifierCannotBePooled() {
        System.setProperty("tassis.server", "amqp://broker.example");
        System.setProperty("tassis.clientId", "billing");

        // Jakarta Messaging 3.1 §6.1.2: one connection at a time for a client identifier
        assertInstanceOf(TassisConnectionFactory.class, factories.named(null));
    }

    @Test
    void aFactoryIsBuiltOnceAndEachNameHasItsOwn() {
        System.setProperty("tassis.server", "amqp://broker.example");
        System.setProperty("tassis.factory.audit.server", "amqp://audit.example");
        System.setProperty("tassis.factory.audit.pool.enabled", "false");

        ConnectionFactory orders = factories.named(null);
        ConnectionFactory audit = factories.named("audit");

        assertSame(orders, factories.named(null), "the pool of the application, not one per request");
        assertSame(audit, factories.named("audit"));
        assertNotSame(orders, audit);
        assertInstanceOf(TassisConnectionPool.class, orders);
        assertEquals("tassis:amqp://audit.example:5672?pool.enabled=false",
                assertInstanceOf(TassisConnectionFactory.class, audit).toUrl());
    }

    @Test
    void thePoolsAreClosedWithTheApplication() {
        System.setProperty("tassis.server", "amqp://broker.example");
        ConnectionFactory pool = factories.named(null);

        factories.close();

        assertThrows(IllegalStateException.class, pool::createConnection);
        assertThrows(java.lang.IllegalStateException.class, () -> factories.named(null),
                "and no other is built, which nobody would close");
    }
}
