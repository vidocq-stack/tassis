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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Configuring a connection factory from settings rather than in code: the
 * administered object of Jakarta Messaging 3.1 §5.3 is "created by an
 * administrator", which here means from keys an application does not hard-code.
 */
class ConnectionFactorySettingsTest {

    private static final long SOON = 5_000;

    private final String broker = "test-" + UUID.randomUUID();

    private static String roundTrip(TassisConnectionFactory sender, TassisConnectionFactory receiver) {
        try (JMSContext sending = sender.createContext(); JMSContext receiving = receiver.createContext()) {
            Queue queue = sending.createQueue("orders");
            sending.createProducer().send(queue, "hello");
            return receiving.createConsumer(queue).receiveBody(String.class, SOON);
        }
    }

    // ---- settings ----------------------------------------------------------

    @Test
    void settingsNameTheBrokerAndTheClientIdentifier() throws JMSException {
        TassisConnectionFactory configured = TassisConnectionFactory.from(Map.of(
                "tassis.broker", broker,
                "tassis.clientId", "reader"));

        assertEquals("hello", roundTrip(new TassisConnectionFactory(broker), configured),
                "both factories reach the broker of that name");
        try (Connection connection = configured.createConnection()) {
            assertEquals("reader", connection.getClientID());
        }
    }

    @Test
    void noSettingMeansTheDefaults() throws JMSException {
        TassisConnectionFactory defaults = TassisConnectionFactory.from(Map.of());

        assertEquals("tassis:vm:" + TassisConnectionFactory.DEFAULT_BROKER, defaults.toUrl());
        try (Connection connection = defaults.createConnection()) {
            assertNull(connection.getClientID());
        }
    }

    @Test
    void propertiesMayHoldOtherPeoplesKeys() {
        Properties properties = new Properties();
        properties.setProperty("user.name", "someone");
        properties.setProperty("tassis.broker", broker);

        assertEquals("tassis:vm:" + broker, TassisConnectionFactory.from(properties).toUrl(),
                "only the keys under tassis. are read, so System.getProperties() can be passed as it is");
    }

    @Test
    void unknownSettingIsRejectedByName() {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> TassisConnectionFactory.from(Map.of("tassis.brokr", "typo")));

        assertTrue(rejected.getMessage().contains("tassis.brokr"), rejected.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "0", "-1", ""})
    void invalidValueIsRejectedByName(String value) {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> TassisConnectionFactory.from(Map.of("tassis.listener.maxDeliveries", value)));

        assertTrue(rejected.getMessage().contains("tassis.listener.maxDeliveries"), rejected.getMessage());
    }

    @Test
    void emptyBrokerOrClientIdentifierIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> TassisConnectionFactory.from(Map.of("tassis.broker", "")));
        assertThrows(IllegalArgumentException.class,
                () -> TassisConnectionFactory.from(Map.of("tassis.clientId", "")));
    }

    @Test
    void maximumDeliveriesToAFailingListenerIsASetting() throws Exception {
        TassisConnectionFactory configured = TassisConnectionFactory.from(Map.of(
                "tassis.broker", broker,
                "tassis.listener.maxDeliveries", "3"));
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch healthy = new CountDownLatch(1);

        try (JMSContext context = configured.createContext()) {
            Queue queue = context.createQueue("orders");
            context.createConsumer(queue).setMessageListener(message -> {
                try {
                    if (message.getBody(String.class).equals("poison")) {
                        attempts.incrementAndGet();
                        throw new IllegalStateException("always fails");
                    }
                    healthy.countDown();
                } catch (JMSException e) {
                    throw new AssertionError(e);
                }
            });
            try (JMSContext sender = configured.createContext()) {
                sender.createProducer().send(queue, "poison").send(queue, "healthy");
            }

            assertTrue(healthy.await(SOON, TimeUnit.MILLISECONDS));
            assertEquals(3, attempts.get());
        }
    }

    @Test
    void factoryIsConfiguredInCodeWithTheSameSettings() {
        TassisConnectionFactory factory =
                new TassisConnectionFactory(broker).withClientID("reader").withMaxListenerDeliveries(5);

        assertEquals("tassis:vm:" + broker + "?clientId=reader&listener.maxDeliveries=5", factory.toUrl());
        assertEquals(factory.toUrl(), factory.toString());
        assertThrows(IllegalArgumentException.class, () -> factory.withMaxListenerDeliveries(0));
    }

    // ---- the settings of a pool ------------------------------------------------

    @Test
    void theSettingsOfAPoolAreKeysAndUrlParameters() {
        TassisConnectionFactory byKeys = TassisConnectionFactory.from(Map.of("tassis.broker", broker,
                "tassis.pool.enabled", "false", "tassis.pool.maxSize", "4", "tassis.pool.acquireTimeout", "PT2S",
                "tassis.pool.idleTimeout", "PT1M", "tassis.pool.maxLifetime", "PT0S",
                "tassis.pool.leakThreshold", "PT30S"));

        String url = "tassis:vm:" + broker + "?pool.enabled=false&pool.maxSize=4&pool.acquireTimeout=PT2S"
                + "&pool.idleTimeout=PT1M&pool.maxLifetime=PT0S&pool.leakThreshold=PT30S";
        assertEquals(url, byKeys.toUrl());
        assertEquals(url, TassisConnectionFactory.fromUrl(url).toUrl(), "toUrl gives a URL that fromUrl reads back");
    }

    @Test
    void theDefaultsOfAPoolAreLeftOutOfTheUrl() {
        TassisConnectionFactory defaults = TassisConnectionFactory.from(Map.of("tassis.broker", broker,
                "tassis.pool.enabled", "true", "tassis.pool.maxSize", "10", "tassis.pool.acquireTimeout", "PT5S",
                "tassis.pool.idleTimeout", "PT10M", "tassis.pool.maxLifetime", "PT30M",
                "tassis.pool.leakThreshold", "PT0S"));

        assertEquals("tassis:vm:" + broker, defaults.toUrl());
    }

    @ParameterizedTest
    @ValueSource(strings = {"tassis.pool.maxSize=0", "tassis.pool.maxSize=many", "tassis.pool.enabled=yes",
            "tassis.pool.acquireTimeout=PT0S", "tassis.pool.acquireTimeout=5", "tassis.pool.idleTimeout=-PT1S",
            "tassis.pool.maxLifetime=soon", "tassis.pool.leakThreshold="})
    void invalidSettingOfAPoolIsRejectedByName(String setting) {
        String key = setting.substring(0, setting.indexOf('='));
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> TassisConnectionFactory.from(Map.of(key, setting.substring(key.length() + 1))));

        assertTrue(rejected.getMessage().contains(key), rejected.getMessage());
    }

    // ---- connection URL ----------------------------------------------------

    @Test
    void urlIsTheOneStringFormOfTheSettings() throws JMSException {
        TassisConnectionFactory fromUrl =
                TassisConnectionFactory.fromUrl("tassis:vm:" + broker + "?clientId=reader%20one&listener.maxDeliveries=4");

        assertEquals("hello", roundTrip(new TassisConnectionFactory(broker), fromUrl));
        try (Connection connection = fromUrl.createConnection()) {
            assertEquals("reader one", connection.getClientID(), "values are percent-decoded");
        }
        assertEquals(fromUrl.toUrl(), TassisConnectionFactory.fromUrl(fromUrl.toUrl()).toUrl(),
                "toUrl gives a URL that fromUrl reads back");
    }

    @Test
    void urlMayBeGivenAsASetting() {
        TassisConnectionFactory factory = TassisConnectionFactory.from(Map.of(
                "tassis.url", "tassis:vm:" + broker + "?clientId=from-the-url",
                "tassis.clientId", "from-its-own-key"));

        assertEquals("tassis:vm:" + broker + "?clientId=from-its-own-key", factory.toUrl(),
                "a key of its own overrides what the URL says");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "orders",
        "vm:orders",
        "tassis:orders",
        "tassis:vm:",
        "tassis:tcp://localhost:5672",
        "tassis:vm:orders?",
        "tassis:vm:orders?clientId",
        "tassis:vm:orders?unknown=1",
        "tassis:vm:orders?listener.maxDeliveries=zero",
        "tassis:vm:orders?clientId=a&clientId=b",
        "tassis:vm:orders?clientId=%zz",
    })
    void invalidUrlIsRejected(String url) {
        assertThrows(IllegalArgumentException.class, () -> TassisConnectionFactory.fromUrl(url), url);
    }

    // ---- named factories ---------------------------------------------------

    @Test
    void severalFactoriesAreDefinedByName() {
        Map<String, String> settings = Map.of(
                "tassis.broker", broker,
                "tassis.factory.orders.clientId", "orders-reader",
                "tassis.factory.audit.broker", broker + "-audit",
                "tassis.factory.audit.listener.maxDeliveries", "2");

        assertEquals("tassis:vm:" + broker + "?clientId=orders-reader",
                TassisConnectionFactory.named("orders", settings).toUrl(),
                "a named factory inherits the settings that carry no name");
        assertEquals("tassis:vm:" + broker + "-audit?listener.maxDeliveries=2",
                TassisConnectionFactory.named("audit", settings).toUrl());
        assertEquals("tassis:vm:" + broker, TassisConnectionFactory.from(settings).toUrl(),
                "the settings of named factories are not those of the unnamed one");
    }

    @Test
    void unknownSettingOfANamedFactoryIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> TassisConnectionFactory.named("orders", Map.of("tassis.factory.orders.brokr", "typo")));
        assertThrows(IllegalArgumentException.class, () -> TassisConnectionFactory.named("", Map.of()));
    }

    // ---- a broker in another process (ROADMAP M11.3d) ------------------------

    @Test
    void urlMayNameABrokerOfTheNetwork() {
        assertEquals("tassis:amqp://broker.example:5673?clientId=orders-reader",
                TassisConnectionFactory.fromUrl("tassis:amqp://broker.example:5673?clientId=orders-reader").toUrl());
        assertEquals("tassis:amqp://broker.example:5672",
                TassisConnectionFactory.fromUrl("tassis:amqp://broker.example").toUrl(),
                "5672 is the port of AMQP");
        assertEquals("tassis:amqp://[::1]:5672", TassisConnectionFactory.fromUrl("tassis:amqp://[::1]").toUrl());
    }

    @Test
    void urlMayAskForAnEncryptedConnection() throws Exception {
        assertEquals("tassis:amqps://broker.example:5671",
                TassisConnectionFactory.fromUrl("tassis:amqps://broker.example").toUrl(),
                "5671 is the port of AMQP over TLS");
        assertEquals("tassis:amqps://broker.example:5999?clientId=a",
                TassisConnectionFactory.from(Map.of("tassis.server", "amqps://broker.example:5999",
                        "tassis.clientId", "a")).toUrl());
        // whom to trust is given in code, and is no part of the URL
        TassisConnectionFactory trusting = TassisConnectionFactory.fromUrl("tassis:amqps://broker.example")
                .withSslContext(javax.net.ssl.SSLContext.getDefault());
        assertEquals("tassis:amqps://broker.example:5671?clientId=b", trusting.withClientID("b").toUrl());
    }

    @Test
    void theServerIsASettingOfItsOwn() {
        assertEquals("tassis:amqp://broker.example:5672?clientId=a",
                TassisConnectionFactory.from(Map.of("tassis.server", "amqp://broker.example", "tassis.clientId", "a"))
                        .toUrl());
        assertEquals("tassis:amqp://other.example:5672",
                TassisConnectionFactory.from(Map.of("tassis.url", "tassis:amqp://broker.example",
                        "tassis.server", "amqp://other.example")).toUrl(),
                "a setting given by its own key overrides the URL");
    }

    @ParameterizedTest
    @ValueSource(strings = {"tassis:amqp://", "tassis:amqp://:5672", "tassis:amqp://host:0", "tassis:amqp://host:70000",
            "tassis:amqp://host:port", "tassis:amqp://host/path", "tassis:amqp://user@host", "tassis:http://host"})
    void invalidServerIsRejected(String url) {
        assertThrows(IllegalArgumentException.class, () -> TassisConnectionFactory.fromUrl(url), url);
    }

    @Test
    void aBrokerOfTheNetworkNeedsTheTransportThatReachesIt() {
        // tassis-core alone: the AMQP transport is the module io.vidocq.tassis.amqp
        JMSException missing = assertThrows(JMSException.class,
                () -> TassisConnectionFactory.fromUrl("tassis:amqp://localhost:1").createConnection());
        assertTrue(missing.getMessage().contains("tassis-amqp"), missing.getMessage());
    }

    // ---- fetching ahead (ROADMAP M12-3) ----------------------------------------------

    @Test
    void theNumberOfMessagesFetchedAheadIsASettingLikeTheOthers() {
        assertEquals("tassis:amqp://broker.example:5672", TassisConnectionFactory.fromUrl("tassis:amqp://broker.example").toUrl());
        assertEquals("tassis:amqp://broker.example:5672?prefetch=100",
                TassisConnectionFactory.fromUrl("tassis:amqp://broker.example?prefetch=100").toUrl());
        assertEquals("tassis:amqp://broker.example:5672?prefetch=20",
                TassisConnectionFactory.from(Map.of("tassis.server", "amqp://broker.example", "tassis.prefetch", "20")).toUrl());
        assertEquals("tassis:amqp://broker.example:5672?prefetch=5",
                TassisConnectionFactory.fromUrl("tassis:amqp://broker.example").withPrefetch(5).toUrl());
        // one at a time is the default, and is not written
        assertEquals("tassis:amqp://broker.example:5672",
                TassisConnectionFactory.fromUrl("tassis:amqp://broker.example?prefetch=7").withPrefetch(1).toUrl());
    }

    @Test
    void aNumberOfMessagesFetchedAheadBelowOneIsRefused() {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> TassisConnectionFactory.from(Map.of("tassis.prefetch", "0")));
        assertTrue(rejected.getMessage().contains("tassis.prefetch"), rejected.getMessage());
        assertThrows(IllegalArgumentException.class, () -> new TassisConnectionFactory().withPrefetch(0));
        assertThrows(IllegalArgumentException.class, () -> TassisConnectionFactory.fromUrl("tassis:vm:orders?prefetch=many"));
    }
}
