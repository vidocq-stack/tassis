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
package io.vidocq.tassis.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.Connection;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TextMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The pool over a socket, which is what it is for: the Tassis client, a relay that counts the
 * connections and can cut them, a {@link TassisServer}.
 */
class NetworkPoolTest {

    private static final long SOON = 5_000;

    /** Passes octets between a client and the server, counts the clients, and can cut them all. */
    private static final class Relay implements AutoCloseable {

        private final ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        private final int serverPort;
        private final AtomicInteger accepted = new AtomicInteger();
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();

        Relay(int serverPort) throws IOException {
            this.serverPort = serverPort;
            Thread.ofVirtual().start(this::accept);
        }

        int port() {
            return listener.getLocalPort();
        }

        int accepted() {
            return accepted.get();
        }

        /** How many clients are still connected. */
        int open() {
            return (int) sockets.stream().filter(socket -> !socket.isClosed()).count() / 2;
        }

        private void accept() {
            while (true) {
                try {
                    Socket client = listener.accept();
                    Socket server = new Socket(InetAddress.getLoopbackAddress(), serverPort);
                    sockets.add(client);
                    sockets.add(server);
                    accepted.incrementAndGet();
                    Thread.ofVirtual().start(() -> pass(client, server));
                    Thread.ofVirtual().start(() -> pass(server, client));
                } catch (IOException closed) {
                    return;
                }
            }
        }

        private static void pass(Socket from, Socket to) {
            try (from; to) {
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                byte[] octets = new byte[8_192];
                for (int read = in.read(octets); read >= 0; read = in.read(octets)) {
                    out.write(octets, 0, read);
                    out.flush();
                }
            } catch (IOException over) {
                // one side is gone: the try closes the other
            }
        }

        /** Cuts every connection that goes through, as a network that fails does. */
        void cut() throws IOException {
            for (Socket socket : sockets) {
                socket.close();
            }
            sockets.clear();
        }

        @Override
        public void close() throws IOException {
            listener.close();
            cut();
        }
    }

    private final String broker = "network-pool-" + UUID.randomUUID();
    private TassisServer server;
    private Relay relay;
    private TassisConnectionPool pool;

    @BeforeEach
    void start() throws IOException {
        server = TassisServer.forBroker(broker).port(0).start();
        relay = new Relay(server.port());
        pool = TassisConnectionPool.over(TassisConnectionFactory.fromUrl("tassis:amqp://localhost:" + relay.port()))
                .maxSize(2).acquireTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void stop() throws IOException {
        pool.close();
        relay.close();
        server.close();
    }

    private static void await(String what, java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, what);
            Thread.sleep(20);
        }
    }

    @Test
    void manyConnectionsOneAfterTheOtherAreOneSocket() {
        for (int i = 0; i < 20; i++) {
            try (JMSContext context = pool.createContext()) {
                context.createProducer().send(context.createQueue("orders"), "n" + i);
            }
        }

        assertEquals(1, relay.accepted(), "one handshake, one authentication");
        try (JMSContext context = pool.createContext()) {
            for (int i = 0; i < 20; i++) {
                assertEquals("n" + i,
                        context.createConsumer(context.createQueue("orders")).receiveBody(String.class, SOON));
            }
        }
        assertEquals(1, relay.accepted());
    }

    @Test
    void theNextBorrowerFindsNothingOfTheOneBefore() throws JMSException {
        TemporaryQueue left;
        try (Connection before = pool.createConnection()) {
            before.start();
            Session session = before.createSession(Session.CLIENT_ACKNOWLEDGE);
            session.createProducer(session.createQueue("orders")).send(session.createTextMessage("unacknowledged"));
            assertNotNull(session.createConsumer(session.createQueue("orders")).receive(SOON));
            Session transacted = before.createSession(Session.SESSION_TRANSACTED);
            transacted.createProducer(transacted.createQueue("invoices")).send(transacted.createTextMessage("never sent"));
            left = session.createTemporaryQueue();
            session.createConsumer(left);
        }

        try (Connection connection = pool.createConnection()) {
            Session session = connection.createSession();
            // stopped (§6.1.4), though the one before was started
            assertEquals(null, session.createConsumer(session.createQueue("orders")).receive(200));
            connection.start();
            Message again = session.createConsumer(session.createQueue("orders")).receive(SOON);
            assertEquals("unacknowledged", ((TextMessage) again).getText());
            assertTrue(again.getJMSRedelivered());
            assertEquals(null, session.createConsumer(session.createQueue("invoices")).receive(200));
            // §6.2.2: the temporary queue went with the connection the application closed
            org.junit.jupiter.api.Assertions.assertThrows(JMSException.class,
                    () -> session.createProducer(left).send(session.createTextMessage("lost")));
        }
        assertEquals(1, relay.accepted());
    }

    // A pool reads its factory by its URL, which says how far ahead its consumers fetch (ROADMAP
    // M12-3): what a borrower held ahead is back with the broker for the next one.
    @Test
    void whatABorrowerHeldAheadIsThereForTheNext() {
        try (TassisConnectionPool ahead = TassisConnectionPool
                .over(TassisConnectionFactory.fromUrl("tassis:amqp://localhost:" + relay.port() + "?prefetch=5"))
                .maxSize(1).build()) {
            try (JMSContext context = ahead.createContext()) {
                for (int i = 0; i < 5; i++) {
                    context.createProducer().send(context.createQueue("orders"), "n" + i);
                }
                assertEquals("n0", context.createConsumer(context.createQueue("orders")).receiveBody(String.class, SOON));
            }

            try (JMSContext context = ahead.createContext()) {
                JMSConsumer consumer = context.createConsumer(context.createQueue("orders"));
                for (int i = 1; i < 5; i++) {
                    assertEquals("n" + i, consumer.receiveBody(String.class, SOON));
                }
            }
            assertEquals(1, relay.accepted(), "the same link served both");
        }
    }

    @Test
    void aConnectionGivenAClientIdentifierIsNotKept() throws Exception {
        try (Connection connection = pool.createConnection()) {
            connection.setClientID("billing");
            Session session = connection.createSession();
            session.createDurableSubscriber(session.createTopic("news"), "all").close();
        }

        assertEquals(0, pool.size());
        try (Connection next = pool.createConnection()) {
            // §6.1.2: the identifier is free again, and the next connection has none
            assertEquals(null, next.getClientID());
            next.setClientID("billing");
            next.createSession().unsubscribe("all");
        }
    }

    @Test
    void aConnectionNobodyUsesForLongIsClosedAndItsSocketWithIt() throws Exception {
        try (TassisConnectionPool brief = TassisConnectionPool
                .over(TassisConnectionFactory.fromUrl("tassis:amqp://localhost:" + relay.port()))
                .idleTimeout(Duration.ofMillis(200)).build()) {
            brief.createConnection().close();
            assertEquals(1, relay.accepted());

            await("the idle connection is closed", () -> brief.size() == 0);
            await("and its socket", () -> relay.open() == 0);

            try (JMSContext context = brief.createContext()) {
                context.createProducer().send(context.createQueue("orders"), "on a new connection");
            }
            assertEquals(2, relay.accepted());
        }
    }

    @Test
    void aConnectionLostWhileNobodyUsesItIsDropped() throws Exception {
        pool.createConnection().close();
        assertEquals(1, pool.idle());

        relay.cut();

        await("the pool drops the lost connection", () -> pool.size() == 0);
        try (JMSContext context = pool.createContext()) {
            context.createProducer().send(context.createQueue("orders"), "on a new connection");
            assertEquals("on a new connection",
                    context.createConsumer(context.createQueue("orders")).receiveBody(String.class, SOON));
        }
        assertEquals(2, relay.accepted());
    }

    @Test
    void aConnectionLostWhileBorrowedTellsItsBorrowerAndIsNotKept() throws Exception {
        LinkedBlockingQueue<JMSException> told = new LinkedBlockingQueue<>();
        Connection connection = pool.createConnection();
        connection.setExceptionListener(told::add);
        connection.start();

        relay.cut();

        // §6.1.7: the application is told through its exception listener
        assertNotNull(told.poll(10, TimeUnit.SECONDS), "the borrower is told");
        connection.close();
        assertEquals(0, pool.size(), "a lost connection is not lent again");
        pool.createConnection().close();
        assertEquals(1, pool.idle());
    }
}
