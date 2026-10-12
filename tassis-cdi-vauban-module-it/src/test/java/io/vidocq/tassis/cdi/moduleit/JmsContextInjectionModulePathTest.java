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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import io.vidocq.tassis.server.TassisServer;
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

    /** Passes octets between the clients and a server, and counts the clients. */
    private static final class Relay implements AutoCloseable {

        private final ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        private final int serverPort;
        private final AtomicInteger accepted = new AtomicInteger();
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();

        Relay(int serverPort) throws IOException {
            this.serverPort = serverPort;
            Thread.ofVirtual().start(this::accept);
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

        @Override
        public void close() throws IOException {
            listener.close();
            for (Socket socket : sockets) {
                socket.close();
            }
        }
    }

    @Test
    void contextsInjectedOnABrokerOfTheNetworkShareTheConnectionsOfAPool() throws Exception {
        try (TassisServer server = TassisServer.forBroker(broker).port(0).start();
                Relay relay = new Relay(server.port())) {
            System.clearProperty("tassis.broker");
            System.setProperty("tassis.server", "amqp://localhost:" + relay.listener.getLocalPort());
            System.setProperty("tassis.factory.audit.server", "amqp://localhost:" + relay.listener.getLocalPort());
            try {
                var builder = VaubanContainer.builder();
                TassisCdi.BEANS.forEach(builder::addBeanClass);
                try (VaubanContainer container = builder.addBeanClass(OrderDesk.class).build()) {
                    OrderDesk desk = container.select(OrderDesk.class);
                    List<String> orders = new CopyOnWriteArrayList<>();

                    // §12.4.4: each request has its own contexts, closed when it ends
                    for (int request = 1; request <= 5; request++) {
                        String number = String.valueOf(request);
                        container.requestContext().runInScope(() -> {
                            desk.place(number);
                            orders.add(desk.nextOrder());
                        });
                    }

                    assertEquals(List.of("1", "2", "3", "4", "5"), orders);
                    assertEquals(2, relay.accepted.get(),
                            "five requests, two factories: one connection each, kept by its pool");
                }
            } finally {
                System.clearProperty("tassis.server");
                System.clearProperty("tassis.factory.audit.server");
            }
        }
    }
}
