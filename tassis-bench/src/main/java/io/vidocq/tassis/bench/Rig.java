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
package io.vidocq.tassis.bench;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * A broker and the client that talks to it: what a benchmark is run on. The benchmarks are
 * written once, against Jakarta Messaging, and the rig says who is behind it.
 *
 * <table>
 * <caption>Providers</caption>
 * <tr><th>Name</th><th>Client</th><th>Broker</th></tr>
 * <tr><td>{@code tassis-vm}</td><td>Tassis</td><td>Tassis, in the JVM</td></tr>
 * <tr><td>{@code artemis-vm}</td><td>Artemis</td><td>Artemis, in the JVM</td></tr>
 * <tr><td>{@code tassis-amqp}</td><td>Tassis</td><td>a {@code TassisServer}, on the loopback</td></tr>
 * <tr><td>{@code tassis-ahead}</td><td>Tassis, fetching a thousand messages ahead as Qpid JMS
 * does</td><td>a {@code TassisServer}, on the loopback</td></tr>
 * <tr><td>{@code qpid-tassis}</td><td>Qpid JMS</td><td>a {@code TassisServer}, on the loopback</td></tr>
 * <tr><td>{@code tassis-artemis}</td><td>Tassis</td><td>Artemis, AMQP 1.0, on the loopback</td></tr>
 * <tr><td>{@code qpid-artemis}</td><td>Qpid JMS</td><td>Artemis, AMQP 1.0, on the loopback</td></tr>
 * </table>
 *
 * <p>Artemis keeps nothing on disk and checks no user, as Tassis does; everything else is what
 * each provider does by default. Messages are sent non-persistent ({@link #producer}).
 */
final class Rig implements AutoCloseable {

    /** How long a receive waits for a message that must come. */
    static final long SOON = 10_000;

    private static final AtomicInteger IDS = new AtomicInteger();

    private final ConnectionFactory factory;
    // What to close, the last opened first.
    private final Deque<AutoCloseable> opened = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    private Rig(String provider, String clientOptions) throws Exception {
        int id = IDS.incrementAndGet();
        try {
            factory = switch (provider) {
                case "tassis-vm" -> new TassisConnectionFactory("bench-" + id);
                case "tassis-amqp" ->
                    TassisConnectionFactory.fromUrl("tassis:amqp://127.0.0.1:" + tassis(id) + clientOptions);
                case "tassis-ahead" ->
                    TassisConnectionFactory.fromUrl("tassis:amqp://127.0.0.1:" + tassis(id) + "?prefetch=1000");
                case "qpid-tassis" -> new JmsConnectionFactory("amqp://127.0.0.1:" + tassis(id) + clientOptions);
                case "artemis-vm" -> {
                    artemis("vm://" + id);
                    ActiveMQConnectionFactory artemis = new ActiveMQConnectionFactory("vm://" + id + clientOptions);
                    opened.push(artemis);
                    yield artemis;
                }
                case "tassis-artemis" ->
                    TassisConnectionFactory.fromUrl("tassis:amqp://127.0.0.1:" + artemis(id) + clientOptions);
                case "qpid-artemis" -> new JmsConnectionFactory("amqp://127.0.0.1:" + artemis(id) + clientOptions);
                default -> throw new IllegalArgumentException("unknown provider: " + provider);
            };
        } catch (Exception | Error e) {
            close();
            throw e;
        }
    }

    static Rig start(String provider) throws Exception {
        return new Rig(provider, "");
    }

    /**
     * @param clientOptions the query of the URL of the client: of Qpid JMS, as
     *        {@code ?jms.prefetchPolicy.all=1}, of Artemis, as {@code ?consumerWindowSize=0}, or of
     *        Tassis over the network, as {@code ?prefetch=1000}
     */
    static Rig start(String provider, String clientOptions) throws Exception {
        return new Rig(provider, clientOptions);
    }

    private int tassis(int id) {
        TassisServer server = TassisServer.forBroker("bench-" + id).port(0).start();
        opened.push(server);
        return server.port();
    }

    private int artemis(int id) throws Exception {
        int port = freePort();
        artemis("tcp://127.0.0.1:" + port + "?protocols=AMQP");
        return port;
    }

    private void artemis(String acceptor) throws Exception {
        ConfigurationImpl configuration = new ConfigurationImpl();
        configuration.setPersistenceEnabled(false);
        configuration.setSecurityEnabled(false);
        configuration.setJMXManagementEnabled(false);
        configuration.addAcceptorConfiguration("bench", acceptor);
        EmbeddedActiveMQ broker = new EmbeddedActiveMQ();
        broker.setConfiguration(configuration);
        broker.start();
        opened.push(broker::stop);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    ConnectionFactory factory() {
        return factory;
    }

    /** A started connection, closed with the rig if nobody did before. */
    Connection connect() throws JMSException {
        Connection connection = factory.createConnection();
        lock.lock();
        try {
            opened.push(connection);
        } finally {
            lock.unlock();
        }
        connection.start();
        return connection;
    }

    /** A producer of non-persistent messages: no provider here keeps anything on disk. */
    static MessageProducer producer(Session session, Queue queue) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return producer;
    }

    static BytesMessage message(Session session, byte[] body) throws JMSException {
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(body);
        return message;
    }

    /** The message a receive returned, which had to be one. */
    static Message received(Message message) {
        if (message == null) {
            throw new IllegalStateException("no message after " + SOON + " ms");
        }
        return message;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            while (!opened.isEmpty()) {
                try {
                    opened.pop().close();
                } catch (Exception e) {
                    // A benchmark may have closed it already, or its peer is gone: the rest still closes.
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
