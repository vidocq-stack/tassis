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
package io.vidocq.tassis.server;

import io.vidocq.tassis.core.internal.broker.Broker;
import io.vidocq.tassis.server.internal.AmqpConnection;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Opens a broker of this JVM to the network: clients connect to it with AMQP 1.0.
 *
 * <pre>{@code
 * try (TassisServer server = TassisServer.forBroker("orders").port(5672).start()) {
 *     // AMQP clients reach the broker "orders" at amqp://localhost:5672
 * }
 * }</pre>
 *
 * <p>The broker is the one {@code new TassisConnectionFactory("orders")} connects to in this
 * JVM: clients of the network and clients of the JVM share its queues. Its users are those
 * declared with {@code TassisBroker}: a secured broker asks network clients for a user name
 * and a password too.
 *
 * <p><strong>The connection is not encrypted.</strong> A password travels in clear, like the
 * messages. The server therefore listens on the loopback address unless told otherwise with
 * {@link Builder#host(String)}: open it to a network you trust, or not at all.
 *
 * <p>A client of the network has what a client of the JVM has: queues and topics, durable
 * and shared subscriptions, message selectors, temporary destinations, local transactions,
 * queue browsers. Distributed transactions are not carried by AMQP 1.0.
 */
public final class TassisServer implements AutoCloseable {

    /** The port AMQP is registered with, used when none is given. */
    public static final int DEFAULT_PORT = 5672;

    private static final System.Logger LOG = System.getLogger(TassisServer.class.getName());

    private static final long ACCEPT_RETRY_MILLIS = 100;
    private static final long CLOSE_TIME_OUT_MILLIS = 10_000;

    private final String brokerName;
    private final ServerSocket listener;
    private final Set<AmqpConnection> connections = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    private TassisServer(String brokerName, ServerSocket listener) {
        this.brokerName = brokerName;
        this.listener = listener;
    }

    /** Starts describing a server for the broker of that name in this JVM. */
    public static Builder forBroker(String brokerName) {
        Objects.requireNonNull(brokerName, "brokerName");
        if (brokerName.isEmpty()) {
            throw new IllegalArgumentException("a broker name must not be empty");
        }
        return new Builder(brokerName);
    }

    /** What a server is started with. */
    public static final class Builder {

        private final String brokerName;
        private String host;
        private int port = DEFAULT_PORT;

        private Builder(String brokerName) {
            this.brokerName = brokerName;
        }

        /**
         * The port to listen on: 5672 by default, or 0 for any free port, which
         * {@link TassisServer#port()} then tells.
         */
        public Builder port(int newPort) {
            if (newPort < 0 || newPort > 0xFFFF) {
                throw new IllegalArgumentException(newPort + " is not a port");
            }
            port = newPort;
            return this;
        }

        /**
         * The address to listen on. By default the loopback address, which only this machine
         * reaches; {@code "0.0.0.0"} listens on every interface.
         */
        public Builder host(String newHost) {
            host = Objects.requireNonNull(newHost, "host");
            return this;
        }

        /**
         * Starts listening.
         *
         * @throws UncheckedIOException if the address cannot be listened on
         */
        public TassisServer start() {
            try {
                InetAddress address = host == null ? InetAddress.getLoopbackAddress() : InetAddress.getByName(host);
                ServerSocket listener = new ServerSocket();
                listener.setReuseAddress(true);
                listener.bind(new InetSocketAddress(address, port));
                TassisServer server = new TassisServer(brokerName, listener);
                Thread.ofVirtual().name("tassis-server-" + brokerName).start(server::accept);
                return server;
            } catch (IOException e) {
                throw new UncheckedIOException("cannot listen on port " + port, e);
            }
        }
    }

    /** The port the server listens on. */
    public int port() {
        return listener.getLocalPort();
    }

    /** The name of the broker the server opens to the network. */
    public String brokerName() {
        return brokerName;
    }

    private void accept() {
        Broker broker = Broker.named(brokerName);
        while (!closed) {
            try {
                Socket socket = listener.accept();
                AmqpConnection connection = new AmqpConnection(socket, broker, connections::remove);
                connections.add(connection);
                if (closed) {
                    // close() may have gone through the connections before this one was among them
                    connection.abort();
                }
                connection.start();
            } catch (IOException | RuntimeException e) {
                if (closed) {
                    return;
                }
                // Out of file descriptors, for instance: that passes, and the server must still be
                // listening when it has. Not in a tight loop, though.
                LOG.log(System.Logger.Level.WARNING, "a connection to " + brokerName + " could not be accepted", e);
                try {
                    Thread.sleep(ACCEPT_RETRY_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * Stops listening and ends the connections of the clients. What they had not acknowledged
     * is back in its queue when this returns; the broker itself, and what it holds, stays.
     */
    @Override
    public void close() {
        closed = true;
        try {
            listener.close();
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "closing the listener", e);
        }
        List<AmqpConnection> ending = List.copyOf(connections);
        for (AmqpConnection connection : ending) {
            connection.abort();
        }
        // Not returned from before the broker has back what the clients held.
        for (AmqpConnection connection : ending) {
            connection.awaitEnd(CLOSE_TIME_OUT_MILLIS);
        }
    }
}
