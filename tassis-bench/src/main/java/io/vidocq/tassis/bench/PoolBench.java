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
import io.vidocq.tassis.pool.TassisConnectionPool;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * What the connection pool saves (ROADMAP M11b): a connection opened and closed, by the factory
 * and by a pool over it, on the broker of the JVM and on one of the network. The time is per
 * connection, started and nothing more: no session is created on it. A bounded number of them is opened: each connection of the network leaves a port
 * in use for a while after it closed.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class PoolBench {

    private static final int CONNECTIONS = 100;

    /** {@code vm}: the broker of the JVM; {@code network}: a {@code TassisServer} on the loopback. */
    @Param({"vm", "network"})
    public String broker;

    /** {@code direct}: the factory; {@code pooled}: a pool over it. */
    @Param({"direct", "pooled"})
    public String source;

    private TassisServer server;
    private TassisConnectionPool pool;
    private ConnectionFactory factory;

    @Setup
    public void open() {
        TassisConnectionFactory direct;
        if (broker.equals("network")) {
            server = TassisServer.forBroker("pool-bench").port(0).start();
            direct = TassisConnectionFactory.fromUrl("tassis:amqp://127.0.0.1:" + server.port());
        } else {
            direct = new TassisConnectionFactory("pool-bench");
        }
        if (source.equals("pooled")) {
            pool = TassisConnectionPool.over(direct).build();
            factory = pool;
        } else {
            factory = direct;
        }
    }

    @TearDown
    public void close() {
        if (pool != null) {
            pool.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Benchmark
    @OperationsPerInvocation(CONNECTIONS)
    public void openAndClose() throws JMSException {
        for (int i = 0; i < CONNECTIONS; i++) {
            try (Connection connection = factory.createConnection()) {
                connection.start();
            }
        }
    }
}
