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

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The question of a pool of sessions (ROADMAP M11b): what {@link RoundTripBench} does, with a
 * session, a producer and a consumer opened for each message and closed after it, on a
 * connection that stays open. The difference with {@code RoundTripBench} at 100 bytes is what a
 * pool of sessions could save.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
public class SessionBench {

    @Param({"tassis-vm", "artemis-vm", "tassis-amqp", "qpid-tassis"})
    public String provider;

    private Rig rig;
    private Connection connection;
    private Queue queue;
    private final byte[] body = new byte[100];

    @Setup
    public void open() throws Exception {
        rig = Rig.start(provider);
        connection = rig.connect();
        try (Session session = connection.createSession()) {
            queue = session.createQueue("session");
        }
    }

    @TearDown
    public void close() {
        rig.close();
    }

    @Benchmark
    public Message sessionPerRoundTrip() throws JMSException {
        try (Session session = connection.createSession()) {
            MessageProducer producer = Rig.producer(session, queue);
            MessageConsumer consumer = session.createConsumer(queue);
            producer.send(Rig.message(session, body));
            return Rig.received(consumer.receive(Rig.SOON));
        }
    }
}
