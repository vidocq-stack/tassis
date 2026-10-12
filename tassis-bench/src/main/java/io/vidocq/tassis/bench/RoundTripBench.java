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
 * The reference point: one thread sends a message to a queue and receives it, on one session
 * that stays open. Every provider, a small body and a larger one.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
public class RoundTripBench {

    @Param({"tassis-vm", "artemis-vm", "tassis-amqp", "tassis-ahead", "qpid-tassis", "tassis-artemis", "qpid-artemis"})
    public String provider;

    /** Bytes of the body. */
    @Param({"100", "10000"})
    public int size;

    private Rig rig;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private byte[] body;

    @Setup
    public void open() throws Exception {
        rig = Rig.start(provider);
        Connection connection = rig.connect();
        session = connection.createSession();
        Queue queue = session.createQueue("round-trip");
        producer = Rig.producer(session, queue);
        consumer = session.createConsumer(queue);
        body = new byte[size];
    }

    @TearDown
    public void close() {
        rig.close();
    }

    @Benchmark
    public Message roundTrip() throws JMSException {
        producer.send(Rig.message(session, body));
        return Rig.received(consumer.receive(Rig.SOON));
    }
}
