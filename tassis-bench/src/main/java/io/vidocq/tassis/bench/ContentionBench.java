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
import java.util.concurrent.atomic.AtomicInteger;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The question of the single broker lock (ROADMAP, open since M2): several threads, each with a
 * connection and a session of its own, each sending a message and receiving one. On queues of
 * their own they share nothing but the broker, so what does not scale there is the broker's;
 * on a shared queue they compete for the messages too.
 *
 * <p>The client of Artemis is told to fetch nothing ahead ({@code consumerWindowSize=0}), on
 * both kinds of queues so that they compare: on a shared queue, a consumer that fetches ahead
 * holds the messages another waits for. Tassis has nothing to be told, it never does. Artemis as
 * it comes is in {@link RoundTripBench}.
 *
 * <p>The threads are those of JMH, platform threads.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
public class ContentionBench {

    @Param({"tassis-vm", "artemis-vm"})
    public String provider;

    /** {@code own}: a queue per thread; {@code shared}: one queue for all. */
    @Param({"own", "shared"})
    public String queues;

    private Rig rig;
    private final AtomicInteger workers = new AtomicInteger();

    @Setup
    public void open() throws Exception {
        rig = Rig.start(provider, "?consumerWindowSize=0");
    }

    @TearDown
    public void close() {
        rig.close();
    }

    /** What one thread works with; its connection closes with the rig. */
    @State(Scope.Thread)
    public static class Worker {

        private Session session;
        private MessageProducer producer;
        private MessageConsumer consumer;
        private final byte[] body = new byte[100];

        @Setup
        public void open(ContentionBench bench) throws JMSException {
            Connection connection = bench.rig.connect();
            session = connection.createSession();
            String name = bench.queues.equals("own") ? "contention-" + bench.workers.incrementAndGet() : "contention";
            Queue queue = session.createQueue(name);
            producer = Rig.producer(session, queue);
            consumer = session.createConsumer(queue);
        }

        // On a shared queue a thread may take the message of another, never more than were sent:
        // each sends one before it receives one.
        Message roundTrip() throws JMSException {
            producer.send(Rig.message(session, body));
            return Rig.received(consumer.receive(Rig.SOON));
        }
    }

    @Benchmark
    @Threads(1)
    public Message threads01(Worker worker) throws JMSException {
        return worker.roundTrip();
    }

    @Benchmark
    @Threads(2)
    public Message threads02(Worker worker) throws JMSException {
        return worker.roundTrip();
    }

    @Benchmark
    @Threads(4)
    public Message threads04(Worker worker) throws JMSException {
        return worker.roundTrip();
    }

    @Benchmark
    @Threads(8)
    public Message threads08(Worker worker) throws JMSException {
        return worker.roundTrip();
    }

    @Benchmark
    @Threads(16)
    public Message threads16(Worker worker) throws JMSException {
        return worker.roundTrip();
    }
}
