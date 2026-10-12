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
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * The question of fetching ahead (ROADMAP M11.2 and M11.3): the Tassis client asks a broker of
 * the network for one message at a time, where Qpid JMS has it send a thousand ahead. A queue
 * that already holds the messages is emptied by a new consumer; the time is per message. Qpid
 * JMS asking for one message at a time shows what the client costs apart from that choice.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class DrainBench {

    private static final int MESSAGES = 10_000;

    /** {@code tassis}, or Qpid JMS with the number of messages it lets the broker send ahead. */
    @Param({"tassis", "qpid-1", "qpid-1000"})
    public String client;

    @Param({"tassis", "artemis"})
    public String broker;

    private Rig rig;
    private Connection connection;
    private Session session;
    private Queue queue;
    private final byte[] body = new byte[100];

    @Setup
    public void open() throws Exception {
        if (client.equals("tassis")) {
            rig = Rig.start(broker.equals("tassis") ? "tassis-amqp" : "tassis-artemis");
        } else {
            String ahead = client.substring("qpid-".length());
            rig = Rig.start("qpid-" + broker, "?jms.prefetchPolicy.all=" + ahead);
        }
        connection = rig.connect();
        session = connection.createSession();
        queue = session.createQueue("drain");
    }

    @Setup(Level.Iteration)
    public void fill() throws JMSException {
        // Sent in a transaction: they are all in the queue when it commits.
        try (Session filling = connection.createSession(Session.SESSION_TRANSACTED)) {
            MessageProducer producer = Rig.producer(filling, queue);
            for (int i = 0; i < MESSAGES; i++) {
                producer.send(Rig.message(filling, body));
            }
            filling.commit();
        }
    }

    @TearDown
    public void close() {
        rig.close();
    }

    // The consumer is created here, not before: one that fetches ahead would have its first
    // messages before the measurement starts.
    @Benchmark
    @OperationsPerInvocation(MESSAGES)
    public void drain(Blackhole blackhole) throws JMSException {
        try (MessageConsumer consumer = session.createConsumer(queue)) {
            for (int i = 0; i < MESSAGES; i++) {
                blackhole.consume(Rig.received(consumer.receive(Rig.SOON)));
            }
        }
    }
}
