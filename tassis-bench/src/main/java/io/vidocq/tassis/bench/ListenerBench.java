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
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.concurrent.CountDownLatch;
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
 * Delivery to a {@code MessageListener} (§8.7), by the way its session acknowledges: one
 * thread sends a batch, and the batch is done when the listener has had all of it. Since M11.1
 * an automatically acknowledged session settles a message after {@code onMessage}, one lock
 * acquisition more than a session that may acknowledge lazily.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
public class ListenerBench {

    private static final int BATCH = 100;

    @Param({"tassis-vm", "artemis-vm", "tassis-amqp", "tassis-ahead", "qpid-tassis"})
    public String provider;

    /** {@code auto}, {@code dups-ok}, or {@code transacted}: the listener commits each message. */
    @Param({"auto", "dups-ok", "transacted"})
    public String mode;

    private Rig rig;
    private Session sending;
    private MessageProducer producer;
    private final byte[] body = new byte[100];
    // The batch being delivered; set before its first message is sent.
    private volatile CountDownLatch batch = new CountDownLatch(0);
    private volatile Throwable failure;

    @Setup
    public void open() throws Exception {
        rig = Rig.start(provider);
        Connection connection = rig.connect();
        Session listening = connection.createSession(
                switch (mode) {
                    case "auto" -> Session.AUTO_ACKNOWLEDGE;
                    case "dups-ok" -> Session.DUPS_OK_ACKNOWLEDGE;
                    case "transacted" -> Session.SESSION_TRANSACTED;
                    default -> throw new IllegalArgumentException("unknown mode: " + mode);
                });
        boolean transacted = mode.equals("transacted");
        Queue queue = listening.createQueue("listener");
        listening.createConsumer(queue).setMessageListener(message -> {
            try {
                if (transacted) {
                    listening.commit();
                }
            } catch (JMSException | RuntimeException e) {
                failure = e;
            }
            batch.countDown();
        });

        sending = rig.connect().createSession();
        producer = Rig.producer(sending, queue);
    }

    @TearDown
    public void close() {
        rig.close();
    }

    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void deliver() throws Exception {
        CountDownLatch delivered = new CountDownLatch(BATCH);
        batch = delivered;
        for (int i = 0; i < BATCH; i++) {
            producer.send(Rig.message(sending, body));
        }
        if (!delivered.await(Rig.SOON, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException(delivered.getCount() + " messages not delivered");
        }
        if (failure != null) {
            throw new IllegalStateException("the listener failed", failure);
        }
    }
}
