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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
 * A known cost of the single broker lock: every change of the broker wakes every receive that
 * waits on it. One thread sends and receives while consumers wait, each on a queue of its own
 * where nothing ever comes.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
public class IdleWaitersBench {

    @Param({"tassis-vm", "artemis-vm"})
    public String provider;

    /** Consumers that wait for nothing. */
    @Param({"0", "100", "1000"})
    public int waiters;

    private Rig rig;
    private Connection idle;
    private final List<Thread> waiting = new ArrayList<>();
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private final byte[] body = new byte[100];
    private volatile boolean closing;
    // What ended a waiter before the benchmark did: there would be fewer of them than announced.
    private volatile Exception lost;

    @Setup
    public void open() throws Exception {
        rig = Rig.start(provider);
        idle = rig.connect();
        CountDownLatch aboutToWait = new CountDownLatch(waiters);
        for (int i = 0; i < waiters; i++) {
            Session own = idle.createSession();
            MessageConsumer waiter = own.createConsumer(own.createQueue("idle-" + i));
            waiting.add(Thread.ofVirtual().name("idle-waiter-" + i).start(() -> {
                aboutToWait.countDown();
                try {
                    waiter.receive();
                } catch (JMSException | RuntimeException e) {
                    // The connection closing under it is how it ends; anything sooner is a loss.
                    if (!closing) {
                        lost = e;
                    }
                }
            }));
        }
        aboutToWait.await();
        awaitWaiting();

        session = rig.connect().createSession();
        Queue queue = session.createQueue("busy");
        producer = Rig.producer(session, queue);
        consumer = session.createConsumer(queue);
    }

    // Nothing tells that a receive is waiting but the state of its thread.
    private void awaitWaiting() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Rig.SOON);
        while (true) {
            long parked = waiting.stream()
                    .filter(thread -> thread.getState() == Thread.State.WAITING
                            || thread.getState() == Thread.State.TIMED_WAITING)
                    .count();
            if (lost != null) {
                throw new IllegalStateException("a waiter ended early", lost);
            }
            if (parked == waiters) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(parked + " consumers waiting, of " + waiters);
            }
            Thread.sleep(20);
        }
    }

    @TearDown
    public void close() throws Exception {
        if (lost != null) {
            throw new IllegalStateException("a waiter ended early", lost);
        }
        closing = true;
        idle.close();
        for (Thread thread : waiting) {
            thread.join(Rig.SOON);
        }
        rig.close();
    }

    @Benchmark
    public Message roundTrip() throws JMSException {
        producer.send(Rig.message(session, body));
        return Rig.received(consumer.receive(Rig.SOON));
    }
}
