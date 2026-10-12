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
package io.vidocq.tassis.core.internal.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.internal.broker.BrokerConnection.Consumer;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Delivery;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Outgoing;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Subscribing;
import io.vidocq.tassis.core.internal.message.Messages;
import io.vidocq.tassis.core.internal.message.TassisTextMessage;
import io.vidocq.tassis.core.internal.selector.Selector;
import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What a receive that waits is woken by (ROADMAP M12-2): whatever concerns its queue or its
 * subscription, its consumer or its connection, and nothing else. A receive woken for nothing
 * is slow, which `BENCH.md` measures; one not woken when it should be never returns, which is
 * what most of these tests are about (Jakarta Messaging 3.1 §4.1, §4.2, §6.1.4, §8.8).
 */
@Timeout(30)
class WaitingReceiveTest {

    private static final long SOON = TimeUnit.SECONDS.toNanos(10);

    private Broker broker;
    private BrokerConnection connection;
    private BrokerConnection other;

    @BeforeEach
    void connect() throws JMSException {
        broker = Broker.named("waiting-" + UUID.randomUUID());
        connection = broker.connect(null, null);
        connection.start();
        other = broker.connect(null, null);
        other.start();
    }

    @AfterEach
    void disconnect() {
        connection.close();
        other.close();
    }

    // ---- helpers -----------------------------------------------------------

    private Outgoing to(String destination, boolean topic, String text, long deliveryTime) throws JMSException {
        TassisTextMessage message = new TassisTextMessage();
        message.setText(text);
        message.setJMSMessageID(other.nextMessageId());
        message.setJMSDeliveryTime(deliveryTime);
        return new Outgoing(destination, topic, false, Messages.snapshot(message));
    }

    private Outgoing regional(String region) throws JMSException {
        TassisTextMessage message = new TassisTextMessage();
        message.setText(region);
        message.setStringProperty("region", region);
        message.setJMSMessageID(other.nextMessageId());
        return new Outgoing("orders", false, false, Messages.snapshot(message));
    }

    private Outgoing toQueue(String queue, String text) throws JMSException {
        return to(queue, false, text, 0);
    }

    private static String text(Delivery delivery) throws JMSException {
        return delivery == null ? null : ((TextMessage) delivery.message()).getText();
    }

    /**
     * A receive made by a thread of its own, which is waiting in the broker when this returns:
     * the broker says so, the state of a thread would not tell that wait from another.
     */
    private <T> CompletableFuture<T> waitingIn(Callable<T> call) throws InterruptedException {
        int already = broker.waiting();
        CompletableFuture<T> result = new CompletableFuture<>();
        Thread thread = Thread.ofVirtual().start(() -> {
            try {
                result.complete(call.call());
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });
        long deadline = System.nanoTime() + SOON;
        while (broker.waiting() <= already) {
            assertTrue(System.nanoTime() < deadline && !result.isDone() && thread.isAlive(), "the call does not wait");
            Thread.sleep(2);
        }
        return result;
    }

    private static <T> T soon(CompletableFuture<T> result) throws Exception {
        return result.get(10, TimeUnit.SECONDS);
    }

    // ---- what does not wake a receive ---------------------------------------

    @Test
    void aReceiveThatWaitsIsNotWokenByWhatHappensToAnotherQueue() throws Exception {
        Consumer idle = connection.attach("idle", false, null);
        CompletableFuture<Delivery> waiting = waitingIn(() -> idle.receive(BrokerConnection.FOREVER, true));
        Consumer busy = other.attach("busy", false, null);
        long before = broker.awakenings();

        for (int i = 0; i < 50; i++) {
            other.send(toQueue("busy", "message " + i));
            assertNotNull(busy.receive(0, true));
        }
        busy.close();

        assertEquals(before, broker.awakenings(), "woken for what was no concern of its queue");
        other.send(toQueue("idle", "at last"));
        assertEquals("at last", text(soon(waiting)));
    }

    @Test
    void aSubscriberThatWaitsIsNotWokenByWhatIsPublishedToAnotherTopic() throws Exception {
        Consumer idle = connection.subscribe(new Subscribing("quiet", false, null, false, false, false, null, false));
        CompletableFuture<Delivery> waiting = waitingIn(() -> idle.receive(BrokerConnection.FOREVER, true));
        other.subscribe(new Subscribing("loud", false, null, false, false, false, null, false));
        long before = broker.awakenings();

        for (int i = 0; i < 50; i++) {
            other.send(to("loud", true, "message " + i, 0));
        }

        assertEquals(before, broker.awakenings(), "woken for what was no concern of its subscription");
        other.send(to("quiet", true, "at last", 0));
        assertEquals("at last", text(soon(waiting)));
    }

    // ---- what wakes it -----------------------------------------------------

    @Test
    void aMessageSentToItsQueueWakesEveryReceiveThatWaitsThere() throws Exception {
        Consumer first = connection.attach("orders", false, null);
        Consumer second = other.attach("orders", false, null);
        CompletableFuture<Delivery> one = waitingIn(() -> first.receive(BrokerConnection.FOREVER, true));
        CompletableFuture<Delivery> two = waitingIn(() -> second.receive(BrokerConnection.FOREVER, true));

        long before = broker.awakenings();

        other.commit(List.of(toQueue("orders", "a"), toQueue("orders", "b")), List.of());

        assertEquals(List.of("a", "b"), List.of(text(soon(one)), text(soon(two))).stream().sorted().toList());
        assertEquals(before + 2, broker.awakenings());
    }

    @Test
    void aMessageGivenBackWakesAReceiveThatWaitsForIt() throws Exception {
        other.send(toQueue("orders", "again"));
        Consumer taker = other.attach("orders", false, null);
        Delivery taken = taker.receive(0, false);
        Consumer consumer = connection.attach("orders", false, null);
        CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

        other.redeliver(List.of(taken));

        Delivery delivery = soon(waiting);
        assertEquals("again", text(delivery));
        assertEquals(2, delivery.deliveryCount());
    }

    @Test
    void aMessageThatNeverReachedTheApplicationWakesAReceiveWhenItIsReturned() throws Exception {
        other.send(toQueue("orders", "unseen"));
        Delivery taken = other.attach("orders", false, null).receive(0, false);
        Consumer consumer = connection.attach("orders", false, null);
        CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

        other.returnUndelivered(taken);

        assertEquals(1, soon(waiting).deliveryCount());
    }

    @Test
    void theMessagesAConnectionLeavesUnsettledWakeAReceiveWhenItCloses() throws Exception {
        BrokerConnection leaving = broker.connect(null, null);
        leaving.start();
        leaving.send(toQueue("orders", "left behind"));
        assertNotNull(leaving.attach("orders", false, null).receive(0, false));
        Consumer consumer = connection.attach("orders", false, null);
        CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

        leaving.close();

        assertEquals("left behind", text(soon(waiting)));
    }

    @Test
    void startingItsConnectionWakesAReceive() throws Exception {
        BrokerConnection stopped = broker.connect(null, null);
        try {
            Consumer consumer = stopped.attach("orders", false, null);
            other.send(toQueue("orders", "held"));
            CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

            stopped.start();

            assertEquals("held", text(soon(waiting)));
        } finally {
            stopped.close();
        }
    }

    @Test
    void closingItsConsumerEndsAReceiveAndWaitsForIt() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

        consumer.close();

        // §8.8: close returns once the receive in progress has completed
        assertEquals(0, broker.waiting());
        assertNull(soon(waiting));
    }

    @Test
    void closingAConsumerEndsEveryReceiveInProgressOnIt() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        CompletableFuture<Delivery> one = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));
        CompletableFuture<Delivery> two = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

        consumer.close();

        assertEquals(0, broker.waiting());
        assertNull(soon(one));
        assertNull(soon(two));
    }

    @Test
    void closingAConsumerLeavesTheOthersOfItsQueueWaiting() throws Exception {
        Consumer leaving = connection.attach("orders", false, null);
        Consumer staying = other.attach("orders", false, null);
        waitingIn(() -> leaving.receive(BrokerConnection.FOREVER, true));
        CompletableFuture<Delivery> waiting = waitingIn(() -> staying.receive(BrokerConnection.FOREVER, true));

        leaving.close();
        other.send(toQueue("orders", "for who stayed"));

        assertEquals("for who stayed", text(soon(waiting)));
    }

    @Test
    void aMessageThatArrivedWhileItsConnectionWasStoppedIsReceivedOnceItStarts() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

        // §6.1.5: a stopped connection delivers nothing
        connection.stop();
        other.send(toQueue("orders", "held"));
        while (broker.awakenings() == 0) {
            Thread.sleep(2);
        }
        assertEquals(1, broker.waiting(), "woken, it found its connection stopped and waits again");
        assertEquals(1, broker.messageCount("orders"));
        connection.start();

        assertEquals("held", text(soon(waiting)));
    }

    @Test
    void closingItsConnectionEndsAReceive() throws Exception {
        BrokerConnection closing = broker.connect(null, null);
        closing.start();
        Consumer consumer = closing.attach("orders", false, null);
        CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

        closing.close();

        assertNull(soon(waiting));
    }

    @Test
    void aConsumerThatStopsListeningEndsTheTakeOfItsPeriod() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        long period = consumer.listen(true);
        CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.take(period));

        consumer.listen(false);

        assertNull(soon(waiting));
    }

    @Test
    void aDelayedMessageIsReceivedWhenItsTimeComes() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        other.send(to("orders", false, "later", System.currentTimeMillis() + 300));

        // §7.9: nothing else happens to the broker in the meantime
        Delivery delivery = consumer.receive(SOON, true);

        assertEquals("later", text(delivery));
        assertTrue(System.currentTimeMillis() >= delivery.message().getJMSDeliveryTime());
    }

    @Test
    void aDelayedMessageThatArrivesWhileAReceiveWaitsIsReceivedWhenItsTimeComes() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        CompletableFuture<Delivery> waiting = waitingIn(() -> consumer.receive(BrokerConnection.FOREVER, true));

        // The receive waited without limit: it now has to wait until that time, and no longer.
        long due = System.currentTimeMillis() + 300;
        other.send(to("orders", false, "later", due));

        assertEquals("later", text(soon(waiting)));
        assertTrue(System.currentTimeMillis() >= due);
    }

    @Test
    void aMessageWakesAReceiveWhoseSelectorAcceptsIt() throws Exception {
        Consumer north = connection.attach("orders", false, Selector.parse("region = 'north'"));
        CompletableFuture<Delivery> waiting = waitingIn(() -> north.receive(BrokerConnection.FOREVER, true));

        // §4.1.2: what its selector rejects stays on the queue, and it goes on waiting
        other.send(regional("south"));
        other.send(regional("north"));

        assertEquals("north", text(soon(waiting)));
        assertEquals(1, broker.messageCount("orders"));
    }

    @Test
    void aMessagePublishedWakesTheSubscribersOfItsTopicOnly() throws Exception {
        Consumer news = connection.subscribe(new Subscribing("news", false, null, false, false, false, null, false));
        Consumer sport = other.subscribe(new Subscribing("sport", false, null, false, false, false, null, false));
        CompletableFuture<Delivery> waiting = waitingIn(() -> news.receive(BrokerConnection.FOREVER, true));

        other.send(to("news", true, "headline", 0));

        assertEquals("headline", text(soon(waiting)));
        assertNull(sport.receive(0, true));
    }
}
