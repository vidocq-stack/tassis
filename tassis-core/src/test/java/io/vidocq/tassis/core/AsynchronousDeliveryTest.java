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
package io.vidocq.tassis.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.CompletionListener;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Asynchronous delivery to message listeners (Jakarta Messaging 3.1 §8.7, §6.2.13),
 * what may be called from a callback (§6.1, §6.2), and asynchronous send (§7.3).
 */
class AsynchronousDeliveryTest {

    private static final long SOON = 5;
    private static final long BRIEFLY = 150;

    private Connection connection;
    private Session session;
    private Queue queue;
    private final LinkedBlockingQueue<String> received = new LinkedBlockingQueue<>();

    @BeforeEach
    void connect() throws JMSException {
        connection = new TassisConnectionFactory("test-" + UUID.randomUUID()).createConnection();
        session = connection.createSession();
        queue = session.createQueue("orders");
        connection.start();
    }

    @AfterEach
    void disconnect() throws JMSException {
        connection.close();
    }

    /** Sends through a session of its own: the session under test belongs to its listeners. */
    private void send(Queue destination, String... texts) throws JMSException {
        try (Session sender = connection.createSession()) {
            MessageProducer producer = sender.createProducer(destination);
            for (String text : texts) {
                producer.send(sender.createTextMessage(text));
            }
        }
    }

    private static String text(Message message) {
        try {
            return ((TextMessage) message).getText();
        } catch (JMSException e) {
            throw new AssertionError(e);
        }
    }

    private String next() throws InterruptedException {
        return received.poll(SOON, TimeUnit.SECONDS);
    }

    private String nextIfAny() throws InterruptedException {
        return received.poll(BRIEFLY, TimeUnit.MILLISECONDS);
    }

    // ---- message listeners -----------------------------------------------

    @Test
    void listenerGetsTheMessagesInOrder() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);
        MessageListener listener = message -> received.add(text(message));
        consumer.setMessageListener(listener);

        send(queue, "one", "two", "three");

        assertEquals("one", next());
        assertEquals("two", next());
        assertEquals("three", next());
        assertSame(listener, consumer.getMessageListener());
    }

    @Test
    void listenersOfOneSessionAreNeverCalledConcurrently() throws Exception {
        Queue other = session.createQueue("other");
        AtomicInteger running = new AtomicInteger();
        AtomicInteger overlaps = new AtomicInteger();
        MessageListener listener = message -> {
            if (running.incrementAndGet() > 1) {
                overlaps.incrementAndGet();
            }
            Thread.onSpinWait();
            received.add(text(message));
            running.decrementAndGet();
        };
        session.createConsumer(queue).setMessageListener(listener);
        session.createConsumer(other).setMessageListener(listener);

        for (int i = 0; i < 50; i++) {
            send(queue, "a" + i);
            send(other, "b" + i);
        }
        for (int i = 0; i < 100; i++) {
            next();
        }

        assertEquals(0, overlaps.get(), "§6.2.13: a session serialises the execution of its listeners");
    }

    @Test
    void listenerIsNotCalledWhileTheConnectionIsStopped() throws Exception {
        session.createConsumer(queue).setMessageListener(message -> received.add(text(message)));
        connection.stop();

        send(queue, "held");

        assertNull(nextIfAny());
        connection.start();
        assertEquals("held", next());
    }

    @Test
    void stopWaitsForTheListenerInProgress() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        session.createConsumer(queue).setMessageListener(message -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        send(queue, "slow");
        assertTrue(entered.await(SOON, TimeUnit.SECONDS));

        CompletableFuture<Void> stopped = CompletableFuture.runAsync(() -> {
            try {
                connection.stop();
            } catch (JMSException e) {
                throw new AssertionError(e);
            }
        });

        assertThrows(java.util.concurrent.TimeoutException.class, () -> stopped.get(BRIEFLY, TimeUnit.MILLISECONDS),
                "§6.1.5: stop returns only once the running listener has returned");
        release.countDown();
        stopped.get(SOON, TimeUnit.SECONDS);
    }

    @Test
    void messageIsRedeliveredWhenAnAutoAcknowledgedListenerThrows() throws Exception {
        List<Message> deliveries = new CopyOnWriteArrayList<>();
        session.createConsumer(queue).setMessageListener(message -> {
            deliveries.add(message);
            if (deliveries.size() == 1) {
                throw new IllegalStateException("first attempt fails");
            }
            received.add(text(message));
        });

        send(queue, "retried");

        assertEquals("retried", next());
        assertFalse(deliveries.get(0).getJMSRedelivered());
        assertTrue(deliveries.get(1).getJMSRedelivered(), "§8.7: redelivered with JMSRedelivered set");
        assertEquals(2, deliveries.get(1).getIntProperty("JMSXDeliveryCount"));
    }

    @Test
    void recoverInAnAutoAcknowledgedListenerRedeliversTheMessageInHand() throws Exception {
        session.createConsumer(queue).setMessageListener(message -> {
            try {
                received.add(text(message) + (message.getJMSRedelivered() ? " again" : ""));
                if (text(message).equals("two") && !message.getJMSRedelivered()) {
                    session.recover();
                }
            } catch (JMSException e) {
                received.add("failed: " + e);
            }
        });

        send(queue, "one", "two");

        assertEquals("one", next());
        assertEquals("two", next());
        assertEquals("two again", next(), "§6.2.10: the message is acknowledged only when onMessage returns");
        assertNull(nextIfAny(), "the first message was acknowledged when its onMessage returned");
    }

    @Test
    void listenerThatThrowsAnErrorNeitherLosesTheMessageNorStopsTheConsumer() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        session.createConsumer(queue).setMessageListener(message -> {
            if (attempts.incrementAndGet() == 1) {
                throw new AssertionError("an Error, not a RuntimeException");
            }
            received.add(text(message));
        });

        send(queue, "one", "two");

        assertEquals("one", next(), "the message is redelivered like after any other failure");
        assertEquals("two", next(), "and the consumer keeps listening");
    }

    @Test
    void messageIsDroppedAfterTooManyFailedDeliveriesToAListener() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        session.createConsumer(queue).setMessageListener(message -> {
            if (text(message).equals("poison")) {
                attempts.incrementAndGet();
                throw new IllegalStateException("always fails");
            }
            received.add(text(message));
        });

        send(queue, "poison", "healthy");

        assertEquals("healthy", next(), "the poison message must not block the queue for ever");
        assertEquals(10, attempts.get());
    }

    @Test
    void clientAcknowledgedListenerMovesOnAfterAnException() throws Exception {
        try (Session clientAcknowledged = connection.createSession(Session.CLIENT_ACKNOWLEDGE)) {
            clientAcknowledged.createConsumer(queue).setMessageListener(message -> {
                received.add(text(message));
                if (text(message).equals("one") && received.size() == 1) {
                    throw new IllegalStateException("fails once");
                }
            });

            send(queue, "one", "two");

            assertEquals("one", next());
            assertEquals("two", next(), "§8.7: the next message is delivered");
            assertNull(nextIfAny(), "no automatic redelivery in a client-acknowledged session");
        }
    }

    @Test
    void transactedListenerCommitsFromOnMessage() throws Exception {
        try (Session transacted = connection.createSession(Session.SESSION_TRANSACTED)) {
            transacted.createConsumer(queue).setMessageListener(message -> {
                try {
                    transacted.commit();
                    received.add(text(message));
                } catch (JMSException e) {
                    received.add("commit failed: " + e);
                }
            });

            send(queue, "one");

            assertEquals("one", next());
        }
        try (Session check = connection.createSession()) {
            assertNull(check.createConsumer(queue).receiveNoWait(), "the commit consumed the message");
        }
    }

    @Test
    void listenerMayNotCloseOrStopWhatItRunsOn() throws Exception {
        LinkedBlockingQueue<Object> outcomes = new LinkedBlockingQueue<>();
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(message -> {
            outcomes.add(outcome(session::close));
            outcomes.add(outcome(connection::stop));
            outcomes.add(outcome(connection::close));
            outcomes.add(outcome(consumer::close));
        });

        send(queue, "one");

        for (int i = 0; i < 3; i++) {
            assertInstanceOf(jakarta.jms.IllegalStateException.class, outcomes.poll(SOON, TimeUnit.SECONDS));
        }
        assertEquals("ok", outcomes.poll(SOON, TimeUnit.SECONDS),
                "§8.8: a listener may close its own consumer");
    }

    private interface Action {
        void run() throws Exception;
    }

    private static Object outcome(Action action) {
        try {
            action.run();
            return "ok";
        } catch (Exception e) {
            return e;
        }
    }

    @Test
    void closingAConsumerWaitsForItsListenerInProgress() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(message -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        send(queue, "slow");
        assertTrue(entered.await(SOON, TimeUnit.SECONDS));

        CompletableFuture<Void> closed = CompletableFuture.runAsync(() -> {
            try {
                consumer.close();
            } catch (JMSException e) {
                throw new AssertionError(e);
            }
        });

        assertThrows(java.util.concurrent.TimeoutException.class, () -> closed.get(BRIEFLY, TimeUnit.MILLISECONDS));
        release.countDown();
        closed.get(SOON, TimeUnit.SECONDS);
    }

    @Test
    void removingTheListenerStopsAsynchronousDelivery() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(message -> received.add(text(message)));
        send(queue, "one");
        assertEquals("one", next());

        consumer.setMessageListener(null);
        send(queue, "two");

        assertNull(nextIfAny());
        assertNull(consumer.getMessageListener());
        assertEquals("two", text(consumer.receive(TimeUnit.SECONDS.toMillis(SOON))));
    }

    @Test
    void listenerMayReplaceItselfFromOnMessage() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);
        MessageListener second = message -> received.add("second:" + text(message));
        consumer.setMessageListener(message -> {
            received.add("first:" + text(message));
            try {
                consumer.setMessageListener(null);
                consumer.setMessageListener(second);
            } catch (JMSException e) {
                received.add("failed: " + e);
            }
        });

        send(queue, "one", "two", "three");

        assertEquals("first:one", next());
        assertEquals("second:two", next());
        assertEquals("second:three", next());
        assertNull(nextIfAny(), "one dispatcher only: no message is delivered twice");
    }

    @Test
    void consumerWithAListenerRefusesSynchronousReceive() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(message -> received.add(text(message)));

        assertThrows(jakarta.jms.IllegalStateException.class, consumer::receiveNoWait);
        assertThrows(jakarta.jms.IllegalStateException.class, () -> consumer.receive(10));
    }

    // ---- asynchronous send -----------------------------------------------

    /** Records the callbacks of asynchronous sends. */
    private final class Completions implements CompletionListener {

        final LinkedBlockingQueue<Message> completed = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<Thread> threads = new LinkedBlockingQueue<>();

        @Override
        public void onCompletion(Message message) {
            threads.add(Thread.currentThread());
            completed.add(message);
        }

        @Override
        public void onException(Message message, Exception exception) {
            completed.add(message);
        }
    }

    @Test
    void asynchronousSendCallsBackFromAnotherThreadOnceTheMessageIsSent() throws Exception {
        Completions completions = new Completions();
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("async");

        producer.send(message, completions);

        assertSame(message, completions.completed.poll(SOON, TimeUnit.SECONDS));
        assertNotEquals(Thread.currentThread(), completions.threads.poll(SOON, TimeUnit.SECONDS),
                "§7.3.8: never from the thread that called send");
        assertTrue(message.getJMSMessageID().startsWith("ID:"));
        assertEquals("async", text(session.createConsumer(queue).receive(TimeUnit.SECONDS.toMillis(SOON))));
    }

    @Test
    void completionCallbacksFollowTheOrderOfTheSends() throws Exception {
        Completions completions = new Completions();
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < 20; i++) {
            producer.send(session.createTextMessage("m" + i), completions);
        }

        for (int i = 0; i < 20; i++) {
            assertEquals("m" + i, text(completions.completed.poll(SOON, TimeUnit.SECONDS)));
        }
    }

    @Test
    void closingTheSessionWaitsForTheCompletionCallbacks() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger returned = new AtomicInteger();
        session.createProducer(queue).send(session.createTextMessage("async"), new CompletionListener() {
            @Override
            public void onCompletion(Message message) {
                try {
                    release.await(BRIEFLY, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                returned.incrementAndGet();
            }

            @Override
            public void onException(Message message, Exception exception) {
            }
        });

        session.close();

        assertEquals(1, returned.get(), "§7.3.4: close returns after the callbacks have returned");
    }

    @Test
    void completionCallbackMayNotCloseOrCommitItsOwnSession() throws Exception {
        LinkedBlockingQueue<Object> outcomes = new LinkedBlockingQueue<>();
        try (Session transacted = connection.createSession(Session.SESSION_TRANSACTED)) {
            MessageProducer producer = transacted.createProducer(queue);
            producer.send(transacted.createTextMessage("async"), new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                    outcomes.add(outcome(transacted::commit));
                    outcomes.add(outcome(transacted::rollback));
                    outcomes.add(outcome(transacted::close));
                    outcomes.add(outcome(producer::close));
                    outcomes.add(outcome(connection::close));
                }

                @Override
                public void onException(Message message, Exception exception) {
                }
            });

            for (int i = 0; i < 5; i++) {
                assertInstanceOf(jakarta.jms.IllegalStateException.class, outcomes.poll(SOON, TimeUnit.SECONDS));
            }
            transacted.commit();
        }
    }

    @Test
    void failedAsynchronousSendThrowsInTheCallerAndNeverCallsBack() throws Exception {
        Completions completions = new Completions();
        MessageProducer producer = session.createProducer(queue);

        assertThrows(MessageFormatException.class, () -> producer.send(null, completions));
        assertThrows(IllegalArgumentException.class,
                () -> producer.send(session.createTextMessage("x"), (CompletionListener) null));

        assertNull(completions.completed.poll(BRIEFLY, TimeUnit.MILLISECONDS));
    }
}
