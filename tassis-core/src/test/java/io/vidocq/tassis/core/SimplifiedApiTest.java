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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.InvalidDestinationRuntimeException;
import jakarta.jms.InvalidSelectorRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.Queue;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The simplified API (Jakarta Messaging 3.1 §2.8): {@code JMSContext},
 * {@code JMSProducer} and {@code JMSConsumer}, and their unchecked exceptions (§10.2).
 */
class SimplifiedApiTest {

    private static final long SOON = 5_000;
    private static final long BRIEFLY = 100;

    private TassisConnectionFactory factory;
    private JMSContext context;
    private Queue queue;

    @BeforeEach
    void connect() {
        factory = new TassisConnectionFactory("test-" + UUID.randomUUID());
        context = factory.createContext();
        queue = context.createQueue("orders");
    }

    @AfterEach
    void disconnect() {
        context.close();
    }

    // ---- sending and receiving bodies ------------------------------------

    @Test
    void bodyIsSentAndReceivedWithoutNamingAMessage() {
        context.createProducer().send(queue, "hello");

        assertEquals("hello", context.createConsumer(queue).receiveBody(String.class, SOON));
    }

    @Test
    void everyKindOfBodyHasItsSendMethod() {
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(queue);
        ArrayList<String> serializable = new ArrayList<>(List.of("a"));

        producer.send(queue, Map.<String, Object>of("key", 1));
        producer.send(queue, new byte[] {1, 2});
        producer.send(queue, (Serializable) serializable);
        producer.send(queue, context.createTextMessage("as a message"));

        assertEquals(Map.of("key", 1), consumer.receiveBody(Map.class, SOON));
        assertArrayEquals(new byte[] {1, 2}, consumer.receiveBody(byte[].class, SOON));
        assertEquals(serializable, consumer.receiveBody(Serializable.class, SOON));
        assertEquals("as a message", consumer.receiveBody(String.class, SOON));
        assertNull(consumer.receiveBodyNoWait(String.class));
    }

    @Test
    void connectionStartsWithTheFirstConsumerUnlessAutoStartIsOff() {
        assertTrue(context.getAutoStart());
        try (JMSContext manual = factory.createContext()) {
            manual.setAutoStart(false);
            JMSConsumer consumer = manual.createConsumer(queue);
            context.createProducer().send(queue, "held");

            assertNull(consumer.receive(BRIEFLY), "the connection of this context was never started");

            manual.start();
            assertEquals("held", consumer.receiveBody(String.class, SOON));
        }
    }

    // ---- JMSProducer -----------------------------------------------------

    @Test
    void producerOptionsHeadersAndPropertiesApplyToWhatItSends() throws JMSException {
        Queue replies = context.createQueue("replies");
        TextMessage message = context.createTextMessage("configured");
        message.setStringProperty("color", "set on the message");

        JMSProducer producer = context.createProducer()
                .setPriority(8)
                .setDeliveryMode(DeliveryMode.NON_PERSISTENT)
                .setTimeToLive(60_000)
                .setJMSCorrelationID("corr")
                .setJMSType("order")
                .setJMSReplyTo(replies)
                .setProperty("color", "set on the producer")
                .setProperty("attempt", 3);
        producer.send(queue, message);
        Message received = context.createConsumer(queue).receive(SOON);

        assertEquals(8, received.getJMSPriority());
        assertEquals(DeliveryMode.NON_PERSISTENT, received.getJMSDeliveryMode());
        assertTrue(received.getJMSExpiration() > 0);
        assertEquals("corr", received.getJMSCorrelationID());
        assertEquals("order", received.getJMSType());
        assertEquals(replies, received.getJMSReplyTo());
        assertEquals("set on the producer", received.getStringProperty("color"),
                "a producer property overrides the message's own");
        assertEquals(3, received.getIntProperty("attempt"));
        assertEquals(8, producer.getPriority());
        assertEquals(60_000, producer.getTimeToLive());
        assertEquals("corr", producer.getJMSCorrelationID());
    }

    @Test
    void producerPropertiesConvertLikeMessageProperties() {
        JMSProducer producer = context.createProducer().setProperty("number", "42").setProperty("flag", true);

        assertEquals(42, producer.getIntProperty("number"));
        assertEquals("true", producer.getStringProperty("flag"));
        assertTrue(producer.propertyExists("number"));
        assertEquals(java.util.Set.of("number", "flag"), producer.getPropertyNames());
        assertThrows(MessageFormatRuntimeException.class, () -> producer.getIntProperty("flag"));
        assertThrows(MessageFormatRuntimeException.class, () -> producer.setProperty("list", List.of()));
        assertThrows(IllegalArgumentException.class, () -> producer.setProperty("", 1));

        producer.clearProperties();
        assertFalse(producer.propertyExists("number"));
    }

    @Test
    void asynchronousProducerCallsItsCompletionListener() throws Exception {
        LinkedBlockingQueue<Message> completed = new LinkedBlockingQueue<>();
        CompletionListener listener = new CompletionListener() {
            @Override
            public void onCompletion(Message message) {
                completed.add(message);
            }

            @Override
            public void onException(Message message, Exception exception) {
            }
        };
        JMSProducer producer = context.createProducer().setAsync(listener);
        TextMessage message = context.createTextMessage("async");

        producer.send(queue, message);

        assertSame(message, completed.poll(SOON, TimeUnit.MILLISECONDS));
        assertSame(listener, producer.getAsync());
    }

    // ---- receiveBody that cannot convert (JMSConsumer.receiveBody) ----------

    @Test
    void unconvertibleBodyStaysUndeliveredInAnAutoAcknowledgedContext() throws JMSException {
        context.createProducer().send(queue, "text");
        JMSConsumer consumer = context.createConsumer(queue);

        assertThrows(MessageFormatRuntimeException.class, () -> consumer.receiveBody(Integer.class, SOON));

        Message again = consumer.receive(SOON);
        assertEquals("text", again.getBody(String.class));
        assertFalse(again.getJMSRedelivered(), "the failed call counts for nothing: this is not a redelivery");
        assertEquals(1, again.getIntProperty("JMSXDeliveryCount"));
    }

    @Test
    void unconvertibleBodyCountsAsDeliveredInAClientAcknowledgedContext() throws JMSException {
        context.createProducer().send(queue, "text");
        try (JMSContext clientAcknowledged = factory.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
            JMSConsumer consumer = clientAcknowledged.createConsumer(queue);

            assertThrows(MessageFormatRuntimeException.class, () -> consumer.receiveBody(Integer.class, SOON));
            assertNull(consumer.receiveNoWait(), "the message is not delivered again by itself");

            clientAcknowledged.recover();
            Message again = consumer.receive(SOON);
            assertTrue(again.getJMSRedelivered());
            clientAcknowledged.acknowledge();
        }
        assertNull(context.createConsumer(queue).receiveNoWait(), "acknowledge settled it");
    }

    @Test
    void messageWithoutABodyCannotBeReceivedAsABody() {
        JMSConsumer consumer = context.createConsumer(queue);
        context.createProducer().send(queue, context.createMessage());
        context.createProducer().send(queue, context.createStreamMessage());

        assertThrows(MessageFormatRuntimeException.class, () -> consumer.receiveBody(Object.class, SOON));
        assertInstanceOf(Message.class, consumer.receive(SOON));
        assertThrows(MessageFormatRuntimeException.class, () -> consumer.receiveBody(Object.class, SOON));
    }

    // ---- contexts and their connection -----------------------------------

    @Test
    void contextsCreatedFromOneAnotherShareAConnectionUntilTheLastIsClosed() {
        JMSContext second = context.createContext(JMSContext.AUTO_ACKNOWLEDGE);
        TemporaryQueue temporary = context.createTemporaryQueue();

        JMSConsumer consumer = second.createConsumer(temporary);
        context.createProducer().send(temporary, "same connection");
        assertEquals("same connection", consumer.receiveBody(String.class, SOON),
                "a temporary queue can only be consumed by the connection that created it");

        context.close();
        second.createProducer().send(temporary, "still open");
        assertEquals("still open", consumer.receiveBody(String.class, SOON),
                "the connection, and its temporary queue, live as long as one of the contexts");
        second.close();
        assertThrows(IllegalStateRuntimeException.class, () -> second.createProducer().send(queue, "closed"));
    }

    @Test
    void transactedContextCommitsAndRollsBack() {
        try (JMSContext transacted = factory.createContext(JMSContext.SESSION_TRANSACTED)) {
            assertTrue(transacted.getTransacted());
            assertEquals(JMSContext.SESSION_TRANSACTED, transacted.getSessionMode());
            transacted.createProducer().send(queue, "discarded");
            transacted.rollback();
            transacted.createProducer().send(queue, "kept");
            transacted.commit();
        }

        JMSConsumer consumer = context.createConsumer(queue);
        assertEquals("kept", consumer.receiveBody(String.class, SOON));
        assertNull(consumer.receiveNoWait());
        assertFalse(context.getTransacted());
        assertThrows(IllegalStateRuntimeException.class, context::commit);
    }

    @Test
    void clientIdentifierIsSetBeforeAnythingElse() {
        try (JMSContext fresh = factory.createContext()) {
            fresh.setClientID("reader");
            assertEquals("reader", fresh.getClientID());
            Topic topic = fresh.createTopic("news");
            fresh.createDurableConsumer(topic, "inbox").close();
            fresh.unsubscribe("inbox");
        }
        context.createConsumer(queue);
        assertThrows(IllegalStateRuntimeException.class, () -> context.setClientID("late"));
    }

    @Test
    void contextCanBeCreatedWithCredentialsAndASessionMode() throws JMSException {
        try (JMSContext withCredentials = factory.createContext("user", "password");
                JMSContext withBoth = factory.createContext("user", "password", JMSContext.DUPS_OK_ACKNOWLEDGE)) {
            assertEquals(JMSContext.AUTO_ACKNOWLEDGE, withCredentials.getSessionMode());
            assertEquals(JMSContext.DUPS_OK_ACKNOWLEDGE, withBoth.getSessionMode());
            assertEquals("3.1", withBoth.getMetaData().getJMSVersion());
        }
        assertThrows(JMSRuntimeException.class, () -> factory.createContext(42));
        assertThrows(JMSRuntimeException.class, () -> context.createContext(42));
    }

    @Test
    void closingAContextDoesNotDeadlockWithAListenerThatUsesIt() throws Exception {
        JMSContext transacted = factory.createContext(JMSContext.SESSION_TRANSACTED);
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        LinkedBlockingQueue<Object> outcomes = new LinkedBlockingQueue<>();
        transacted.createConsumer(queue).setMessageListener(message -> {
            entered.countDown();
            try {
                release.await();
                transacted.commit();
                outcomes.add("committed");
            } catch (RuntimeException | InterruptedException e) {
                outcomes.add(e);
            }
        });
        context.createProducer().send(queue, "one");
        assertTrue(entered.await(SOON, TimeUnit.MILLISECONDS));

        java.util.concurrent.CompletableFuture<Void> closed =
                java.util.concurrent.CompletableFuture.runAsync(transacted::close);
        Thread.sleep(BRIEFLY);
        release.countDown();

        assertEquals("committed", outcomes.poll(SOON, TimeUnit.MILLISECONDS),
                "the context stays available to the listener in progress while it is being closed");
        closed.get(SOON, TimeUnit.MILLISECONDS);
    }

    @Test
    void closedContextCreatesNoOtherContext() {
        JMSContext other = factory.createContext();
        other.close();

        assertThrows(IllegalStateRuntimeException.class, () -> other.createContext(JMSContext.AUTO_ACKNOWLEDGE));
    }

    @Test
    void correlationIdGivenAsBytesIsSentAsBytes() throws JMSException {
        byte[] notUtf8 = {(byte) 0xff, 0x01, (byte) 0xfe};

        context.createProducer().setJMSCorrelationIDAsBytes(notUtf8).send(queue, "x");

        assertArrayEquals(notUtf8, context.createConsumer(queue).receive(SOON).getJMSCorrelationIDAsBytes());
    }

    // ---- consumers --------------------------------------------------------

    @Test
    void listenerIsSetOnTheConsumerAndMayNotCloseItsContext() throws Exception {
        LinkedBlockingQueue<Object> outcomes = new LinkedBlockingQueue<>();
        JMSConsumer consumer = context.createConsumer(queue);
        consumer.setMessageListener(message -> {
            try {
                context.close();
                outcomes.add("closed");
            } catch (RuntimeException e) {
                outcomes.add(e);
            }
        });

        context.createContext(JMSContext.AUTO_ACKNOWLEDGE).createProducer().send(queue, "one");

        assertInstanceOf(IllegalStateRuntimeException.class, outcomes.poll(SOON, TimeUnit.MILLISECONDS));
        consumer.close();
    }

    @Test
    void contextCreatesEveryKindOfConsumerAndABrowser() throws JMSException {
        Topic topic = context.createTopic("news");
        JMSConsumer shared = context.createSharedConsumer(topic, "workers");
        JMSConsumer selective = context.createConsumer(queue, "color = 'blue'");
        context.createProducer().send(topic, "headline");
        context.createProducer().setProperty("color", "blue").send(queue, "blue one");

        assertEquals("headline", shared.receiveBody(String.class, SOON));
        assertEquals("color = 'blue'", selective.getMessageSelector());
        try (var browser = context.createBrowser(queue)) {
            assertEquals(1, Collections.list(browser.getEnumeration()).size());
        }
        assertEquals("blue one", selective.receiveBody(String.class, SOON));
    }

    // ---- unchecked exceptions (§10.2) ----------------------------------------

    @Test
    void failuresAreReportedAsTheUncheckedCounterpartOfTheCheckedException() {
        assertThrows(InvalidSelectorRuntimeException.class, () -> context.createConsumer(queue, "not a selector!"));
        assertThrows(InvalidDestinationRuntimeException.class, () -> context.createConsumer(null));
        assertThrows(InvalidDestinationRuntimeException.class, () -> context.createProducer().send(null, "x"));
        assertThrows(MessageFormatRuntimeException.class, () -> context.createProducer().send(queue, (Message) null));
        assertThrows(IllegalStateRuntimeException.class, context::rollback);

        context.close();
        assertThrows(IllegalStateRuntimeException.class, () -> context.createTextMessage("x"));
        context.close();
    }
}
