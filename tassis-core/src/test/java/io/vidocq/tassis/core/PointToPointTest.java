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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.internal.message.ForeignTextMessage;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Point-to-point messaging through the classic API on an in-VM queue (Jakarta
 * Messaging 3.1 §4.1, §6.1, §7 and §8). Each test runs against its own broker.
 */
class PointToPointTest {

    /** Upper bound for a receive that is expected to succeed. */
    private static final long SOON = 5_000;

    private TassisConnectionFactory factory;
    private Connection connection;
    private Session session;
    private Queue queue;
    private MessageProducer producer;
    private MessageConsumer consumer;

    @BeforeEach
    void connect() throws JMSException {
        factory = new TassisConnectionFactory("test-" + UUID.randomUUID());
        connection = factory.createConnection();
        session = connection.createSession();
        queue = session.createQueue("orders");
        producer = session.createProducer(queue);
        consumer = session.createConsumer(queue);
        connection.start();
    }

    @AfterEach
    void disconnect() throws JMSException {
        connection.close();
    }

    private String receiveText(long timeout) throws JMSException {
        Message message = consumer.receive(timeout);
        return message == null ? null : ((TextMessage) message).getText();
    }

    @Test
    void sentMessageIsReceived() throws JMSException {
        producer.send(session.createTextMessage("hello"));

        assertEquals("hello", receiveText(SOON));
        assertNull(consumer.receiveNoWait(), "a queue message is delivered once");
    }

    @Test
    void sendSetsTheProviderHeadersOnBothCopies() throws JMSException {
        TextMessage sent = session.createTextMessage("hello");
        long before = System.currentTimeMillis();

        producer.send(sent);
        Message received = consumer.receive(SOON);

        for (Message message : new Message[] {sent, received}) {
            assertEquals(queue, message.getJMSDestination());
            assertTrue(message.getJMSMessageID().startsWith("ID:"), "§3.4.3: message IDs start with ID:");
            assertTrue(message.getJMSTimestamp() >= before);
            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
            assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());
            assertEquals(0, message.getJMSExpiration(), "no time to live means no expiration");
        }
        assertEquals(sent.getJMSMessageID(), received.getJMSMessageID());
    }

    @Test
    void receivedMessageIsAReadOnlyCopyOfWhatWasSent() throws JMSException {
        TextMessage sent = session.createTextMessage("hello");
        sent.setIntProperty("attempt", 1);
        producer.send(sent);

        sent.setText("changed after send");
        TextMessage received = (TextMessage) consumer.receive(SOON);

        assertNotSame(sent, received);
        assertEquals("hello", received.getText());
        assertEquals(1, received.getIntProperty("attempt"));
        assertThrows(MessageNotWriteableException.class, () -> received.setText("no"));
        assertThrows(MessageNotWriteableException.class, () -> received.setIntProperty("attempt", 2));
    }

    @Test
    void everyBodyTypeSurvivesTheQueue() throws JMSException {
        BytesMessage bytes = session.createBytesMessage();
        bytes.writeInt(3);
        MapMessage map = session.createMapMessage();
        map.setString("key", "value");
        StreamMessage stream = session.createStreamMessage();
        stream.writeLong(4L);
        producer.send(bytes);
        producer.send(map);
        producer.send(stream);
        producer.send(session.createObjectMessage("payload"));
        producer.send(session.createMessage());

        assertEquals(3, ((BytesMessage) consumer.receive(SOON)).readInt());
        assertEquals("value", ((MapMessage) consumer.receive(SOON)).getString("key"));
        assertEquals(4L, ((StreamMessage) consumer.receive(SOON)).readLong());
        assertEquals("payload", ((ObjectMessage) consumer.receive(SOON)).getObject());
        assertNull(consumer.receive(SOON).getBody(Object.class));
    }

    @Test
    void foreignMessageIsSentAndGetsTheProviderHeaders() throws JMSException {
        ForeignTextMessage foreign = new ForeignTextMessage();
        foreign.setText("from elsewhere");

        producer.send(foreign);

        assertEquals("from elsewhere", receiveText(SOON));
        assertEquals(queue, foreign.getJMSDestination(), "§3.12: the headers are set on the foreign message too");
        assertTrue(foreign.getJMSMessageID().startsWith("ID:"));
    }

    @Test
    void messagesOfEqualPriorityAreReceivedInSendOrder() throws JMSException {
        for (String text : new String[] {"one", "two", "three"}) {
            producer.send(session.createTextMessage(text));
        }

        assertEquals("one", receiveText(SOON));
        assertEquals("two", receiveText(SOON));
        assertEquals("three", receiveText(SOON));
    }

    @Test
    void higherPriorityIsReceivedFirst() throws JMSException {
        producer.send(session.createTextMessage("low"), DeliveryMode.PERSISTENT, 1, 0);
        producer.send(session.createTextMessage("high"), DeliveryMode.PERSISTENT, 9, 0);

        assertEquals("high", receiveText(SOON));
        assertEquals("low", receiveText(SOON));
    }

    @Test
    void expiredMessageIsNotDelivered() throws Exception {
        producer.setTimeToLive(1);
        TextMessage sent = session.createTextMessage("short-lived");
        producer.send(sent);

        Thread.sleep(50);

        assertTrue(sent.getJMSExpiration() > 0);
        assertNull(consumer.receiveNoWait());
    }

    @Test
    void messageIsNotDeliveredBeforeItsDeliveryTime() throws JMSException {
        producer.setDeliveryDelay(300);
        TextMessage sent = session.createTextMessage("later");
        producer.send(sent);

        assertNull(consumer.receiveNoWait(), "§7.9: not before the delivery time");
        Message received = consumer.receive(SOON);

        assertNotNull(received);
        assertTrue(System.currentTimeMillis() >= received.getJMSDeliveryTime());
        assertEquals(sent.getJMSTimestamp() + 300, sent.getJMSDeliveryTime());
    }

    @Test
    void receiveTimesOutOnAnEmptyQueue() throws JMSException {
        assertNull(consumer.receive(50));
        assertNull(consumer.receiveNoWait());
    }

    @Test
    void nothingIsDeliveredWhileTheConnectionIsStopped() throws JMSException {
        connection.stop();
        producer.send(session.createTextMessage("held"));

        assertNull(consumer.receive(100), "§6.1.5: receive timers keep running while stopped");

        connection.start();
        assertEquals("held", receiveText(SOON));
    }

    @Test
    void blockedReceiveGetsAMessageSentLater() throws Exception {
        CompletableFuture<String> received = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                received.complete(receiveText(0));
            } catch (JMSException e) {
                received.completeExceptionally(e);
            }
        });

        try (Session other = connection.createSession()) {
            other.createProducer(queue).send(other.createTextMessage("late"));
        }

        assertEquals("late", received.get(SOON, TimeUnit.MILLISECONDS));
    }

    @Test
    void blockedReceiveReturnsNullWhenTheConsumerIsClosed() throws Exception {
        CompletableFuture<Message> received = new CompletableFuture<>();
        Thread receiver = Thread.startVirtualThread(() -> {
            try {
                received.complete(consumer.receive());
            } catch (JMSException e) {
                received.completeExceptionally(e);
            }
        });
        while (receiver.getState() == Thread.State.RUNNABLE || receiver.getState() == Thread.State.NEW) {
            Thread.onSpinWait();
        }

        consumer.close();

        assertNull(received.get(SOON, TimeUnit.MILLISECONDS), "§8.8: a blocked receive returns null on close");
    }

    @Test
    void aMessageGoesToOneConsumerOnly() throws JMSException {
        MessageConsumer second = session.createConsumer(queue);
        producer.send(session.createTextMessage("only once"));

        Message first = consumer.receive(SOON);

        assertNotNull(first);
        assertNull(second.receiveNoWait());
    }

    @Test
    void unidentifiedProducerNamesTheDestinationOnEachSend() throws JMSException {
        MessageProducer anonymous = session.createProducer(null);

        anonymous.send(queue, session.createTextMessage("addressed"));

        assertEquals("addressed", receiveText(SOON));
        assertThrows(UnsupportedOperationException.class, () -> anonymous.send(session.createTextMessage("x")));
        assertThrows(UnsupportedOperationException.class,
                () -> producer.send(queue, session.createTextMessage("x")),
                "a producer bound to a destination must not be given another one");
    }

    @Test
    void sendRejectsANullMessageAndAnUnknownDestination() throws JMSException {
        MessageProducer anonymous = session.createProducer(null);
        Queue foreign = () -> "foreign";

        assertThrows(MessageFormatException.class, () -> producer.send(null));
        assertThrows(InvalidDestinationException.class,
                () -> anonymous.send(null, session.createTextMessage("x")));
        assertThrows(InvalidDestinationException.class,
                () -> anonymous.send(foreign, session.createTextMessage("x")));
        assertThrows(InvalidDestinationException.class, () -> session.createConsumer(null));
    }

    @Test
    void closedObjectsRejectFurtherUse() throws JMSException {
        TextMessage message = session.createTextMessage("kept");
        producer.close();
        consumer.close();

        assertThrows(jakarta.jms.IllegalStateException.class, () -> producer.send(message));
        assertThrows(jakarta.jms.IllegalStateException.class, () -> consumer.receiveNoWait());

        session.close();
        assertThrows(jakarta.jms.IllegalStateException.class, () -> session.createProducer(queue));
        assertThrows(jakarta.jms.IllegalStateException.class, () -> session.createTextMessage());

        connection.close();
        assertThrows(jakarta.jms.IllegalStateException.class, () -> connection.createSession());
        assertThrows(jakarta.jms.IllegalStateException.class, () -> connection.start());

        connection.close();
        session.close();
        consumer.close();
        producer.close();
        assertEquals("kept", message.getText(), "a message outlives the session that created it");
    }

    @Test
    void closingTheConnectionClosesItsSessionsAndConsumers() throws JMSException {
        connection.close();

        assertThrows(jakarta.jms.IllegalStateException.class, () -> session.createTextMessage());
        assertThrows(jakarta.jms.IllegalStateException.class, () -> consumer.receiveNoWait());
        assertThrows(jakarta.jms.IllegalStateException.class,
                () -> producer.send(new io.vidocq.tassis.core.internal.message.TassisTextMessage()));
    }

    @Test
    void connectionsOfOneFactoryShareTheBroker() throws JMSException {
        try (Connection other = factory.createConnection("user", "password");
                Session otherSession = other.createSession()) {
            otherSession.createProducer(queue).send(otherSession.createTextMessage("from elsewhere"));
        }

        assertEquals("from elsewhere", receiveText(SOON));
    }

    @Test
    void sessionIsNonTransactedAutoAcknowledgeByDefault() throws JMSException {
        assertFalse(session.getTransacted());
        assertEquals(Session.AUTO_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertThrows(jakarta.jms.IllegalStateException.class, session::commit,
                "§6.2.7: commit is only for transacted sessions");
        assertThrows(jakarta.jms.IllegalStateException.class, session::rollback);
    }

    @Test
    void metaDataReportsTheSpecVersion() throws JMSException {
        assertEquals("3.1", connection.getMetaData().getJMSVersion());
        assertEquals(3, connection.getMetaData().getJMSMajorVersion());
        assertEquals(1, connection.getMetaData().getJMSMinorVersion());
        assertEquals("Tassis", connection.getMetaData().getJMSProviderName());
    }
}
