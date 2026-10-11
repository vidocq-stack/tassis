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
package io.vidocq.tassis.interop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.TassisBroker;
import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A third-party Jakarta Messaging client, Apache Qpid JMS, against a Tassis broker opened to
 * the network by {@code TassisServer}: nothing of Tassis is on the client's side. This is what
 * AMQP 1.0 was chosen for. Queues only for now (ROADMAP M11.3b).
 */
class QpidJmsAgainstTassisTest {

    private static final long SOON = 5_000;

    private String brokerName;
    private TassisServer server;
    private ConnectionFactory qpid;
    private final List<Connection> opened = new ArrayList<>();

    @BeforeEach
    void startServer() {
        brokerName = "qpid-" + UUID.randomUUID();
        server = TassisServer.forBroker(brokerName).port(0).start();
        qpid = new JmsConnectionFactory("amqp://127.0.0.1:" + server.port());
    }

    @AfterEach
    void stopServer() throws JMSException {
        for (Connection connection : opened) {
            connection.close();
        }
        server.close();
    }

    private Connection connect() throws JMSException {
        Connection connection = qpid.createConnection();
        opened.add(connection);
        connection.start();
        return connection;
    }

    // ---- send and receive (Jakarta Messaging 3.1 §4.1, §8.5) -------------------

    @Test
    void aTextMessageGoesFromOneQpidClientToAnother() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("orders");

        session.createProducer(queue).send(session.createTextMessage("hello"));
        Message received = connect().createSession().createConsumer(queue).receive(SOON);

        assertEquals("hello", ((TextMessage) received).getText());
        assertEquals(queue.getQueueName(), ((Queue) received.getJMSDestination()).getQueueName());
        assertNotNull(received.getJMSMessageID());
        assertTrue(received.getJMSMessageID().startsWith("ID:"), received.getJMSMessageID());
        assertFalse(received.getJMSRedelivered());
    }

    @Test
    void everyKindOfMessageKeepsItsBody() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("bodies");
        MessageProducer producer = session.createProducer(queue);

        BytesMessage bytes = session.createBytesMessage();
        bytes.writeBytes(new byte[] {1, 2, 3});
        bytes.writeLong(7);
        producer.send(bytes);
        MapMessage map = session.createMapMessage();
        map.setString("s", "text");
        map.setInt("i", 12);
        map.setBoolean("b", true);
        map.setBytes("bytes", new byte[] {9});
        map.setChar("c", 'x');
        producer.send(map);
        StreamMessage stream = session.createStreamMessage();
        stream.writeString("a");
        stream.writeDouble(1.5);
        stream.writeBytes(new byte[] {4, 5});
        producer.send(stream);
        producer.send(session.createObjectMessage(new ArrayList<>(List.of("in", "a", "list"))));
        producer.send(session.createMessage());
        producer.send(session.createTextMessage(null));

        MessageConsumer consumer = session.createConsumer(queue);
        BytesMessage readBytes = (BytesMessage) consumer.receive(SOON);
        assertEquals(11, readBytes.getBodyLength());
        byte[] three = new byte[3];
        readBytes.readBytes(three);
        assertArrayEquals(new byte[] {1, 2, 3}, three);
        assertEquals(7, readBytes.readLong());
        MapMessage readMap = (MapMessage) consumer.receive(SOON);
        assertEquals("text", readMap.getString("s"));
        assertEquals(12, readMap.getObject("i"));
        assertEquals(true, readMap.getObject("b"));
        assertArrayEquals(new byte[] {9}, readMap.getBytes("bytes"));
        assertEquals('x', readMap.getChar("c"));
        StreamMessage readStream = (StreamMessage) consumer.receive(SOON);
        assertEquals("a", readStream.readObject());
        assertEquals(1.5, readStream.readObject());
        assertArrayEquals(new byte[] {4, 5}, (byte[]) readStream.readObject());
        ObjectMessage readObject = (ObjectMessage) consumer.receive(SOON);
        assertEquals(List.of("in", "a", "list"), readObject.getObject());
        Message plain = consumer.receive(SOON);
        assertFalse(plain instanceof TextMessage || plain instanceof BytesMessage);
        assertNull(((TextMessage) consumer.receive(SOON)).getText());
    }

    @Test
    void headerFieldsAndPropertiesTravel() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("headers");
        Queue replies = session.createQueue("replies");
        TextMessage message = session.createTextMessage("with everything");
        message.setJMSCorrelationID("request-9");
        message.setJMSReplyTo(replies);
        message.setJMSType("order");
        message.setStringProperty("color", "red");
        message.setIntProperty("weight", 12);
        message.setBooleanProperty("fragile", true);
        message.setDoubleProperty("ratio", 0.5);
        message.setLongProperty("big", 1L << 40);
        long before = System.currentTimeMillis();

        session.createProducer(queue).send(message, DeliveryMode.NON_PERSISTENT, 7, 60_000);
        Message received = session.createConsumer(queue).receive(SOON);

        assertEquals(message.getJMSMessageID(), received.getJMSMessageID());
        assertEquals("request-9", received.getJMSCorrelationID());
        assertEquals("replies", ((Queue) received.getJMSReplyTo()).getQueueName());
        assertEquals("order", received.getJMSType());
        assertEquals(DeliveryMode.NON_PERSISTENT, received.getJMSDeliveryMode());
        assertEquals(7, received.getJMSPriority());
        assertTrue(received.getJMSTimestamp() >= before, "timestamp");
        assertEquals(message.getJMSExpiration(), received.getJMSExpiration());
        assertTrue(received.getJMSExpiration() >= before + 60_000);
        assertEquals("red", received.getStringProperty("color"));
        assertEquals(12, received.getObjectProperty("weight"));
        assertEquals(true, received.getObjectProperty("fragile"));
        assertEquals(0.5, received.getObjectProperty("ratio"));
        assertEquals(1L << 40, received.getObjectProperty("big"));
        assertEquals(1, received.getIntProperty("JMSXDeliveryCount"));
    }

    @Test
    void messagesAreDeliveredByPriorityThenInOrder() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("priorities");
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("low 1"), DeliveryMode.PERSISTENT, 1, 0);
        producer.send(session.createTextMessage("high"), DeliveryMode.PERSISTENT, 9, 0);
        producer.send(session.createTextMessage("low 2"), DeliveryMode.PERSISTENT, 1, 0);

        MessageConsumer consumer = session.createConsumer(queue);

        assertEquals("high", ((TextMessage) consumer.receive(SOON)).getText());
        assertEquals("low 1", ((TextMessage) consumer.receive(SOON)).getText());
        assertEquals("low 2", ((TextMessage) consumer.receive(SOON)).getText());
    }

    @Test
    void anExpiredMessageIsNotDelivered() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("expiring");
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("short-lived"), DeliveryMode.PERSISTENT, 4, 50);
        producer.send(session.createTextMessage("lasting"));
        Thread.sleep(200);

        assertEquals("lasting", ((TextMessage) session.createConsumer(queue).receive(SOON)).getText());
    }

    @Test
    void receiveNoWaitAndReceiveWithATimeoutSeeWhatIsThere() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("timing");
        MessageConsumer consumer = session.createConsumer(queue);

        assertNull(consumer.receiveNoWait());
        assertNull(consumer.receive(200));

        session.createProducer(queue).send(session.createTextMessage("now there"));
        Message received = consumer.receive(SOON);
        assertEquals("now there", ((TextMessage) received).getText());
        assertNull(consumer.receiveNoWait());
    }

    @Test
    void aListenerIsCalledForEachMessage() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("listened");
        LinkedBlockingQueue<String> heard = new LinkedBlockingQueue<>();
        session.createConsumer(queue).setMessageListener(message -> {
            try {
                heard.add(((TextMessage) message).getText());
            } catch (JMSException e) {
                heard.add("failed: " + e);
            }
        });

        Session other = connect().createSession();
        MessageProducer producer = other.createProducer(other.createQueue("listened"));
        for (int i = 0; i < 20; i++) {
            producer.send(other.createTextMessage("m" + i));
        }

        for (int i = 0; i < 20; i++) {
            assertEquals("m" + i, heard.poll(SOON, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void aProducerWithoutADestinationSaysItAtEachSend() throws Exception {
        Session session = connect().createSession();
        MessageProducer anonymous = session.createProducer(null);

        anonymous.send(session.createQueue("first"), session.createTextMessage("1"));
        anonymous.send(session.createQueue("second"), session.createTextMessage("2"));

        assertEquals("2", ((TextMessage) session.createConsumer(session.createQueue("second")).receive(SOON)).getText());
        assertEquals("1", ((TextMessage) session.createConsumer(session.createQueue("first")).receive(SOON)).getText());
    }

    @Test
    void aLargeMessageCrossesInSeveralFrames() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("large");
        String large = "0123456789".repeat(150_000);

        session.createProducer(queue).send(session.createTextMessage(large));

        assertEquals(large, ((TextMessage) session.createConsumer(queue).receive(SOON)).getText());
    }

    // ---- acknowledgement (§6.2.10, §6.2.11) --------------------------------------

    @Test
    void aClientAcknowledgedMessageIsRedeliveredUntilAcknowledged() throws Exception {
        Connection connection = connect();
        Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("acknowledged");
        session.createProducer(queue).send(session.createTextMessage("important"));
        MessageConsumer consumer = session.createConsumer(queue);

        Message first = consumer.receive(SOON);
        assertFalse(first.getJMSRedelivered());
        session.recover();
        Message again = consumer.receive(SOON);
        assertEquals("important", ((TextMessage) again).getText());
        assertTrue(again.getJMSRedelivered());
        again.acknowledge();
        session.close();

        try (JMSContext inVm = new TassisConnectionFactory(brokerName).createContext()) {
            assertNull(inVm.createConsumer(inVm.createQueue("acknowledged")).receiveNoWait());
        }
    }

    @Test
    void whatAClosedConnectionHadNotAcknowledgedIsDeliveredToAnother() throws Exception {
        Connection first = qpid.createConnection();
        first.start();
        Session session = first.createSession(Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("handed-over");
        session.createProducer(queue).send(session.createTextMessage("not acknowledged"));
        assertNotNull(session.createConsumer(queue).receive(SOON));

        first.close();

        Message again = connect().createSession().createConsumer(queue).receive(SOON);
        assertEquals("not acknowledged", ((TextMessage) again).getText());
        assertTrue(again.getJMSRedelivered());
        // At least once more: a client that prefetches may be sent the message again while it closes.
        assertTrue(again.getIntProperty("JMSXDeliveryCount") >= 2);
    }

    // ---- with a client of the JVM ---------------------------------------------------

    @Test
    void aQpidClientAndATassisClientOfTheJvmExchangeMessages() throws Exception {
        Session session = connect().createSession();
        try (JMSContext inVm = new TassisConnectionFactory(brokerName).createContext()) {
            session.createProducer(session.createQueue("to-the-jvm")).send(session.createTextMessage("from Qpid"));
            Message received = inVm.createConsumer(inVm.createQueue("to-the-jvm")).receive(SOON);
            assertEquals("from Qpid", received.getBody(String.class));

            MapMessage map = inVm.createMapMessage();
            map.setString("from", "Tassis");
            map.setLong("n", 42L);
            map.setStringProperty("color", "blue");
            inVm.createProducer().setPriority(8).send(inVm.createQueue("to-qpid"), map);
            MapMessage read = (MapMessage) session.createConsumer(session.createQueue("to-qpid")).receive(SOON);
            assertEquals("Tassis", read.getString("from"));
            assertEquals(42L, read.getLong("n"));
            assertEquals("blue", read.getStringProperty("color"));
            assertEquals(8, read.getJMSPriority());
            assertTrue(read.getJMSMessageID().startsWith("ID:"), read.getJMSMessageID());
        }
    }

    // ---- authentication (§6.1.1) and client identifiers (§6.1.2) ---------------------

    @Test
    void aSecuredBrokerAsksAQpidClientForItsCredentials() throws Exception {
        TassisBroker.named(brokerName).addUser("alice", "secret");

        Connection accepted = qpid.createConnection("alice", "secret");
        opened.add(accepted);
        accepted.start();
        accepted.createSession().close();

        assertThrows(JMSSecurityException.class, () -> {
            Connection refused = qpid.createConnection("alice", "wrong");
            opened.add(refused);
            refused.start();
        });
        assertThrows(JMSSecurityException.class, () -> {
            Connection refused = qpid.createConnection();
            opened.add(refused);
            refused.start();
        });
    }

    @Test
    void aClientIdentifierBelongsToOneConnection() throws Exception {
        Connection first = qpid.createConnection();
        opened.add(first);
        first.setClientID("desk");
        first.start();

        assertThrows(InvalidClientIDException.class, () -> {
            Connection second = qpid.createConnection();
            opened.add(second);
            second.setClientID("desk");
        });
    }

    // ---- what is not served yet (ROADMAP M11.3c) -------------------------------------

    @Test
    void whatIsNotServedYetFailsWithAnExceptionAndLeavesTheConnectionUsable() throws Exception {
        Connection connection = connect();
        Session session = connection.createSession();
        Queue queue = session.createQueue("orders");

        assertThrows(JMSException.class, () -> session.createConsumer(session.createTopic("news")));
        assertThrows(JMSException.class, () -> session.createConsumer(queue, "color = 'red'"));
        assertThrows(JMSException.class, () -> session.createTemporaryQueue());
        assertThrows(JMSException.class, () -> {
            Session transacted = connection.createSession(Session.SESSION_TRANSACTED);
            transacted.createProducer(queue).send(transacted.createTextMessage("in a transaction"));
            transacted.commit();
        });

        Session fine = connection.createSession();
        fine.createProducer(queue).send(fine.createTextMessage("still works"));
        assertEquals("still works", ((TextMessage) fine.createConsumer(queue).receive(SOON)).getText());
    }
}
