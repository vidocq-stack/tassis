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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Message acknowledgement (Jakarta Messaging 3.1 §6.2.10), redelivery (§6.2.11,
 * §3.4.7, §3.5.11) and local transactions (§6.2.7).
 */
class AcknowledgementTest {

    private static final long SOON = 5_000;

    private Connection connection;
    private Queue queue;

    @BeforeEach
    void connect() throws JMSException {
        connection = new TassisConnectionFactory("test-" + UUID.randomUUID()).createConnection();
        connection.start();
        try (Session session = connection.createSession()) {
            queue = session.createQueue("orders");
        }
    }

    @AfterEach
    void disconnect() throws JMSException {
        connection.close();
    }

    private void send(String... texts) throws JMSException {
        try (Session session = connection.createSession()) {
            MessageProducer producer = session.createProducer(queue);
            for (String text : texts) {
                producer.send(session.createTextMessage(text));
            }
        }
    }

    private static String text(Message message) throws JMSException {
        return message == null ? null : ((TextMessage) message).getText();
    }

    /** What is left on the queue, read through a fresh auto-acknowledged session. */
    private String next() throws JMSException {
        try (Session session = connection.createSession()) {
            return text(session.createConsumer(queue).receive(200));
        }
    }

    @Test
    void firstDeliveryIsNotMarkedRedelivered() throws JMSException {
        send("one");
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE)) {
            Message message = session.createConsumer(queue).receive(SOON);

            assertFalse(message.getJMSRedelivered());
            assertEquals(1, message.getIntProperty("JMSXDeliveryCount"));
            assertTrue(Collections.list(message.getPropertyNames()).contains("JMSXDeliveryCount"));
            message.acknowledge();
        }
    }

    @Test
    void recoverRedeliversWhatWasNotAcknowledged() throws JMSException {
        send("one", "two");
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE)) {
            MessageConsumer consumer = session.createConsumer(queue);
            assertEquals("one", text(consumer.receive(SOON)));

            session.recover();
            Message again = consumer.receive(SOON);

            assertEquals("one", text(again), "redelivery restarts with the oldest unacknowledged message");
            assertTrue(again.getJMSRedelivered());
            assertEquals(2, again.getIntProperty("JMSXDeliveryCount"));
            assertEquals("two", text(consumer.receive(SOON)));
            again.acknowledge();
        }
    }

    @Test
    void acknowledgeCoversEveryMessageTheSessionHasConsumed() throws JMSException {
        send("one", "two");
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE)) {
            MessageConsumer consumer = session.createConsumer(queue);
            consumer.receive(SOON);
            Message second = consumer.receive(SOON);

            second.acknowledge();
            session.recover();

            assertNull(consumer.receiveNoWait(), "acknowledging the second message acknowledged the first too");
        }
    }

    @Test
    void closingAClientAcknowledgedSessionReturnsItsUnacknowledgedMessages() throws JMSException {
        send("one");
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE)) {
            assertNotNull(session.createConsumer(queue).receive(SOON));
        }

        try (Session other = connection.createSession()) {
            Message again = other.createConsumer(queue).receive(SOON);
            assertEquals("one", text(again));
            assertTrue(again.getJMSRedelivered());
        }
    }

    @Test
    void acknowledgeFailsOnceTheSessionIsClosed() throws JMSException {
        send("one");
        Message message;
        try (Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE)) {
            message = session.createConsumer(queue).receive(SOON);
        }

        assertThrows(jakarta.jms.IllegalStateException.class, message::acknowledge);
    }

    @Test
    void automaticallyAcknowledgedMessageIsNeverRedelivered() throws JMSException {
        send("one");
        try (Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(SOON);

            message.acknowledge();
            session.recover();

            assertNull(consumer.receiveNoWait());
        }
        assertNull(next());
    }

    @Test
    void transactedSendReachesTheQueueOnCommitOnly() throws JMSException {
        try (Session session = connection.createSession(Session.SESSION_TRANSACTED)) {
            assertTrue(session.getTransacted());
            assertEquals(Session.SESSION_TRANSACTED, session.getAcknowledgeMode());
            session.createProducer(queue).send(session.createTextMessage("one"));

            assertNull(next(), "nothing is sent before the commit");

            session.commit();
            assertEquals("one", next());
        }
    }

    @Test
    void rollbackDiscardsTheSendsOfTheTransaction() throws JMSException {
        try (Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer(queue).send(session.createTextMessage("one"));

            session.rollback();
            session.commit();
        }
        assertNull(next());
    }

    @Test
    void rollbackRedeliversTheMessagesOfTheTransaction() throws JMSException {
        send("one");
        try (Session session = connection.createSession(Session.SESSION_TRANSACTED)) {
            MessageConsumer consumer = session.createConsumer(queue);
            assertEquals("one", text(consumer.receive(SOON)));

            session.rollback();
            Message again = consumer.receive(SOON);

            assertEquals("one", text(again));
            assertTrue(again.getJMSRedelivered());
            assertEquals(2, again.getIntProperty("JMSXDeliveryCount"));
            session.commit();
        }
        assertNull(next(), "the commit consumed the message for good");
    }

    @Test
    void closingATransactedSessionRollsItsTransactionBack() throws JMSException {
        send("one");
        try (Session session = connection.createSession(Session.SESSION_TRANSACTED)) {
            assertNotNull(session.createConsumer(queue).receive(SOON));
            session.createProducer(queue).send(session.createTextMessage("never sent"));
        }

        assertEquals("one", next());
        assertNull(next());
    }

    @Test
    void redeliveredMessageKeepsItsPlaceInTheQueue() throws JMSException {
        send("one", "two");
        try (Session session = connection.createSession(Session.SESSION_TRANSACTED)) {
            assertEquals("one", text(session.createConsumer(queue).receive(SOON)));
            session.rollback();
        }

        assertEquals("one", next());
        assertEquals("two", next());
    }

    @Test
    void sessionModesAreChecked() throws JMSException {
        try (Session clientAcknowledged = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                Session transacted = connection.createSession(Session.SESSION_TRANSACTED)) {
            assertEquals(Session.CLIENT_ACKNOWLEDGE, clientAcknowledged.getAcknowledgeMode());
            assertThrows(jakarta.jms.IllegalStateException.class, transacted::recover,
                    "recover is for non-transacted sessions");
            assertThrows(jakarta.jms.IllegalStateException.class, clientAcknowledged::commit);
        }
        assertThrows(JMSException.class, () -> connection.createSession(false, Session.SESSION_TRANSACTED));
        assertThrows(JMSException.class, () -> connection.createSession(42));
    }

    @Test
    void metaDataListsTheSupportedJmsxProperties() throws JMSException {
        assertTrue(Collections.list(connection.getMetaData().getJMSXPropertyNames()).contains("JMSXDeliveryCount"));
    }
}
