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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueReceiver;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Temporary queues (Jakarta Messaging 3.1 §6.2.2), queue browsers (§4.1.3) and
 * the legacy queue-specific API (§2.9).
 */
class QueueFacilitiesTest {

    private static final long SOON = 5_000;

    private TassisConnectionFactory factory;
    private Connection connection;
    private Session session;

    @BeforeEach
    void connect() throws JMSException {
        factory = new TassisConnectionFactory("test-" + UUID.randomUUID());
        connection = factory.createConnection();
        session = connection.createSession();
        connection.start();
    }

    @AfterEach
    void disconnect() throws JMSException {
        connection.close();
    }

    private static List<String> texts(QueueBrowser browser) throws JMSException {
        List<String> texts = new ArrayList<>();
        Enumeration<?> messages = browser.getEnumeration();
        while (messages.hasMoreElements()) {
            texts.add(((TextMessage) messages.nextElement()).getText());
        }
        return texts;
    }

    @Test
    void temporaryQueueCarriesAReply() throws JMSException {
        Queue requests = session.createQueue("requests");
        TemporaryQueue replies = session.createTemporaryQueue();
        TextMessage request = session.createTextMessage("ping");
        request.setJMSReplyTo(replies);
        session.createProducer(requests).send(request);

        Message received = session.createConsumer(requests).receive(SOON);
        session.createProducer(received.getJMSReplyTo()).send(session.createTextMessage("pong"));

        assertEquals("pong", ((TextMessage) session.createConsumer(replies).receive(SOON)).getText());
    }

    @Test
    void temporaryQueueCannotBeDeletedWhileItHasAConsumer() throws JMSException {
        TemporaryQueue temporary = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(temporary);

        assertThrows(JMSException.class, temporary::delete);

        consumer.close();
        temporary.delete();
        assertThrows(InvalidDestinationException.class, () -> session.createProducer(temporary));
    }

    @Test
    void temporaryQueueIsConsumedByItsOwnConnectionOnly() throws JMSException {
        TemporaryQueue temporary = session.createTemporaryQueue();

        try (Connection other = factory.createConnection(); Session otherSession = other.createSession()) {
            assertThrows(JMSException.class, () -> otherSession.createConsumer(temporary));
            otherSession.createProducer(temporary).send(otherSession.createTextMessage("anyone may send"));
        }

        assertEquals("anyone may send", ((TextMessage) session.createConsumer(temporary).receive(SOON)).getText());
    }

    @Test
    void temporaryQueueIsDeletedWithItsConnection() throws JMSException {
        TemporaryQueue temporary = session.createTemporaryQueue();
        connection.close();

        try (Connection other = factory.createConnection(); Session otherSession = other.createSession()) {
            assertThrows(InvalidDestinationException.class, () -> otherSession.createProducer(temporary));
        }
    }

    @Test
    void browserShowsTheQueueWithoutConsumingIt() throws JMSException {
        Queue queue = session.createQueue("orders");
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("one"));
        producer.send(session.createTextMessage("two"));

        try (QueueBrowser browser = session.createBrowser(queue)) {
            assertEquals(queue, browser.getQueue());
            assertNull(browser.getMessageSelector());
            assertEquals(List.of("one", "two"), texts(browser));
            assertEquals(List.of("one", "two"), texts(browser), "browsing removes nothing");
        }
        assertEquals("one", ((TextMessage) session.createConsumer(queue).receive(SOON)).getText());
    }

    @Test
    void browserDoesNotShowAMessageBeforeItsDeliveryTime() throws JMSException {
        Queue queue = session.createQueue("orders");
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryDelay(60_000);
        producer.send(session.createTextMessage("later"));

        try (QueueBrowser browser = session.createBrowser(queue)) {
            assertFalse(browser.getEnumeration().hasMoreElements());
        }
    }

    @Test
    void legacyQueueApiSendsAndReceives() throws JMSException {
        try (QueueConnection queueConnection = factory.createQueueConnection()) {
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = queueSession.createQueue("legacy");
            QueueSender sender = queueSession.createSender(queue);
            QueueReceiver receiver = queueSession.createReceiver(queue);
            queueConnection.start();

            sender.send(queueSession.createTextMessage("via QueueSender"));

            assertEquals(queue, sender.getQueue());
            assertEquals(queue, receiver.getQueue());
            assertEquals("via QueueSender", ((TextMessage) receiver.receive(SOON)).getText());
        }
    }

    @Test
    void queueDestinationsAreEqualByName() throws JMSException {
        assertEquals(session.createQueue("orders"), new TassisQueue("orders"));
        assertEquals("orders", session.createQueue("orders").getQueueName());
        assertTrue(session.createTemporaryQueue().getQueueName().length() > 0);
    }
}
