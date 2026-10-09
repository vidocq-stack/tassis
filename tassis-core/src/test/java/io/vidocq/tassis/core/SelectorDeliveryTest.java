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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.Connection;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What a selector does to delivery (Jakarta Messaging 3.1 §4.1.2 for queues,
 * §4.2.2 and §8.3 for topics).
 */
class SelectorDeliveryTest {

    private static final long SOON = 5_000;
    private static final long BRIEFLY = 100;

    private Connection connection;
    private Session session;
    private Queue queue;
    private Topic topic;

    @BeforeEach
    void connect() throws JMSException {
        connection = new TassisConnectionFactory("test-" + UUID.randomUUID()).withClientID("reader").createConnection();
        session = connection.createSession();
        queue = session.createQueue("orders");
        topic = session.createTopic("news");
        connection.start();
    }

    @AfterEach
    void disconnect() throws JMSException {
        connection.close();
    }

    private void send(jakarta.jms.Destination destination, String text, String color) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage(text);
        message.setStringProperty("color", color);
        producer.send(message);
        producer.close();
    }

    private static String text(Message message) throws JMSException {
        return message == null ? null : ((TextMessage) message).getText();
    }

    @Test
    void queueMessageTheSelectorRejectsStaysForAnotherConsumer() throws JMSException {
        MessageConsumer blue = session.createConsumer(queue, "color = 'blue'");
        send(queue, "red one", "red");
        send(queue, "blue one", "blue");

        assertEquals("blue one", text(blue.receive(SOON)), "the selector skips the message ahead of it");
        assertNull(blue.receiveNoWait());
        assertEquals("color = 'blue'", blue.getMessageSelector());
        assertEquals("red one", text(session.createConsumer(queue).receive(SOON)));
    }

    @Test
    void emptySelectorIsNoSelector() throws JMSException {
        MessageConsumer all = session.createConsumer(queue, "");
        send(queue, "any", "red");

        assertEquals("any", text(all.receive(SOON)));
        assertNull(all.getMessageSelector());
    }

    @Test
    void browserShowsTheMessagesItsSelectorMatches() throws JMSException {
        send(queue, "red one", "red");
        send(queue, "blue one", "blue");

        try (QueueBrowser browser = session.createBrowser(queue, "color = 'blue'")) {
            assertEquals("color = 'blue'", browser.getMessageSelector());
            assertEquals(1, Collections.list(browser.getEnumeration()).size());
        }
    }

    @Test
    void topicMessageTheSelectorRejectsNeverEntersTheSubscription() throws JMSException {
        MessageConsumer blue = session.createConsumer(topic, "color = 'blue'");
        send(topic, "red one", "red");
        send(topic, "blue one", "blue");

        assertEquals("blue one", text(blue.receive(SOON)));
        assertNull(blue.receive(BRIEFLY));
    }

    @Test
    void durableSubscriptionWithAnotherSelectorIsANewSubscription() throws JMSException {
        session.createDurableConsumer(topic, "inbox", "color = 'blue'", false).close();
        send(topic, "blue one", "blue");

        MessageConsumer red = session.createDurableConsumer(topic, "inbox", "color = 'red'", false);

        assertNull(red.receive(BRIEFLY), "changing the selector deleted the old subscription and its messages");
        send(topic, "red one", "red");
        assertEquals("red one", text(red.receive(SOON)));
    }

    @Test
    void durableSubscriptionWithTheSameSelectorIsReused() throws JMSException {
        session.createDurableConsumer(topic, "inbox", "color = 'blue'", false).close();
        send(topic, "blue one", "blue");

        assertEquals("blue one",
                text(session.createDurableConsumer(topic, "inbox", "color = 'blue'", false).receive(SOON)));
    }

    @Test
    void sharedSubscriptionCannotChangeSelectorWhileItHasConsumers() throws JMSException {
        session.createSharedConsumer(topic, "workers", "color = 'blue'");

        assertThrows(JMSException.class, () -> session.createSharedConsumer(topic, "workers", "color = 'red'"));
    }

    @Test
    void invalidSelectorIsRejectedWhereverOneIsGiven() {
        String invalid = "definitely not a message selector!";

        assertThrows(InvalidSelectorException.class, () -> session.createConsumer(queue, invalid));
        assertThrows(InvalidSelectorException.class, () -> session.createConsumer(topic, invalid, true));
        assertThrows(InvalidSelectorException.class, () -> session.createBrowser(queue, invalid));
        assertThrows(InvalidSelectorException.class,
                () -> session.createDurableConsumer(topic, "inbox", invalid, false));
        assertThrows(InvalidSelectorException.class, () -> session.createSharedConsumer(topic, "workers", invalid));
        assertThrows(InvalidSelectorException.class,
                () -> session.createSharedDurableConsumer(topic, "archive", invalid));
    }
}
