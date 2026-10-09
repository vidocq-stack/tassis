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

import io.vidocq.tassis.core.internal.broker.Broker;
import io.vidocq.tassis.core.internal.selector.Selector;
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

    private String brokerName;
    private Connection connection;
    private Session session;
    private Queue queue;
    private Topic topic;

    @BeforeEach
    void connect() throws JMSException {
        brokerName = "test-" + UUID.randomUUID();
        connection = new TassisConnectionFactory(brokerName).withClientID("reader").createConnection();
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
    void queueSelectorSeesTheDeliveryCountOfTheDeliveryToCome() throws JMSException {
        send(queue, "retried", "red");
        try (Session transacted = connection.createSession(Session.SESSION_TRANSACTED)) {
            MessageConsumer firstAttempts = transacted.createConsumer(queue, "JMSXDeliveryCount = 1");
            assertEquals("retried", text(firstAttempts.receive(SOON)));
            transacted.rollback();

            assertNull(firstAttempts.receive(BRIEFLY), "its next delivery would be the second");
        }

        Message retried = session.createConsumer(queue, "JMSXDeliveryCount >= 2").receive(SOON);
        assertEquals("retried", text(retried));
        assertEquals(2, retried.getIntProperty("JMSXDeliveryCount"));
    }

    @Test
    void subscriptionSelectorOnTheDeliveryCountIsCheckedAgainOnRedelivery() throws JMSException {
        try (Session transacted = connection.createSession(Session.SESSION_TRANSACTED)) {
            MessageConsumer firstAttempts = transacted.createConsumer(topic, "JMSXDeliveryCount = 1");
            send(topic, "once only", "red");
            assertEquals("once only", text(firstAttempts.receive(SOON)));

            transacted.rollback();

            assertNull(firstAttempts.receive(BRIEFLY), "a second delivery no longer matches the subscription");
        }
    }

    @Test
    void browserSelectorSeesTheDeliveryCountToo() throws JMSException {
        send(queue, "fresh", "red");

        try (QueueBrowser first = session.createBrowser(queue, "JMSXDeliveryCount = 1");
                QueueBrowser later = session.createBrowser(queue, "JMSXDeliveryCount > 1")) {
            assertEquals(1, Collections.list(first.getEnumeration()).size());
            assertEquals(0, Collections.list(later.getEnumeration()).size());
        }
    }

    @Test
    void queueMessageIsEvaluatedOncePerSelector() throws JMSException {
        for (int i = 0; i < 50; i++) {
            send(queue, "red " + i, "red");
        }
        Selector.resetEvaluationCount();
        MessageConsumer blue = session.createConsumer(queue, "color = 'blue'");
        MessageConsumer alsoBlue = session.createConsumer(queue, "color = 'blue'");

        for (int i = 0; i < 5; i++) {
            assertNull(blue.receiveNoWait());
            assertNull(alsoBlue.receiveNoWait());
        }
        assertEquals(50, Selector.evaluationCount(),
                "the 50 messages are evaluated once for the selector, however many consumers and receives");

        send(queue, "blue one", "blue");
        assertEquals("blue one", text(blue.receive(SOON)));
        assertEquals(51, Selector.evaluationCount(), "a new message is evaluated once when it arrives");
        assertNull(alsoBlue.receiveNoWait());
        assertEquals(51, Selector.evaluationCount());
    }

    @Test
    void expiredMessageIsRemovedEvenWhenNoSelectorInUseAcceptsIt() throws Exception {
        MessageProducer producer = session.createProducer(queue);
        TextMessage shortLived = session.createTextMessage("red and short-lived");
        shortLived.setStringProperty("color", "red");
        producer.send(shortLived, jakarta.jms.DeliveryMode.PERSISTENT, 4, 1);
        MessageConsumer blue = session.createConsumer(queue, "color = 'blue'");
        Thread.sleep(50);

        assertNull(blue.receiveNoWait());

        assertEquals(0, Broker.named(brokerName).messageCount("orders"),
                "a consumer that only looks at its view must still let expired messages go");
    }

    @Test
    void indexedSelectorKeepsPriorityOrderAndRedelivery() throws JMSException {
        try (Session transacted = connection.createSession(Session.SESSION_TRANSACTED)) {
            MessageConsumer blue = transacted.createConsumer(queue, "color = 'blue'");
            MessageProducer producer = session.createProducer(queue);
            for (int priority : new int[] {1, 9, 5}) {
                TextMessage message = session.createTextMessage("p" + priority);
                message.setStringProperty("color", "blue");
                producer.send(message, jakarta.jms.DeliveryMode.PERSISTENT, priority, 0);
            }
            send(queue, "red one", "red");

            assertEquals("p9", text(blue.receive(SOON)));
            assertEquals("p5", text(blue.receive(SOON)));
            transacted.rollback();

            assertEquals("p9", text(blue.receive(SOON)), "messages given back return to their place in the view");
            assertEquals("p5", text(blue.receive(SOON)));
            assertEquals("p1", text(blue.receive(SOON)));
            assertNull(blue.receiveNoWait());
            transacted.commit();
        }
        assertEquals("red one", text(session.createConsumer(queue).receive(SOON)));
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
