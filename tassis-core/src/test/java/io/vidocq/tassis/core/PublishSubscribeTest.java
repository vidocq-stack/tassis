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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicPublisher;
import jakarta.jms.TopicSession;
import jakarta.jms.TopicSubscriber;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Publish/subscribe (Jakarta Messaging 3.1 §4.2): topics, the four kinds of
 * subscription (§8.3), client identifiers (§6.1.2), temporary topics (§6.2.2)
 * and the legacy topic API (§2.9, §9.2).
 */
class PublishSubscribeTest {

    private static final long SOON = 5_000;
    private static final long BRIEFLY = 100;

    private TassisConnectionFactory factory;
    private Connection connection;
    private Session session;
    private Topic topic;

    @BeforeEach
    void connect() throws JMSException {
        factory = new TassisConnectionFactory("test-" + UUID.randomUUID());
        connection = factory.createConnection();
        session = connection.createSession();
        topic = session.createTopic("news");
        connection.start();
    }

    @AfterEach
    void disconnect() throws JMSException {
        connection.close();
    }

    private void publish(String text) throws JMSException {
        session.createProducer(topic).send(session.createTextMessage(text));
    }

    private static String text(Message message) throws JMSException {
        return message == null ? null : ((TextMessage) message).getText();
    }

    /** A second connection to the same broker, with that client identifier, already started. */
    private Connection connect(String clientId) throws JMSException {
        Connection other = factory.createConnection();
        if (clientId != null) {
            other.setClientID(clientId);
        }
        other.start();
        return other;
    }

    // ---- non-durable, unshared -------------------------------------------

    @Test
    void everySubscriberGetsItsOwnCopy() throws JMSException {
        MessageConsumer first = session.createConsumer(topic);
        MessageConsumer second = session.createConsumer(topic);

        publish("headline");

        assertEquals("headline", text(first.receive(SOON)));
        assertEquals("headline", text(second.receive(SOON)));
        assertNull(first.receiveNoWait());
    }

    @Test
    void nonDurableSubscriberMissesWhatWasPublishedBefore() throws JMSException {
        publish("too early");

        MessageConsumer late = session.createConsumer(topic);

        assertNull(late.receive(BRIEFLY));
    }

    @Test
    void topicAndQueueOfTheSameNameAreDifferentDestinations() throws JMSException {
        MessageConsumer subscriber = session.createConsumer(topic);

        session.createProducer(session.createQueue("news")).send(session.createTextMessage("to the queue"));

        assertNull(subscriber.receive(BRIEFLY));
        assertNotEquals(topic, session.createQueue("news"));
        assertEquals(topic, new TassisTopic("news"));
        assertEquals("news", topic.getTopicName());
    }

    @Test
    void noLocalSubscriberDoesNotGetItsOwnConnectionsMessages() throws JMSException {
        MessageConsumer noLocal = session.createConsumer(topic, null, true);

        publish("from myself");
        try (Connection other = connect(null); Session otherSession = other.createSession()) {
            otherSession.createProducer(topic).send(otherSession.createTextMessage("from someone else"));
        }

        assertEquals("from someone else", text(noLocal.receive(SOON)));
        assertNull(noLocal.receiveNoWait());
    }

    @Test
    void rolledBackMessageReturnsToItsSubscription() throws JMSException {
        try (Session transacted = connection.createSession(Session.SESSION_TRANSACTED)) {
            MessageConsumer subscriber = transacted.createConsumer(topic);
            publish("headline");
            assertEquals("headline", text(subscriber.receive(SOON)));

            transacted.rollback();
            Message again = subscriber.receive(SOON);

            assertEquals("headline", text(again));
            assertTrue(again.getJMSRedelivered());
            transacted.commit();
        }
    }

    // ---- durable ---------------------------------------------------------

    @Test
    void durableSubscriptionKeepsWhatIsPublishedWhileItHasNoConsumer() throws JMSException {
        try (Connection durable = connect("reader"); Session durableSession = durable.createSession()) {
            durableSession.createDurableConsumer(topic, "inbox").close();

            publish("while away");

            assertEquals("while away", text(durableSession.createDurableConsumer(topic, "inbox").receive(SOON)));
        }
    }

    @Test
    void durableSubscriptionSurvivesItsConnection() throws JMSException {
        try (Connection durable = connect("reader"); Session durableSession = durable.createSession()) {
            durableSession.createDurableSubscriber(topic, "inbox");
        }
        publish("while disconnected");

        try (Connection durable = connect("reader"); Session durableSession = durable.createSession()) {
            TopicSubscriber subscriber = durableSession.createDurableSubscriber(topic, "inbox");
            assertEquals("while disconnected", text(subscriber.receive(SOON)));
            assertEquals(topic, subscriber.getTopic());
        }
    }

    @Test
    void unsharedDurableSubscriptionNeedsAClientIdentifier() {
        assertThrows(jakarta.jms.IllegalStateException.class, () -> session.createDurableConsumer(topic, "inbox"));
    }

    @Test
    void unsharedDurableSubscriptionHasOneActiveConsumerAtMost() throws JMSException {
        try (Connection durable = connect("reader"); Session durableSession = durable.createSession()) {
            durableSession.createDurableConsumer(topic, "inbox");

            assertThrows(JMSException.class, () -> durableSession.createDurableConsumer(topic, "inbox"));
        }
    }

    @Test
    void resubscribingToAnotherTopicStartsANewSubscription() throws JMSException {
        try (Connection durable = connect("reader"); Session durableSession = durable.createSession()) {
            durableSession.createDurableConsumer(topic, "inbox").close();
            publish("for the old subscription");

            MessageConsumer moved = durableSession.createDurableConsumer(durableSession.createTopic("sport"), "inbox");

            assertNull(moved.receive(BRIEFLY), "the old subscription and its messages are gone");
        }
    }

    @Test
    void unsubscribeDeletesTheDurableSubscription() throws JMSException {
        try (Connection durable = connect("reader"); Session durableSession = durable.createSession()) {
            MessageConsumer consumer = durableSession.createDurableConsumer(topic, "inbox");
            assertThrows(JMSException.class, () -> durableSession.unsubscribe("inbox"),
                    "not while it has an active consumer");
            consumer.close();
            publish("kept until unsubscribe");

            durableSession.unsubscribe("inbox");

            assertNull(durableSession.createDurableConsumer(topic, "inbox").receive(BRIEFLY));
            assertThrows(InvalidDestinationException.class, () -> durableSession.unsubscribe("unknown"));
        }
    }

    // ---- shared ----------------------------------------------------------

    @Test
    void consumersOfASharedSubscriptionShareItsMessages() throws JMSException {
        MessageConsumer first = session.createSharedConsumer(topic, "workers");
        MessageConsumer second = session.createSharedConsumer(topic, "workers");

        publish("job");

        assertEquals("job", text(first.receive(SOON)));
        assertNull(second.receive(BRIEFLY), "one message goes to one consumer of the subscription");
    }

    @Test
    void sharedSubscriptionCannotChangeTopicWhileItHasConsumers() throws JMSException {
        session.createSharedConsumer(topic, "workers");

        assertThrows(JMSException.class, () -> session.createSharedConsumer(session.createTopic("sport"), "workers"));
    }

    @Test
    void sharedNonDurableSubscriptionEndsWithItsLastConsumer() throws JMSException {
        session.createSharedConsumer(topic, "workers").close();
        publish("nobody subscribed");

        assertNull(session.createSharedConsumer(topic, "workers").receive(BRIEFLY));
    }

    @Test
    void sharedDurableSubscriptionNeedsNoClientIdentifierAndPersists() throws JMSException {
        session.createSharedDurableConsumer(topic, "archive").close();
        publish("kept");

        MessageConsumer first = session.createSharedDurableConsumer(topic, "archive");
        MessageConsumer second = session.createSharedDurableConsumer(topic, "archive");

        assertEquals("kept", text(first.receive(SOON)));
        assertNull(second.receive(BRIEFLY));
        first.close();
        second.close();
        session.unsubscribe("archive");
    }

    @Test
    void sharedAndUnsharedDurableSubscriptionsShareOneNamespace() throws JMSException {
        try (Connection durable = connect("reader"); Session durableSession = durable.createSession()) {
            durableSession.createDurableConsumer(topic, "inbox").close();

            assertThrows(JMSException.class, () -> durableSession.createSharedDurableConsumer(topic, "inbox"));

            durableSession.createSharedConsumer(topic, "inbox").close();
        }
    }

    // ---- client identifier ----------------------------------------------

    @Test
    void clientIdentifierIsSetFirstOrNotAtAll() throws JMSException {
        assertThrows(jakarta.jms.IllegalStateException.class, () -> connection.setClientID("late"),
                "§6.1.2: before any other action on the connection");

        try (Connection fresh = factory.createConnection()) {
            fresh.setClientID("reader");
            assertEquals("reader", fresh.getClientID());
            assertThrows(jakarta.jms.IllegalStateException.class, () -> fresh.setClientID("again"));
        }
    }

    @Test
    void clientIdentifierIsUniqueAmongOpenConnections() throws JMSException {
        try (Connection first = connect("reader"); Connection second = factory.createConnection()) {
            assertNotNull(first);
            assertThrows(InvalidClientIDException.class, () -> second.setClientID("reader"));
        }

        connect("reader").close();
    }

    @Test
    void factoryCanAssignTheClientIdentifier() throws JMSException {
        try (Connection configured = factory.withClientID("configured").createConnection()) {
            assertEquals("configured", configured.getClientID());
            assertThrows(jakarta.jms.IllegalStateException.class, () -> configured.setClientID("other"));
        }
    }

    // ---- temporary topics ------------------------------------------------

    @Test
    void temporaryTopicBelongsToItsConnection() throws JMSException {
        TemporaryTopic temporary = session.createTemporaryTopic();
        MessageConsumer subscriber = session.createConsumer(temporary);

        try (Connection other = connect(null); Session otherSession = other.createSession()) {
            assertThrows(JMSException.class, () -> otherSession.createConsumer(temporary));
            otherSession.createProducer(temporary).send(otherSession.createTextMessage("anyone may publish"));
        }

        assertEquals("anyone may publish", text(subscriber.receive(SOON)));
        assertThrows(JMSException.class, temporary::delete, "not while it has a subscriber");
        subscriber.close();
        temporary.delete();
        assertThrows(InvalidDestinationException.class, () -> session.createProducer(temporary));
    }

    @Test
    void temporaryTopicCanBeDeletedOnceItsSubscriptionsHaveNoConsumer() throws JMSException {
        TemporaryTopic temporary = session.createTemporaryTopic();
        session.createSharedDurableConsumer(temporary, "idle").close();

        temporary.delete();

        assertThrows(InvalidDestinationException.class, () -> session.createConsumer(temporary));
    }

    // ---- legacy API ------------------------------------------------------

    @Test
    void legacyTopicApiPublishesAndSubscribes() throws JMSException {
        try (TopicConnection topicConnection = factory.createTopicConnection()) {
            TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic legacy = topicSession.createTopic("legacy");
            TopicSubscriber subscriber = topicSession.createSubscriber(legacy);
            TopicPublisher publisher = topicSession.createPublisher(legacy);
            topicConnection.start();

            publisher.publish(topicSession.createTextMessage("via TopicPublisher"));

            assertEquals(legacy, publisher.getTopic());
            assertEquals(legacy, subscriber.getTopic());
            assertFalse(subscriber.getNoLocal());
            assertEquals("via TopicPublisher", text(subscriber.receive(SOON)));
        }
    }

    @Test
    void domainSpecificSessionsRejectTheOtherDomainsMethods() throws JMSException {
        try (TopicConnection topicConnection = factory.createTopicConnection()) {
            TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            assertThrows(jakarta.jms.IllegalStateException.class, () -> topicSession.createQueue("q"));
            assertThrows(jakarta.jms.IllegalStateException.class, topicSession::createTemporaryQueue);
            assertThrows(jakarta.jms.IllegalStateException.class, () -> topicSession.createBrowser(null));
        }
        try (var queueConnection = factory.createQueueConnection()) {
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            assertThrows(jakarta.jms.IllegalStateException.class, () -> queueSession.createTopic("t"));
            assertThrows(jakarta.jms.IllegalStateException.class, queueSession::createTemporaryTopic);
            assertThrows(jakarta.jms.IllegalStateException.class, () -> queueSession.unsubscribe("s"));
            assertThrows(jakarta.jms.IllegalStateException.class,
                    () -> queueSession.createDurableSubscriber(topic, "s"));
            assertThrows(jakarta.jms.IllegalStateException.class, () -> queueSession.createSharedConsumer(topic, "s"));
        }
    }
}
