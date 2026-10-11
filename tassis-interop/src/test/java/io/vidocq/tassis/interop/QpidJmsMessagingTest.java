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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TransactionRolledBackException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Apache Qpid JMS against a Tassis broker opened to the network, beyond queues: topics and the
 * four kinds of subscription (Jakarta Messaging 3.1 §8.3), message selectors (§3.8), temporary
 * destinations (§6.2.2), local transactions (§6.2.7), queue browsers (§4.1.3) and delivery
 * delay (§7.9). Nothing of Tassis is on the client's side.
 */
class QpidJmsMessagingTest {

    private static final long SOON = 5_000;
    private static final long BRIEFLY = 300;

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
        return connect(null);
    }

    private Connection connect(String clientId) throws JMSException {
        Connection connection = qpid.createConnection();
        opened.add(connection);
        if (clientId != null) {
            connection.setClientID(clientId);
        }
        connection.start();
        return connection;
    }

    private static String text(Message message) throws JMSException {
        return message == null ? null : ((TextMessage) message).getText();
    }

    private static void send(Session session, jakarta.jms.Destination destination, String... texts) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        for (String text : texts) {
            producer.send(session.createTextMessage(text));
        }
        producer.close();
    }

    // ---- topics and subscriptions (§8.3) ---------------------------------------

    @Test
    void aTopicDeliversToEverySubscriberAndKeepsNothingForLater() throws Exception {
        Session session = connect().createSession();
        Topic topic = session.createTopic("news");
        send(session, topic, "before anyone listens");
        MessageConsumer one = session.createConsumer(topic);
        MessageConsumer two = connect().createSession().createConsumer(topic);

        send(session, topic, "headline");

        Message first = one.receive(SOON);
        assertEquals("headline", text(first));
        assertEquals("news", ((Topic) first.getJMSDestination()).getTopicName());
        assertEquals("headline", text(two.receive(SOON)));
        assertNull(one.receive(BRIEFLY));
    }

    @Test
    void aTopicAndAQueueOfTheSameNameAreDifferent() throws Exception {
        Session session = connect().createSession();
        MessageConsumer subscriber = session.createConsumer(session.createTopic("shared-name"));

        send(session, session.createQueue("shared-name"), "to the queue");

        assertNull(subscriber.receive(BRIEFLY));
        assertEquals("to the queue", text(session.createConsumer(session.createQueue("shared-name")).receive(SOON)));
    }

    @Test
    void aDurableSubscriptionKeepsWhatIsPublishedWhileItsClientIsAway() throws Exception {
        Connection first = qpid.createConnection();
        first.setClientID("desk");
        first.start();
        Session session = first.createSession();
        Topic topic = session.createTopic("news");
        session.createDurableConsumer(topic, "archive").close();
        first.close();

        Session publisher = connect().createSession();
        send(publisher, publisher.createTopic("news"), "while away 1", "while away 2");

        Session back = connect("desk").createSession();
        MessageConsumer consumer = back.createDurableConsumer(back.createTopic("news"), "archive");
        assertEquals("while away 1", text(consumer.receive(SOON)));
        assertEquals("while away 2", text(consumer.receive(SOON)));
    }

    @Test
    void unsubscribeDeletesADurableSubscription() throws Exception {
        Session session = connect("desk").createSession();
        Topic topic = session.createTopic("news");
        MessageConsumer consumer = session.createDurableConsumer(topic, "archive");

        assertThrows(JMSException.class, () -> session.unsubscribe("archive"), "it has an active consumer");
        consumer.close();
        session.unsubscribe("archive");

        assertThrows(InvalidDestinationException.class, () -> session.unsubscribe("archive"));
        send(session, topic, "published to nobody");
        assertNull(session.createDurableConsumer(topic, "archive").receive(BRIEFLY), "a new subscription starts empty");
    }

    @Test
    void anUnsharedDurableSubscriptionHasOneConsumerAtATime() throws Exception {
        Session session = connect("desk").createSession();
        Topic topic = session.createTopic("news");
        session.createDurableConsumer(topic, "archive");

        assertThrows(JMSException.class, () -> session.createDurableConsumer(topic, "archive"));
    }

    @Test
    void theConsumersOfASharedSubscriptionShareItsMessages() throws Exception {
        Session one = connect().createSession();
        Session two = connect().createSession();
        Topic topic = one.createTopic("jobs");
        MessageConsumer first = one.createSharedConsumer(topic, "workers");
        MessageConsumer second = two.createSharedConsumer(two.createTopic("jobs"), "workers");
        MessageConsumer alone = one.createConsumer(topic);

        Session publisher = connect().createSession();
        send(publisher, publisher.createTopic("jobs"), "a", "b", "c", "d");

        List<String> shared = new ArrayList<>();
        for (MessageConsumer consumer : List.of(first, second)) {
            for (Message message = consumer.receive(BRIEFLY); message != null; message = consumer.receive(BRIEFLY)) {
                shared.add(text(message));
            }
        }
        Collections.sort(shared);
        assertEquals(List.of("a", "b", "c", "d"), shared, "each message to one of the two, and to one only");
        List<String> own = new ArrayList<>();
        for (Message message = alone.receive(BRIEFLY); message != null; message = alone.receive(BRIEFLY)) {
            own.add(text(message));
        }
        assertEquals(List.of("a", "b", "c", "d"), own, "an unshared subscriber gets them all");
    }

    @Test
    void aSharedDurableSubscriptionSurvivesItsConsumers() throws Exception {
        Session session = connect().createSession();
        Topic topic = session.createTopic("jobs");
        session.createSharedDurableConsumer(topic, "pool").close();

        send(session, topic, "kept");

        assertEquals("kept", text(session.createSharedDurableConsumer(topic, "pool").receive(SOON)));
    }

    @Test
    void aNoLocalSubscriptionIgnoresItsOwnConnection() throws Exception {
        Connection connection = connect("desk");
        Session session = connection.createSession();
        Topic topic = session.createTopic("chat");
        MessageConsumer noLocal = session.createConsumer(topic, null, true);

        send(session, topic, "from myself");
        Session other = connect().createSession();
        send(other, other.createTopic("chat"), "from another");

        assertEquals("from another", text(noLocal.receive(SOON)));
        assertNull(noLocal.receive(BRIEFLY));
    }

    // ---- selectors (§3.8) -------------------------------------------------------

    @Test
    void aSelectorChoosesAmongTheMessagesOfAQueue() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("parcels");
        MessageProducer producer = session.createProducer(queue);
        for (String color : List.of("red", "blue", "red")) {
            TextMessage message = session.createTextMessage(color);
            message.setStringProperty("color", color);
            message.setIntProperty("weight", color.length());
            producer.send(message);
        }

        MessageConsumer blue = session.createConsumer(queue, "color = 'blue' AND weight > 3");
        assertEquals("blue", text(blue.receive(SOON)));
        assertNull(blue.receive(BRIEFLY));
        blue.close();

        MessageConsumer rest = session.createConsumer(queue);
        assertEquals("red", text(rest.receive(SOON)));
        assertEquals("red", text(rest.receive(SOON)));
    }

    @Test
    void aSelectorOnHeaderFieldsAndOnATopic() throws Exception {
        Session session = connect().createSession();
        Topic topic = session.createTopic("alerts");
        MessageConsumer urgent = session.createConsumer(topic, "JMSPriority >= 7 AND JMSType = 'alert'");
        MessageProducer producer = session.createProducer(topic);

        TextMessage low = session.createTextMessage("low");
        low.setJMSType("alert");
        producer.send(low, jakarta.jms.DeliveryMode.PERSISTENT, 2, 0);
        TextMessage high = session.createTextMessage("high");
        high.setJMSType("alert");
        producer.send(high, jakarta.jms.DeliveryMode.PERSISTENT, 8, 0);

        assertEquals("high", text(urgent.receive(SOON)));
        assertNull(urgent.receive(BRIEFLY));
    }

    @Test
    void aSelectorThatIsNotOneIsRefused() throws Exception {
        Session session = connect().createSession();

        assertThrows(InvalidSelectorException.class,
                () -> session.createConsumer(session.createQueue("parcels"), "color = = 'blue'"));
        // and the session is still usable
        send(session, session.createQueue("parcels"), "fine");
        assertEquals("fine", text(session.createConsumer(session.createQueue("parcels")).receive(SOON)));
    }

    // ---- temporary destinations (§6.2.2) ------------------------------------------

    @Test
    void aRequestIsAnsweredOnATemporaryQueue() throws Exception {
        Connection requester = connect();
        Session session = requester.createSession();
        Queue requests = session.createQueue("requests");
        TemporaryQueue replies = session.createTemporaryQueue();
        TextMessage request = session.createTextMessage("ping");
        request.setJMSReplyTo(replies);
        session.createProducer(requests).send(request);

        Session responder = connect().createSession();
        Message received = responder.createConsumer(responder.createQueue("requests")).receive(SOON);
        assertNotNull(received.getJMSReplyTo());
        TextMessage reply = responder.createTextMessage("pong");
        reply.setJMSCorrelationID(received.getJMSMessageID());
        responder.createProducer(received.getJMSReplyTo()).send(reply);

        Message answer = session.createConsumer(replies).receive(SOON);
        assertEquals("pong", text(answer));
        assertEquals(request.getJMSMessageID(), answer.getJMSCorrelationID());
    }

    @Test
    void aTemporaryQueueIsAnsweredFromTheJvmToo() throws Exception {
        Session session = connect().createSession();
        TemporaryQueue replies = session.createTemporaryQueue();
        TextMessage request = session.createTextMessage("ping");
        request.setJMSReplyTo(replies);
        session.createProducer(session.createQueue("requests")).send(request);

        try (JMSContext inVm = new TassisConnectionFactory(brokerName).createContext()) {
            Message received = inVm.createConsumer(inVm.createQueue("requests")).receive(SOON);
            inVm.createProducer().send(received.getJMSReplyTo(), "pong from the JVM");
        }

        assertEquals("pong from the JVM", text(session.createConsumer(replies).receive(SOON)));
    }

    @Test
    void aTemporaryDestinationEndsWhenDeletedOrWithItsConnection() throws Exception {
        Connection owner = qpid.createConnection();
        owner.start();
        Session session = owner.createSession();
        TemporaryQueue deleted = session.createTemporaryQueue();
        TemporaryQueue kept = session.createTemporaryQueue();
        TemporaryTopic topic = session.createTemporaryTopic();
        MessageConsumer subscriber = session.createConsumer(topic);
        send(session, topic, "on a temporary topic");
        assertEquals("on a temporary topic", text(subscriber.receive(SOON)));

        MessageConsumer consumer = session.createConsumer(deleted);
        assertThrows(JMSException.class, deleted::delete, "it has a consumer");
        consumer.close();
        deleted.delete();

        Session other = connect().createSession();
        assertThrows(JMSException.class, () -> send(other, deleted, "to a deleted queue"));
        send(other, kept, "to a living one");
        owner.close();
        Session later = connect().createSession();
        assertThrows(JMSException.class, () -> send(later, kept, "after its connection closed"));
    }

    // ---- local transactions (§6.2.7) ------------------------------------------------

    @Test
    void whatIsSentInATransactionArrivesWhenItCommits() throws Exception {
        Connection connection = connect();
        Session transacted = connection.createSession(Session.SESSION_TRANSACTED);
        Queue queue = transacted.createQueue("orders");
        MessageConsumer outside = connection.createSession().createConsumer(queue);

        send(transacted, queue, "a", "b");
        assertNull(outside.receive(BRIEFLY), "nothing before the commit");
        transacted.commit();

        assertEquals("a", text(outside.receive(SOON)));
        assertEquals("b", text(outside.receive(SOON)));
    }

    @Test
    void whatIsSentInATransactionThatRollsBackNeverArrives() throws Exception {
        Connection connection = connect();
        Session transacted = connection.createSession(Session.SESSION_TRANSACTED);
        Queue queue = transacted.createQueue("orders");

        send(transacted, queue, "never");
        transacted.rollback();
        send(transacted, queue, "kept");
        transacted.commit();

        MessageConsumer outside = connection.createSession().createConsumer(queue);
        assertEquals("kept", text(outside.receive(SOON)));
        assertNull(outside.receive(BRIEFLY));
    }

    @Test
    void whatIsReceivedInATransactionThatRollsBackIsDeliveredAgain() throws Exception {
        Connection connection = connect();
        Session transacted = connection.createSession(Session.SESSION_TRANSACTED);
        Queue queue = transacted.createQueue("orders");
        send(transacted, queue, "to process");
        transacted.commit();
        MessageConsumer consumer = transacted.createConsumer(queue);

        Message first = consumer.receive(SOON);
        assertFalse(first.getJMSRedelivered());
        transacted.rollback();

        Message again = consumer.receive(SOON);
        assertEquals("to process", text(again));
        assertTrue(again.getJMSRedelivered());
        transacted.commit();
        consumer.close();

        try (JMSContext inVm = new TassisConnectionFactory(brokerName).createContext()) {
            assertNull(inVm.createConsumer(inVm.createQueue("orders")).receiveNoWait(), "consumed by the commit");
        }
    }

    @Test
    void aTransactionMovesAMessageFromAQueueToAnotherOrNotAtAll() throws Exception {
        Connection connection = connect();
        Session setup = connection.createSession();
        send(setup, setup.createQueue("in"), "moved");
        Session transacted = connection.createSession(Session.SESSION_TRANSACTED);
        MessageConsumer in = transacted.createConsumer(transacted.createQueue("in"));
        MessageProducer out = transacted.createProducer(transacted.createQueue("out"));

        out.send(transacted.createTextMessage(text(in.receive(SOON)) + " once"));
        transacted.rollback();
        out.send(transacted.createTextMessage(text(in.receive(SOON)) + " twice"));
        transacted.commit();

        MessageConsumer check = setup.createConsumer(setup.createQueue("out"));
        assertEquals("moved twice", text(check.receive(SOON)));
        assertNull(check.receive(BRIEFLY));
        assertNull(setup.createConsumer(setup.createQueue("in")).receive(BRIEFLY));
    }

    @Test
    void aCommitThatCannotSendRollsTheTransactionBack() throws Exception {
        Connection owner = qpid.createConnection();
        owner.start();
        TemporaryQueue gone = owner.createSession().createTemporaryQueue();
        Connection connection = connect();
        Session transacted = connection.createSession(Session.SESSION_TRANSACTED);
        MessageProducer toGone = transacted.createProducer(gone);
        toGone.send(transacted.createTextMessage("to a queue about to go"));
        send(transacted, transacted.createQueue("orders"), "its companion");
        owner.close();

        assertThrows(TransactionRolledBackException.class, transacted::commit);

        assertNull(connection.createSession().createConsumer(transacted.createQueue("orders")).receive(BRIEFLY),
                "none of the sends was made");
    }

    // ---- queue browsers (§4.1.3) and delivery delay (§7.9) ---------------------------

    @Test
    void aBrowserSeesTheMessagesAndLeavesThem() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("orders");
        MessageProducer producer = session.createProducer(queue);
        for (String color : List.of("red", "blue", "red")) {
            TextMessage message = session.createTextMessage(color);
            message.setStringProperty("color", color);
            producer.send(message);
        }

        List<String> all = new ArrayList<>();
        try (QueueBrowser browser = session.createBrowser(queue)) {
            for (Object message : Collections.list(browser.getEnumeration())) {
                all.add(text((Message) message));
            }
        }
        List<String> red = new ArrayList<>();
        try (QueueBrowser browser = session.createBrowser(queue, "color = 'red'")) {
            for (Object message : Collections.list(browser.getEnumeration())) {
                red.add(text((Message) message));
            }
        }
        List<String> none = new ArrayList<>();
        try (QueueBrowser browser = session.createBrowser(session.createQueue("empty"))) {
            for (Object message : Collections.list(browser.getEnumeration())) {
                none.add(text((Message) message));
            }
        }

        assertEquals(List.of("red", "blue", "red"), all);
        assertEquals(List.of("red", "red"), red);
        assertEquals(List.of(), none);
        MessageConsumer consumer = session.createConsumer(queue);
        assertEquals("red", text(consumer.receive(SOON)));
        assertEquals("blue", text(consumer.receive(SOON)));
        assertEquals("red", text(consumer.receive(SOON)));
    }

    @Test
    void aDelayedMessageIsNotDeliveredBeforeItsTime() throws Exception {
        Session session = connect().createSession();
        Queue queue = session.createQueue("later");
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryDelay(600);
        long sent = System.currentTimeMillis();
        producer.send(session.createTextMessage("delayed"));
        MessageConsumer consumer = session.createConsumer(queue);

        assertNull(consumer.receive(200));
        Message received = consumer.receive(SOON);

        assertEquals("delayed", text(received));
        assertTrue(System.currentTimeMillis() - sent >= 550, "not before its delivery time");
        assertTrue(received.getJMSDeliveryTime() >= sent + 600);
    }
}
