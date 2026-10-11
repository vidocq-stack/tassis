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
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Tassis client against a broker that is not Tassis: Apache ActiveMQ Artemis, embedded
 * (ROADMAP M11.3e). The conventions by which a Jakarta Messaging client and a broker
 * understand each other over AMQP 1.0 are no standard, so what holds against a Tassis server
 * is only known to hold elsewhere once it has been run there.
 */
@org.junit.jupiter.api.Timeout(40)
class TassisClientAgainstArtemisTest {

    private static final int PORT = 25_673;
    private static final long SOON = 5_000;
    private static final long BRIEFLY = 300;

    private static EmbeddedActiveMQ broker;

    private final TassisConnectionFactory tassis = TassisConnectionFactory.fromUrl("tassis:amqp://127.0.0.1:" + PORT);
    private final ConnectionFactory qpid = new JmsConnectionFactory("amqp://127.0.0.1:" + PORT);
    // Each test has destinations of its own: the broker is shared by the tests of the class.
    private String unique;

    @BeforeAll
    static void startBroker() throws Exception {
        ConfigurationImpl configuration = new ConfigurationImpl();
        configuration.setPersistenceEnabled(false);
        configuration.setSecurityEnabled(false);
        configuration.setJMXManagementEnabled(false);
        configuration.addAcceptorConfiguration("amqp", "tcp://127.0.0.1:" + PORT + "?protocols=AMQP");
        broker = new EmbeddedActiveMQ();
        broker.setConfiguration(configuration);
        broker.start();
    }

    @AfterAll
    static void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @BeforeEach
    void nameDestinations() {
        unique = UUID.randomUUID().toString();
    }

    private String name(String base) {
        return base + "-" + unique;
    }

    // ---- queues ---------------------------------------------------------------------

    @Test
    void aMessageIsSentAndReceived() throws Exception {
        try (JMSContext context = tassis.createContext()) {
            Queue orders = context.createQueue(name("orders"));
            context.createProducer().setProperty("region", "north").setPriority(7).send(orders, "first");

            TextMessage received = (TextMessage) context.createConsumer(orders).receive(SOON);

            assertNotNull(received);
            assertEquals("first", received.getText());
            assertEquals("north", received.getStringProperty("region"));
            assertEquals(7, received.getJMSPriority());
            assertEquals(orders, received.getJMSDestination());
            assertFalse(received.getJMSRedelivered());
            assertNotNull(received.getJMSMessageID());
        }
    }

    @Test
    void aReceiveThatFindsNothingReturnsNothing() {
        try (JMSContext context = tassis.createContext()) {
            Queue empty = context.createQueue(name("empty"));
            JMSConsumer consumer = context.createConsumer(empty);

            assertNull(consumer.receiveNoWait());
            assertNull(consumer.receive(BRIEFLY));

            context.createProducer().send(empty, "at last");
            assertEquals("at last", consumer.receiveBody(String.class, SOON));
        }
    }

    @Test
    void aMessageNobodyAskedForStaysWithTheBroker() {
        try (JMSContext idle = tassis.createContext(); JMSContext busy = tassis.createContext()) {
            Queue orders = idle.createQueue(name("orders"));
            idle.createConsumer(orders);
            idle.start();
            JMSConsumer working = busy.createConsumer(orders);

            busy.createProducer().send(orders, "a").send(orders, "b");

            assertEquals("a", working.receiveBody(String.class, SOON));
            assertEquals("b", working.receiveBody(String.class, SOON));
        }
    }

    @Test
    void messagesAreDeliveredToAListener() throws Exception {
        BlockingQueue<String> heard = new ArrayBlockingQueue<>(10);
        try (JMSContext context = tassis.createContext()) {
            Queue orders = context.createQueue(name("orders"));
            context.createConsumer(orders).setMessageListener(message -> {
                try {
                    heard.add(message.getBody(String.class));
                } catch (JMSException e) {
                    heard.add(e.toString());
                }
            });

            context.createProducer().send(orders, "a").send(orders, "b").send(orders, "c");

            assertEquals("a", heard.poll(SOON, TimeUnit.MILLISECONDS));
            assertEquals("b", heard.poll(SOON, TimeUnit.MILLISECONDS));
            assertEquals("c", heard.poll(SOON, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void everyKindOfMessageIsCarried() throws Exception {
        try (JMSContext context = tassis.createContext()) {
            Queue all = context.createQueue(name("all"));
            MapMessage map = context.createMapMessage();
            map.setInt("count", 3);
            BytesMessage bytes = context.createBytesMessage();
            bytes.writeBytes(new byte[] {1, 2, 3});
            context.createProducer().send(all, map).send(all, bytes).send(all, new ArrayList<>(List.of("serialized")))
                    .send(all, context.createMessage());
            JMSConsumer consumer = context.createConsumer(all);

            assertEquals(3, ((MapMessage) consumer.receive(SOON)).getInt("count"));
            assertEquals(3, ((BytesMessage) consumer.receive(SOON)).getBodyLength());
            assertEquals(List.of("serialized"), consumer.receiveBody(List.class, SOON));
            assertNotNull(consumer.receive(SOON));
        }
    }

    @Test
    void aLargeMessageIsCarried() {
        String large = "x".repeat(500_000);
        try (JMSContext context = tassis.createContext()) {
            Queue queue = context.createQueue(name("large"));
            context.createProducer().send(queue, large);
            assertEquals(large, context.createConsumer(queue).receiveBody(String.class, SOON));
        }
    }

    @Test
    void aTassisClientAndAQpidClientUnderstandEachOther() {
        try (JMSContext ours = tassis.createContext(); JMSContext theirs = qpid.createContext()) {
            ours.createProducer().send(ours.createQueue(name("to-qpid")), "from Tassis");
            assertEquals("from Tassis",
                    theirs.createConsumer(theirs.createQueue(name("to-qpid"))).receiveBody(String.class, SOON));

            theirs.createProducer().send(theirs.createQueue(name("to-tassis")), "from Qpid JMS");
            assertEquals("from Qpid JMS",
                    ours.createConsumer(ours.createQueue(name("to-tassis"))).receiveBody(String.class, SOON));
        }
    }

    // ---- acknowledgement and transactions ------------------------------------------------

    @Test
    void anUnacknowledgedMessageIsRedeliveredAfterRecover() throws Exception {
        try (JMSContext context = tassis.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
            Queue orders = context.createQueue(name("orders"));
            context.createProducer().send(orders, "once");
            JMSConsumer consumer = context.createConsumer(orders);

            assertEquals("once", consumer.receiveBody(String.class, SOON));
            context.recover();

            Message again = consumer.receive(SOON);
            assertNotNull(again);
            assertEquals("once", again.getBody(String.class));
            assertTrue(again.getJMSRedelivered());
            context.acknowledge();
            assertNull(consumer.receive(BRIEFLY));
        }
    }

    @Test
    void aTransactionSendsAndAcknowledgesAtCommitOnly() {
        try (JMSContext transacted = tassis.createContext(JMSContext.SESSION_TRANSACTED);
                JMSContext observer = tassis.createContext()) {
            Queue in = transacted.createQueue(name("in"));
            Queue out = transacted.createQueue(name("out"));
            observer.createProducer().send(in, "work");
            JMSConsumer results = observer.createConsumer(out);

            assertEquals("work", transacted.createConsumer(in).receiveBody(String.class, SOON));
            transacted.createProducer().send(out, "result 1").send(out, "result 2");
            assertNull(results.receive(BRIEFLY), "nothing is sent before the commit");

            transacted.commit();

            assertEquals("result 1", results.receiveBody(String.class, SOON));
            assertEquals("result 2", results.receiveBody(String.class, SOON));
            assertNull(observer.createConsumer(in).receive(BRIEFLY), "what was received is acknowledged");
        }
    }

    @Test
    void aRolledBackTransactionSendsNothingAndGivesBackWhatItReceived() throws Exception {
        try (JMSContext transacted = tassis.createContext(JMSContext.SESSION_TRANSACTED);
                JMSContext observer = tassis.createContext()) {
            Queue in = transacted.createQueue(name("in"));
            Queue out = transacted.createQueue(name("out"));
            observer.createProducer().send(in, "work");
            JMSConsumer consumer = transacted.createConsumer(in);

            assertEquals("work", consumer.receiveBody(String.class, SOON));
            transacted.createProducer().send(out, "never");
            transacted.rollback();

            Message again = consumer.receive(SOON);
            assertNotNull(again);
            assertTrue(again.getJMSRedelivered());
            transacted.commit();
            assertNull(observer.createConsumer(out).receive(BRIEFLY));
        }
    }

    // ---- topics -----------------------------------------------------------------------

    @Test
    void everySubscriberOfATopicGetsTheMessage() {
        try (JMSContext context = tassis.createContext(); JMSContext other = tassis.createContext()) {
            Topic news = context.createTopic(name("news"));
            JMSConsumer first = context.createConsumer(news);
            JMSConsumer second = other.createConsumer(news, "kind = 'sport'");

            context.createProducer().setProperty("kind", "politics").send(news, "a vote");
            context.createProducer().setProperty("kind", "sport").send(news, "a match");

            assertEquals("a vote", first.receiveBody(String.class, SOON));
            assertEquals("a match", first.receiveBody(String.class, SOON));
            assertEquals("a match", second.receiveBody(String.class, SOON));
        }
    }

    @Test
    void aDurableSubscriptionKeepsWhatIsPublishedWhileItsConsumerIsAway() {
        TassisConnectionFactory named = tassis.withClientID(name("reader"));
        String topic = name("news");
        try (JMSContext context = named.createContext()) {
            context.createDurableConsumer(context.createTopic(topic), "mine").close();
        }
        try (JMSContext publisher = tassis.createContext()) {
            publisher.createProducer().send(publisher.createTopic(topic), "while away");
        }
        try (JMSContext context = named.createContext()) {
            JMSConsumer back = context.createDurableConsumer(context.createTopic(topic), "mine");
            assertEquals("while away", back.receiveBody(String.class, SOON));
            back.close();

            context.unsubscribe("mine");
            assertThrows(jakarta.jms.InvalidDestinationRuntimeException.class, () -> context.unsubscribe("mine"));
        }
    }

    @Test
    void aSharedSubscriptionGivesEachMessageToOneOfItsConsumers() {
        try (JMSContext one = tassis.createContext(); JMSContext two = tassis.createContext()) {
            Topic news = one.createTopic(name("news"));
            JMSConsumer first = one.createSharedConsumer(news, name("pool"));
            JMSConsumer second = two.createSharedConsumer(news, name("pool"));

            one.createProducer().send(news, "only once");

            String byFirst = first.receiveBody(String.class, BRIEFLY);
            String bySecond = second.receiveBody(String.class, BRIEFLY);
            assertTrue(byFirst == null ^ bySecond == null, "one of them, not both: " + byFirst + ", " + bySecond);
        }
    }

    @Test
    void aSharedDurableSubscriptionIsSharedByConnectionsWithoutAClientIdentifier() {
        String topic = name("news");
        String subscription = name("pool");
        try (JMSContext one = tassis.createContext(); JMSContext two = tassis.createContext()) {
            Topic news = one.createTopic(topic);
            JMSConsumer first = one.createSharedDurableConsumer(news, subscription);
            JMSConsumer second = two.createSharedDurableConsumer(news, subscription);
            // and a second consumer of the same connection, which is another link to it
            JMSConsumer third = one.createSharedDurableConsumer(news, subscription);

            one.createProducer().send(news, "only once");

            int received = 0;
            for (JMSConsumer consumer : List.of(first, second, third)) {
                received += consumer.receiveBody(String.class, BRIEFLY) == null ? 0 : 1;
            }
            assertEquals(1, received, "one of them, and only one");
            first.close();
            second.close();
            third.close();

            // §8.3.5: the name is the broker's, so any connection without an identifier deletes it
            two.unsubscribe(subscription);
            assertThrows(jakarta.jms.InvalidDestinationRuntimeException.class, () -> one.unsubscribe(subscription));
        }
    }

    @Test
    void aSubscriberThatWantsNoLocalMessagesGetsNoneOfItsOwnConnection() {
        try (JMSContext context = tassis.createContext(); JMSContext other = tassis.createContext()) {
            Topic news = context.createTopic(name("news"));
            JMSConsumer consumer = context.createConsumer(news, null, true);

            context.createProducer().send(news, "mine");
            other.createProducer().send(news, "theirs");

            assertEquals("theirs", consumer.receiveBody(String.class, SOON));
        }
    }

    // ---- temporary destinations, browsers, identity ------------------------------------------

    @Test
    void aReplyComesBackToATemporaryQueue() throws Exception {
        try (JMSContext requester = tassis.createContext(); JMSContext replier = tassis.createContext()) {
            Queue requests = requester.createQueue(name("requests"));
            TemporaryQueue replies = requester.createTemporaryQueue();
            requester.createProducer().setJMSReplyTo(replies).send(requests, "ping");

            Message request = replier.createConsumer(requests).receive(SOON);
            assertNotNull(request);
            replier.createProducer().send(request.getJMSReplyTo(), "pong");

            assertEquals("pong", requester.createConsumer(replies).receiveBody(String.class, SOON));
        }
    }

    @Test
    void aTemporaryQueueIsDeleted() throws Exception {
        try (JMSContext context = tassis.createContext()) {
            TemporaryQueue temporary = context.createTemporaryQueue();
            JMSConsumer consumer = context.createConsumer(temporary);
            assertThrows(JMSException.class, temporary::delete);
            consumer.close();
            temporary.delete();
        }
    }

    @Test
    void aBrowserSeesTheQueueAndTakesNothing() throws Exception {
        try (JMSContext context = tassis.createContext()) {
            Queue orders = context.createQueue(name("orders"));
            for (int i = 0; i < 12; i++) {
                context.createProducer().setProperty("even", i % 2 == 0).send(orders, "order " + i);
            }
            try (QueueBrowser all = context.createBrowser(orders);
                    QueueBrowser even = context.createBrowser(orders, "even = TRUE")) {
                List<?> seen = Collections.list(all.getEnumeration());
                assertEquals(12, seen.size());
                assertEquals("order 0", ((TextMessage) seen.get(0)).getText());
                assertEquals(6, Collections.list(even.getEnumeration()).size());
            }
            assertEquals("order 0", context.createConsumer(orders).receiveBody(String.class, SOON));
        }
    }

    @Test
    void aClientIdentifierBelongsToOneConnectionAtATime() throws Exception {
        String identifier = name("only-one");
        try (Connection first = tassis.createConnection()) {
            first.setClientID(identifier);
            assertThrows(InvalidClientIDException.class, () -> tassis.withClientID(identifier).createConnection());
        }
        try (Connection again = tassis.withClientID(identifier).createConnection()) {
            assertEquals(identifier, again.getClientID());
        }
    }

    @Test
    void whatAClosedConnectionHadNotAcknowledgedIsDeliveredAgain() throws Exception {
        String queue = name("orders");
        try (JMSContext sender = tassis.createContext()) {
            sender.createProducer().send(sender.createQueue(queue), "not lost");
        }
        try (JMSContext careless = tassis.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
            assertEquals("not lost", careless.createConsumer(careless.createQueue(queue)).receiveBody(String.class, SOON));
        }
        try (JMSContext careful = tassis.createContext()) {
            Message again = careful.createConsumer(careful.createQueue(queue)).receive(SOON);
            assertNotNull(again);
            assertEquals("not lost", again.getBody(String.class));
        }
    }

    @Test
    void aConnectionMayStayIdle() throws Exception {
        try (Connection connection = tassis.createConnection()) {
            Session session = connection.createSession();
            Thread.sleep(2_000);
            Queue queue = session.createQueue(name("idle"));
            session.createProducer(queue).send(session.createTextMessage("still there"));
            connection.start();
            assertEquals("still there", ((TextMessage) session.createConsumer(queue).receive(SOON)).getText());
        }
    }

    @Test
    void aStoppedConnectionReceivesNothingUntilItStartsAgain() throws Exception {
        try (Connection connection = tassis.createConnection(); JMSContext other = tassis.createContext()) {
            Session session = connection.createSession();
            Queue orders = session.createQueue(name("orders"));
            jakarta.jms.MessageConsumer consumer = session.createConsumer(orders);
            session.createProducer(orders).send(session.createTextMessage("held"));

            assertNull(consumer.receive(BRIEFLY), "a connection is created stopped");
            assertEquals("held", other.createConsumer(other.createQueue(name("orders"))).receiveBody(String.class, SOON),
                    "and keeps nothing from another consumer meanwhile");

            session.createProducer(orders).send(session.createTextMessage("delivered"));
            connection.start();
            assertEquals("delivered", ((TextMessage) consumer.receive(SOON)).getText());
            connection.stop();
            session.createProducer(orders).send(session.createTextMessage("after the stop"));
            assertNull(consumer.receive(BRIEFLY));
            connection.start();
            assertEquals("after the stop", ((TextMessage) consumer.receive(SOON)).getText());
        }
    }

    @Test
    void whatAClosedConsumerDeliveredCanStillBeAcknowledged() throws Exception {
        String queue = name("orders");
        try (JMSContext context = tassis.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
            Queue orders = context.createQueue(queue);
            context.createProducer().send(orders, "kept");
            JMSConsumer consumer = context.createConsumer(orders);
            Message received = consumer.receive(SOON);
            assertNotNull(received);

            consumer.close();
            assertNull(context.createConsumer(orders).receive(BRIEFLY), "not delivered again");
            received.acknowledge();
        }
        try (JMSContext after = tassis.createContext()) {
            assertNull(after.createConsumer(after.createQueue(queue)).receive(BRIEFLY), "acknowledged for good");
        }
    }

    @Test
    void aMessageIsDeliveredAfterItsDelayAndNotAfterItsExpiration() throws Exception {
        try (JMSContext context = tassis.createContext()) {
            Queue orders = context.createQueue(name("orders"));
            long sent = System.nanoTime();
            context.createProducer().setTimeToLive(200).send(orders, "short-lived");
            context.createProducer().setDeliveryDelay(1_500).send(orders, "delayed");
            Thread.sleep(500);
            JMSConsumer consumer = context.createConsumer(orders);

            assertEquals("delayed", consumer.receiveBody(String.class, SOON), "the first one has expired by then");
            assertTrue(System.nanoTime() - sent >= TimeUnit.MILLISECONDS.toNanos(1_300), "not before its delay");
        }
    }

    @Test
    void sessionsOfOneConnectionCommitAtTheSameTime() throws Exception {
        String queue = name("committed");
        try (Connection connection = tassis.createConnection()) {
            List<java.util.concurrent.CompletableFuture<Void>> commits = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                Session session = connection.createSession(Session.SESSION_TRANSACTED);
                jakarta.jms.MessageProducer producer = session.createProducer(session.createQueue(queue));
                producer.send(session.createTextMessage("a"));
                producer.send(session.createTextMessage("b"));
                commits.add(java.util.concurrent.CompletableFuture.runAsync(() -> {
                    try {
                        session.commit();
                    } catch (JMSException e) {
                        throw new IllegalStateException(e);
                    }
                }));
            }
            for (java.util.concurrent.CompletableFuture<Void> commit : commits) {
                commit.get(SOON, TimeUnit.MILLISECONDS);
            }
            Session session = connection.createSession();
            try (QueueBrowser browser = session.createBrowser(session.createQueue(queue))) {
                assertEquals(12, Collections.list(browser.getEnumeration()).size());
            }
        }
    }
}
