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
package io.vidocq.tassis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.TassisBroker;
import io.vidocq.tassis.core.TassisConnectionFactory;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Tassis client through the network: a {@code TassisConnectionFactory} whose URL is
 * {@code tassis:amqp://...} reaches a broker a {@code TassisServer} opens, and an application
 * sees no difference with the broker of its JVM (Jakarta Messaging 3.1; ROADMAP M11.3d).
 */
class NetworkClientTest {

    private static final long SOON = 5_000;
    private static final long BRIEFLY = 200;

    private String brokerName;
    TassisServer server;
    TassisConnectionFactory network;
    private TassisConnectionFactory inVm;

    /** What the server of a test is started with, beyond its broker and a free port. */
    TassisServer.Builder configured(TassisServer.Builder server) {
        return server;
    }

    /** A client of the same broker that does not go through the network. */
    JMSContext inVmContext() {
        return inVm.createContext();
    }

    /** The factory of a client of that server. */
    TassisConnectionFactory factoryFor(int port) {
        return TassisConnectionFactory.fromUrl("tassis:amqp://localhost:" + port);
    }

    @BeforeEach
    void startServer() {
        brokerName = "network-" + UUID.randomUUID();
        server = configured(TassisServer.forBroker(brokerName).port(0)).start();
        network = factoryFor(server.port());
        inVm = new TassisConnectionFactory(brokerName);
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    // ---- queues (§4.1) ----------------------------------------------------------

    @Test
    void aMessageSentThroughTheNetworkIsReceivedThroughTheNetwork() throws Exception {
        try (JMSContext context = network.createContext()) {
            Queue orders = context.createQueue("orders");
            context.createProducer().setProperty("region", "north").setPriority(7).send(orders, "first");

            TextMessage received = (TextMessage) context.createConsumer(orders).receive(SOON);

            assertEquals("first", received.getText());
            assertEquals("north", received.getStringProperty("region"));
            assertEquals(7, received.getJMSPriority());
            assertEquals(orders, received.getJMSDestination());
            assertFalse(received.getJMSRedelivered());
            assertEquals(1, received.getIntProperty("JMSXDeliveryCount"));
            assertNotNull(received.getJMSMessageID());
        }
    }

    @Test
    void aClientOfTheNetworkAndOneOfTheJvmShareTheBroker() {
        try (JMSContext far = network.createContext(); JMSContext near = inVm.createContext()) {
            far.createProducer().send(far.createQueue("to-the-jvm"), "from the network");
            assertEquals("from the network",
                    near.createConsumer(near.createQueue("to-the-jvm")).receiveBody(String.class, SOON));

            near.createProducer().send(near.createQueue("to-the-network"), "from the JVM");
            assertEquals("from the JVM",
                    far.createConsumer(far.createQueue("to-the-network")).receiveBody(String.class, SOON));
        }
    }

    @Test
    void aReceiveThatFindsNothingReturnsNothing() {
        try (JMSContext context = network.createContext()) {
            JMSConsumer consumer = context.createConsumer(context.createQueue("empty"));

            assertNull(consumer.receiveNoWait());
            assertNull(consumer.receive(BRIEFLY));

            // and what comes later is still received
            context.createProducer().send(context.createQueue("empty"), "at last");
            assertEquals("at last", consumer.receiveBody(String.class, SOON));
        }
    }

    @Test
    void aMessageNobodyAskedForStaysWithTheBroker() {
        try (JMSContext idle = network.createContext(); JMSContext busy = network.createContext()) {
            Queue orders = idle.createQueue("orders");
            // a consumer that is there and receives nothing
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
        try (JMSContext context = network.createContext()) {
            Queue orders = context.createQueue("orders");
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
    void aStoppedConnectionReceivesNothingUntilItStartsAgain() throws Exception {
        try (Connection connection = network.createConnection()) {
            Session session = connection.createSession();
            Queue orders = session.createQueue("orders");
            MessageConsumer consumer = session.createConsumer(orders);
            session.createProducer(orders).send(session.createTextMessage("held"));

            // §6.1.4: a connection is created stopped
            assertNull(consumer.receive(BRIEFLY));
            // and the message is not kept from another consumer meanwhile
            try (JMSContext other = network.createContext()) {
                assertEquals("held", other.createConsumer(other.createQueue("orders")).receiveBody(String.class, SOON));
            }

            session.createProducer(orders).send(session.createTextMessage("delivered"));
            connection.start();
            assertEquals("delivered", ((TextMessage) consumer.receive(SOON)).getText());

            // §6.1.5: a receive in progress goes on, and completes once the connection starts again
            CompletableFuture<Message> waiting = CompletableFuture.supplyAsync(() -> {
                try {
                    return consumer.receive(SOON);
                } catch (JMSException e) {
                    throw new java.lang.IllegalStateException(e);
                }
            });
            Thread.sleep(BRIEFLY);
            connection.stop();
            session.createProducer(orders).send(session.createTextMessage("after the stop"));
            Thread.sleep(BRIEFLY);
            assertFalse(waiting.isDone());
            connection.start();
            assertEquals("after the stop", ((TextMessage) waiting.get(SOON, TimeUnit.MILLISECONDS)).getText());
        }
    }

    @Test
    void everyKindOfMessageCrossesTheNetwork() throws Exception {
        try (JMSContext context = network.createContext()) {
            Queue all = context.createQueue("all");
            MapMessage map = context.createMapMessage();
            map.setInt("count", 3);
            map.setString("name", "three");
            BytesMessage bytes = context.createBytesMessage();
            bytes.writeBytes(new byte[] {1, 2, 3});
            context.createProducer().send(all, map).send(all, bytes).send(all, new ArrayList<>(List.of("serialized")))
                    .send(all, context.createMessage());
            JMSConsumer consumer = context.createConsumer(all);

            MapMessage receivedMap = (MapMessage) consumer.receive(SOON);
            assertEquals(3, receivedMap.getInt("count"));
            assertEquals("three", receivedMap.getString("name"));
            assertEquals(3, ((BytesMessage) consumer.receive(SOON)).getBodyLength());
            assertEquals(List.of("serialized"), consumer.receiveBody(List.class, SOON));
            assertNotNull(consumer.receive(SOON));
        }
    }

    @Test
    void aMessageLargerThanAFrameCrossesTheNetwork() {
        String large = "x".repeat(1_000_000);
        try (JMSContext context = network.createContext()) {
            context.createProducer().send(context.createQueue("large"), large);
            assertEquals(large, context.createConsumer(context.createQueue("large")).receiveBody(String.class, SOON));
        }
    }

    // ---- acknowledgement and transactions (§6.2.7, §6.2.10) ------------------------

    @Test
    void anUnacknowledgedMessageIsRedeliveredAfterRecover() throws Exception {
        try (JMSContext context = network.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
            Queue orders = context.createQueue("orders");
            context.createProducer().send(orders, "once").send(orders, "twice");
            JMSConsumer consumer = context.createConsumer(orders);

            assertEquals("once", consumer.receiveBody(String.class, SOON));
            context.recover();

            Message again = consumer.receive(SOON);
            assertEquals("once", again.getBody(String.class));
            assertTrue(again.getJMSRedelivered());
            assertEquals(2, again.getIntProperty("JMSXDeliveryCount"));
            assertEquals("twice", consumer.receiveBody(String.class, SOON));
            context.acknowledge();
        }
        assertEquals(0, messagesLeft("orders"));
    }

    @Test
    void whatAClosedConsumerDeliveredCanStillBeAcknowledged() throws Exception {
        try (JMSContext context = network.createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
            Queue orders = context.createQueue("orders");
            context.createProducer().send(orders, "kept");
            JMSConsumer consumer = context.createConsumer(orders);
            Message received = consumer.receive(SOON);

            // §8.8: closing a consumer does not give back what its session has not acknowledged
            consumer.close();
            assertNull(context.createConsumer(orders).receiveNoWait(), "not delivered again");
            received.acknowledge();
        }
        assertEquals(0, messagesLeft("orders"));
    }

    @Test
    void aTransactionSendsAndAcknowledgesAtCommitOnly() throws Exception {
        try (JMSContext transacted = network.createContext(JMSContext.SESSION_TRANSACTED);
                JMSContext observer = network.createContext()) {
            Queue in = transacted.createQueue("in");
            Queue out = transacted.createQueue("out");
            observer.createProducer().send(in, "work");
            JMSConsumer results = observer.createConsumer(out);

            assertEquals("work", transacted.createConsumer(in).receiveBody(String.class, SOON));
            transacted.createProducer().send(out, "result 1").send(out, "result 2");
            assertNull(results.receive(BRIEFLY), "nothing is sent before the commit");

            transacted.commit();

            assertEquals("result 1", results.receiveBody(String.class, SOON));
            assertEquals("result 2", results.receiveBody(String.class, SOON));
        }
        assertEquals(0, messagesLeft("in"));
    }

    @Test
    void aRolledBackTransactionSendsNothingAndGivesBackWhatItReceived() throws Exception {
        try (JMSContext transacted = network.createContext(JMSContext.SESSION_TRANSACTED)) {
            Queue in = transacted.createQueue("in");
            transacted.createProducer().send(in, "work");
            transacted.commit();
            JMSConsumer consumer = transacted.createConsumer(in);

            assertEquals("work", consumer.receiveBody(String.class, SOON));
            transacted.createProducer().send(transacted.createQueue("out"), "never");
            transacted.rollback();

            Message again = consumer.receive(SOON);
            assertTrue(again.getJMSRedelivered());
            transacted.commit();
        }
        assertEquals(0, messagesLeft("out"));
    }

    @Test
    void sessionsOfOneConnectionCommitAtTheSameTime() throws Exception {
        try (Connection connection = network.createConnection()) {
            // the first transactions of a connection, all at once: they share what declares them
            List<CompletableFuture<Void>> commits = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                Session session = connection.createSession(Session.SESSION_TRANSACTED);
                MessageProducer producer = session.createProducer(session.createQueue("committed"));
                producer.send(session.createTextMessage("a"));
                producer.send(session.createTextMessage("b"));
                commits.add(CompletableFuture.runAsync(() -> {
                    try {
                        session.commit();
                    } catch (JMSException e) {
                        throw new java.lang.IllegalStateException(e);
                    }
                }));
            }
            for (CompletableFuture<Void> commit : commits) {
                commit.get(SOON, TimeUnit.MILLISECONDS);
            }
        }
        assertEquals(16, messagesLeft("committed"));
    }

    private int messagesLeft(String queue) throws JMSException {
        try (Connection connection = inVm.createConnection()) {
            Session session = connection.createSession();
            try (QueueBrowser browser = session.createBrowser(session.createQueue(queue))) {
                return Collections.list(browser.getEnumeration()).size();
            }
        }
    }

    // ---- topics (§4.2, §8.3) ---------------------------------------------------------

    @Test
    void everySubscriberOfATopicGetsTheMessage() {
        try (JMSContext context = network.createContext(); JMSContext other = network.createContext()) {
            Topic news = context.createTopic("news");
            JMSConsumer first = context.createConsumer(news);
            JMSConsumer second = other.createConsumer(news, "kind = 'sport'");

            context.createProducer().setProperty("kind", "politics").send(news, "a vote");
            context.createProducer().setProperty("kind", "sport").send(news, "a match");

            assertEquals("a vote", first.receiveBody(String.class, SOON));
            assertEquals("a match", first.receiveBody(String.class, SOON));
            assertEquals("a match", second.receiveBody(String.class, SOON), "a selector keeps the rest away (§3.8)");
        }
    }

    @Test
    void aDurableSubscriptionKeepsWhatIsPublishedWhileItsConsumerIsAway() {
        TassisConnectionFactory named = network.withClientID("reader");
        try (JMSContext context = named.createContext()) {
            context.createDurableConsumer(context.createTopic("news"), "mine").close();
        }
        try (JMSContext publisher = network.createContext()) {
            publisher.createProducer().send(publisher.createTopic("news"), "while away");
        }
        try (JMSContext context = named.createContext()) {
            JMSConsumer back = context.createDurableConsumer(context.createTopic("news"), "mine");
            assertEquals("while away", back.receiveBody(String.class, SOON));
            back.close();

            // §8.3.5
            context.unsubscribe("mine");
            assertThrows(jakarta.jms.InvalidDestinationRuntimeException.class, () -> context.unsubscribe("mine"));
        }
    }

    @Test
    void aSharedSubscriptionGivesEachMessageToOneOfItsConsumers() {
        try (JMSContext one = network.createContext(); JMSContext two = network.createContext()) {
            Topic news = one.createTopic("news");
            JMSConsumer first = one.createSharedConsumer(news, "pool");
            JMSConsumer second = two.createSharedConsumer(news, "pool");

            one.createProducer().send(news, "only once");

            String byFirst = first.receiveBody(String.class, BRIEFLY);
            String bySecond = second.receiveBody(String.class, BRIEFLY);
            assertTrue(byFirst == null ^ bySecond == null, "one of them, not both: " + byFirst + ", " + bySecond);
        }
    }

    @Test
    void aSubscriberThatWantsNoLocalMessagesGetsNoneOfItsOwnConnection() {
        try (JMSContext context = network.createContext(); JMSContext other = network.createContext()) {
            Topic news = context.createTopic("news");
            JMSConsumer consumer = context.createConsumer(news, null, true);

            context.createProducer().send(news, "mine");
            other.createProducer().send(news, "theirs");

            assertEquals("theirs", consumer.receiveBody(String.class, SOON));
        }
    }

    // ---- temporary destinations and browsers (§6.2.2, §4.1.3) --------------------------

    @Test
    void aReplyComesBackToATemporaryQueue() throws Exception {
        try (JMSContext requester = network.createContext(); JMSContext replier = network.createContext()) {
            Queue requests = requester.createQueue("requests");
            TemporaryQueue replies = requester.createTemporaryQueue();
            requester.createProducer().setJMSReplyTo(replies).send(requests, "ping");

            Message request = replier.createConsumer(requests).receive(SOON);
            replier.createProducer().send(request.getJMSReplyTo(), "pong");

            assertEquals("pong", requester.createConsumer(replies).receiveBody(String.class, SOON));
        }
    }

    @Test
    void aTemporaryQueueIsDeletedOnceItHasNoConsumer() throws Exception {
        try (JMSContext context = network.createContext(); JMSContext other = network.createContext()) {
            TemporaryQueue temporary = context.createTemporaryQueue();
            JMSConsumer consumer = context.createConsumer(temporary);

            assertThrows(JMSException.class, temporary::delete, "still in use (§6.2.2)");
            consumer.close();
            temporary.delete();

            // what was a reply address is one no more
            Queue gone = other.createQueue(temporary.getQueueName());
            assertThrows(jakarta.jms.InvalidDestinationRuntimeException.class,
                    () -> other.createProducer().send(gone, "too late"));
        }
    }

    @Test
    void aBrowserSeesTheQueueAndTakesNothing() throws Exception {
        try (JMSContext context = network.createContext()) {
            Queue orders = context.createQueue("orders");
            for (int i = 0; i < 450; i++) {
                context.createProducer().setProperty("even", i % 2 == 0).send(orders, "order " + i);
            }

            try (QueueBrowser all = context.createBrowser(orders);
                    QueueBrowser even = context.createBrowser(orders, "even = TRUE")) {
                List<?> seen = Collections.list(all.getEnumeration());
                assertEquals(450, seen.size());
                assertEquals("order 0", ((TextMessage) seen.get(0)).getText());
                assertEquals("order 449", ((TextMessage) seen.get(449)).getText());
                assertEquals(225, Collections.list(even.getEnumeration()).size());
            }
            assertEquals(450, messagesLeft("orders"));
        }
    }

    // ---- the connection (§6.1) ---------------------------------------------------------

    @Test
    void aClientIdentifierBelongsToOneConnectionAtATime() throws Exception {
        try (Connection first = network.createConnection()) {
            // §6.1.2: set by the application, as the first thing it does
            first.setClientID("only-one");
            assertEquals("only-one", first.getClientID());

            assertThrows(InvalidClientIDException.class, () -> network.withClientID("only-one").createConnection());
            try (Connection second = network.createConnection()) {
                assertThrows(InvalidClientIDException.class, () -> second.setClientID("only-one"));
                // and the connection goes on without one
                second.setClientID("another");
                Session session = second.createSession();
                session.createProducer(session.createQueue("orders")).send(session.createTextMessage("still usable"));
            }
        }
        try (Connection again = network.withClientID("only-one").createConnection()) {
            assertEquals("only-one", again.getClientID(), "given back when its connection closed");
        }
    }

    @Test
    void aSecuredBrokerChecksWhoConnects() throws Exception {
        TassisBroker.named(brokerName).addUser("alice", "secret");

        assertThrows(JMSSecurityException.class, () -> network.createConnection());
        assertThrows(JMSSecurityException.class, () -> network.createConnection("alice", "wrong"));
        try (Connection connection = network.createConnection("alice", "secret")) {
            Session session = connection.createSession();
            Queue orders = session.createQueue("orders");
            session.createProducer(orders).send(session.createTextMessage("let in"));
            try (QueueBrowser browser = session.createBrowser(orders)) {
                assertEquals(1, Collections.list(browser.getEnumeration()).size());
            }
        }
    }

    @Test
    void aBrokerThatIsNotThereIsReportedAsSuch() {
        server.close();
        JMSException unreachable = assertThrows(JMSException.class, () -> network.createConnection());
        assertTrue(unreachable.getMessage().contains("cannot be reached"), unreachable.getMessage());
    }

    @Test
    void sendingToADestinationThatIsGoneFails() throws Exception {
        try (Connection connection = network.createConnection(); Connection owner = network.createConnection()) {
            Session owning = owner.createSession();
            TemporaryQueue temporary = owning.createTemporaryQueue();
            Session session = connection.createSession();
            MessageProducer producer = session.createProducer(null);
            producer.send(temporary, session.createTextMessage("in time"));

            owner.close();

            assertThrows(InvalidDestinationException.class,
                    () -> producer.send(session.createQueue(temporary.getQueueName()),
                            session.createTextMessage("too late")));
        }
    }

    @Test
    void theApplicationIsToldWhenTheConnectionIsLost() throws Exception {
        BlockingQueue<JMSException> told = new ArrayBlockingQueue<>(1);
        Connection connection = network.createConnection();
        // §6.1.7
        connection.setExceptionListener(told::add);
        Session session = connection.createSession();
        MessageConsumer consumer = session.createConsumer(session.createQueue("orders"));
        connection.start();
        CompletableFuture<Object> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                return consumer.receive();
            } catch (JMSException e) {
                return e;
            }
        });
        Thread.sleep(BRIEFLY);

        server.close();

        JMSException problem = told.poll(SOON, TimeUnit.MILLISECONDS);
        assertNotNull(problem, "the exception listener is called");
        assertTrue(problem.getMessage().contains("lost"), problem.getMessage());
        // a receive in progress does not wait for ever, and nothing more can be asked
        assertTrue(waiting.get(SOON, TimeUnit.MILLISECONDS) instanceof JMSException);
        assertThrows(JMSException.class,
                () -> session.createProducer(session.createQueue("orders")).send(session.createTextMessage("x")));
        connection.close();
    }

    @Test
    void whatAConnectionThatVanishesHadNotAcknowledgedIsDeliveredAgain() throws Exception {
        try (JMSContext sender = network.createContext()) {
            sender.createProducer().send(sender.createQueue("orders"), "not lost");
        }
        JMSContext careless = network.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
        assertEquals("not lost", careless.createConsumer(careless.createQueue("orders")).receiveBody(String.class, SOON));

        careless.close();

        try (JMSContext careful = network.createContext()) {
            Message again = careful.createConsumer(careful.createQueue("orders")).receive(SOON);
            assertEquals("not lost", again.getBody(String.class));
            assertTrue(again.getJMSRedelivered());
        }
    }
}
