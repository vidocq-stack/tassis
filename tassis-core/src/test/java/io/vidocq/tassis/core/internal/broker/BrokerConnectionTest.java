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
package io.vidocq.tassis.core.internal.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.core.TassisQueue;
import io.vidocq.tassis.core.TassisTopic;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Consumer;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Delivery;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Outgoing;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Subscribing;
import io.vidocq.tassis.core.internal.message.Messages;
import io.vidocq.tassis.core.internal.message.TassisTextMessage;
import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The contract between a client connection and its broker, exercised directly: what the broker
 * keeps for a connection, and what it does with it when the connection closes with things left
 * undone, which is what a lost connection looks like (Jakarta Messaging 3.1 §6.1.8, §6.2.10,
 * §6.2.11).
 */
class BrokerConnectionTest {

    private static final long NOT_AT_ALL = 0;
    private static final long SOON = TimeUnit.SECONDS.toNanos(5);

    private String name;
    private Broker broker;
    private BrokerConnection connection;
    private TassisConnectionFactory factory;

    @BeforeEach
    void connect() throws JMSException {
        name = "seam-" + UUID.randomUUID();
        broker = Broker.named(name);
        connection = broker.connect(null, null);
        connection.start();
        factory = new TassisConnectionFactory(name);
    }

    @AfterEach
    void disconnect() {
        connection.close();
    }

    // ---- helpers -----------------------------------------------------------

    private Outgoing toQueue(String queue, String text) throws JMSException {
        return outgoing(queue, false, false, text);
    }

    private Outgoing outgoing(String destination, boolean topic, boolean temporary, String text)
            throws JMSException {
        TassisTextMessage message = new TassisTextMessage();
        message.setText(text);
        message.setJMSMessageID(connection.nextMessageId());
        return new Outgoing(destination, topic, temporary, Messages.snapshot(message));
    }

    private void sendToQueue(String queue, String... texts) throws JMSException {
        for (String text : texts) {
            connection.send(toQueue(queue, text));
        }
    }

    private static String text(Delivery delivery) throws JMSException {
        return delivery == null ? null : ((TextMessage) delivery.message()).getText();
    }

    /** What a new client finds on the queue: text, then whether it is a redelivery. */
    private List<String> whatAnotherClientReceives(String queue) throws JMSException {
        List<String> received = new java.util.ArrayList<>();
        try (Connection other = factory.createConnection()) {
            other.start();
            MessageConsumer consumer = other.createSession().createConsumer(new TassisQueue(queue));
            for (Message message = consumer.receiveNoWait(); message != null; message = consumer.receiveNoWait()) {
                received.add(((TextMessage) message).getText() + (message.getJMSRedelivered() ? " again, delivery "
                        + message.getIntProperty("JMSXDeliveryCount") : ""));
            }
        }
        return received;
    }

    // ---- what the broker keeps of a delivery --------------------------------

    @Test
    void unsettledDeliveriesOfAClosedConnectionGoBackToTheirPlace() throws JMSException {
        sendToQueue("orders", "a", "b", "c");
        Consumer consumer = connection.attach("orders", false, null);
        assertEquals("a", text(consumer.receive(NOT_AT_ALL, false)));
        assertEquals("b", text(consumer.receive(NOT_AT_ALL, false)));

        connection.close();

        assertEquals(List.of("a again, delivery 2", "b again, delivery 2", "c"), whatAnotherClientReceives("orders"));
    }

    @Test
    void settledDeliveriesDoNotComeBack() throws JMSException {
        sendToQueue("orders", "a", "b", "c", "d");
        Consumer consumer = connection.attach("orders", false, null);
        Delivery a = consumer.receive(NOT_AT_ALL, false);
        Delivery b = consumer.receive(NOT_AT_ALL, false);
        assertEquals("c", text(consumer.receive(NOT_AT_ALL, true)), "settled as it is delivered");

        assertTrue(connection.settle(a));
        connection.settle(List.of(b));
        assertFalse(connection.settle(a), "a delivery is settled once");
        connection.close();

        assertEquals(List.of("d"), whatAnotherClientReceives("orders"));
    }

    @Test
    void aDeliveryGivenBackIsNoLongerTheConnections() throws JMSException {
        sendToQueue("orders", "a", "b");
        Consumer consumer = connection.attach("orders", false, null);
        Delivery a = consumer.receive(NOT_AT_ALL, false);
        Delivery b = consumer.receive(NOT_AT_ALL, false);

        connection.returnUndelivered(a);
        connection.redeliver(List.of(b));
        connection.redeliver(List.of(b));
        connection.returnUndelivered(a);
        assertFalse(connection.settle(a));

        assertEquals(2, broker.messageCount("orders"), "giving back twice must not duplicate a message");
        connection.close();
        assertEquals(List.of("a", "b again, delivery 2"), whatAnotherClientReceives("orders"));
    }

    @Test
    void aStoppedConnectionIsDeliveredNothing() throws JMSException {
        sendToQueue("orders", "a");
        Consumer consumer = connection.attach("orders", false, null);

        connection.stop();
        assertFalse(connection.isStarted());
        assertNull(consumer.receive(NOT_AT_ALL, true));

        assertTrue(connection.start(), "delivery resumes");
        assertFalse(connection.start(), "starting a started connection changes nothing");
        assertEquals("a", text(consumer.receive(NOT_AT_ALL, true)));
    }

    // ---- transactions -------------------------------------------------------

    @Test
    void aCommitSendsAndSettlesTogether() throws JMSException {
        sendToQueue("in", "a");
        Delivery a = connection.attach("in", false, null).receive(NOT_AT_ALL, false);

        connection.commit(List.of(toQueue("out", "x"), toQueue("out", "y")), List.of(a));
        connection.close();

        assertEquals(List.of(), whatAnotherClientReceives("in"));
        assertEquals(List.of("x", "y"), whatAnotherClientReceives("out"));
    }

    @Test
    void aCommitThatCannotSendChangesNothing() throws JMSException {
        sendToQueue("in", "a");
        Delivery a = connection.attach("in", false, null).receive(NOT_AT_ALL, false);
        String gone = connection.createTemporaryQueue();
        connection.deleteTemporaryQueue(gone);

        assertThrows(InvalidDestinationException.class, () -> connection
                .commit(List.of(toQueue("out", "x"), outgoing(gone, false, true, "y")), List.of(a)));

        assertEquals(0, broker.messageCount("out"), "none of the sends was made");
        connection.redeliver(List.of(a));
        assertEquals(1, broker.messageCount("in"), "the delivery was still the connection's to give back");
    }

    // ---- what a closing connection leaves behind ----------------------------

    @Test
    void closingEndsAReceiveInProgress() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        CompletableFuture<Delivery> waiting = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                waiting.complete(consumer.receive(BrokerConnection.FOREVER, false));
            } catch (JMSException | RuntimeException e) {
                waiting.completeExceptionally(e);
            }
        });
        // Long enough for the receive to be waiting; were it not, it would find the consumer closed.
        Thread.sleep(50);

        connection.close();

        assertNull(waiting.get(5, TimeUnit.SECONDS));
        assertEquals(0, broker.consumerCount("orders"));
    }

    @Test
    void closingEndsNonDurableSubscriptionsAndKeepsDurableOnes() throws JMSException {
        connection.claimClientId("desk");
        Consumer durable = connection.subscribe(new Subscribing("news", false, "archive", true, false, false, null));
        connection.subscribe(new Subscribing("news", false, null, false, false, false, null));
        connection.send(outgoing("news", true, false, "a"));
        connection.send(outgoing("news", true, false, "b"));
        assertEquals("a", text(durable.receive(NOT_AT_ALL, false)));

        connection.close();

        assertEquals(1, broker.subscriptionCount("news"));
        try (Connection again = factory.withClientID("desk").createConnection()) {
            again.start();
            MessageConsumer consumer =
                    again.createSession().createDurableSubscriber(new TassisTopic("news"), "archive");
            Message a = consumer.receiveNoWait();
            assertEquals("a", ((TextMessage) a).getText());
            assertTrue(a.getJMSRedelivered());
            assertEquals("b", ((TextMessage) consumer.receiveNoWait()).getText());
        }
    }

    @Test
    void closingReleasesTheClientIdentifier() throws JMSException {
        connection.claimClientId("desk");
        BrokerConnection other = broker.connect(null, null);
        assertThrows(InvalidClientIDException.class, () -> other.claimClientId("desk"));

        connection.close();

        other.claimClientId("desk");
        other.close();
    }

    @Test
    void closingDeletesTheTemporaryDestinationsOfTheConnection() throws JMSException {
        String queue = connection.createTemporaryQueue();
        String topic = connection.createTemporaryTopic();
        BrokerConnection other = broker.connect(null, null);
        assertTrue(other.hasTemporaryQueue(queue));
        assertTrue(other.hasTemporaryTopic(topic));

        connection.close();

        assertFalse(other.hasTemporaryQueue(queue));
        assertFalse(other.hasTemporaryTopic(topic));
        other.close();
    }

    @Test
    void aClosedConnectionRefusesWhatWouldLeaveSomethingBehind() throws JMSException {
        sendToQueue("orders", "a");
        Delivery a = connection.attach("orders", false, null).receive(NOT_AT_ALL, false);

        connection.close();
        connection.close();

        assertThrows(IllegalStateException.class, () -> connection.attach("orders", false, null));
        assertThrows(IllegalStateException.class,
                () -> connection.subscribe(new Subscribing("news", false, null, false, false, false, null)));
        assertThrows(IllegalStateException.class, () -> connection.createTemporaryQueue());
        assertThrows(IllegalStateException.class, () -> connection.createTemporaryTopic());
        assertThrows(IllegalStateException.class, () -> connection.claimClientId("late"));
        assertThrows(IllegalStateException.class, () -> connection.send(toQueue("orders", "late")));
        assertThrows(IllegalStateException.class, () -> connection.commit(List.of(toQueue("orders", "late")), List.of()));
        assertFalse(connection.start());
        // Already given back by the close: neither of these may put it there a second time.
        connection.redeliver(List.of(a));
        connection.returnUndelivered(a);
        assertEquals(1, broker.messageCount("orders"));
    }

    // ---- who is who ----------------------------------------------------------

    @Test
    void noLocalIgnoresWhatItsOwnConnectionPublishes() throws JMSException {
        Consumer subscriber = connection.subscribe(new Subscribing("news", false, null, false, false, true, null));
        BrokerConnection other = broker.connect(null, null);

        connection.send(outgoing("news", true, false, "mine"));
        other.send(outgoing("news", true, false, "theirs"));

        assertEquals("theirs", text(subscriber.receive(NOT_AT_ALL, true)));
        assertNull(subscriber.receive(NOT_AT_ALL, true));
        other.close();
    }

    @Test
    void aListeningConsumerIsHandedMessagesUntilItStopsListening() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        long period = consumer.listen(true);
        sendToQueue("orders", "a");
        assertEquals("a", text(consumer.take(period)));

        CompletableFuture<Delivery> waiting = taking(consumer, period);
        consumer.listen(false);

        assertNull(waiting.get(5, TimeUnit.SECONDS));
        sendToQueue("orders", "b");
        assertNull(consumer.take(period), "a consumer that does not listen is handed nothing");
        assertEquals("b", text(consumer.receive(SOON, true)));
    }

    @Test
    void whoWaitedForAListenerThatWentIsNotServedByTheNextOne() throws Exception {
        // A listener removed and another set at once: the thread that served the first must not
        // stay to serve the second beside the thread started for it.
        Consumer consumer = connection.attach("orders", false, null);
        long first = consumer.listen(true);
        CompletableFuture<Delivery> waiting = taking(consumer, first);
        Thread.sleep(50);

        consumer.listen(false);
        long second = consumer.listen(true);

        assertNull(waiting.get(5, TimeUnit.SECONDS));
        assertEquals(second, consumer.listen(true), "replacing a listener by another keeps the period going");
        sendToQueue("orders", "a");
        assertNull(consumer.take(first));
        assertEquals("a", text(consumer.take(second)));
    }

    @Test
    void aClosedConsumerHandsNothingToWhoListens() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        long period = consumer.listen(true);
        CompletableFuture<Delivery> waiting = taking(consumer, period);

        consumer.close();

        assertNull(waiting.get(5, TimeUnit.SECONDS));
        assertNull(consumer.take(period));
    }

    private static CompletableFuture<Delivery> taking(Consumer consumer, long period) {
        CompletableFuture<Delivery> waiting = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                waiting.complete(consumer.take(period));
            } catch (JMSException | RuntimeException e) {
                waiting.completeExceptionally(e);
            }
        });
        return waiting;
    }

    @Test
    void connectingChecksTheCredentials() throws JMSException {
        broker.users().add("alice", "secret");

        assertThrows(JMSSecurityException.class, () -> broker.connect("alice", "wrong"));
        assertThrows(JMSSecurityException.class, () -> broker.connect(null, null));
        broker.connect("alice", "secret").close();
    }

    @Test
    void theClientApiGoesThroughTheSameContract() throws JMSException {
        // A client that vanishes is, for the broker, a connection closed with work left undone.
        try (JMSContext sender = factory.createContext()) {
            sender.createProducer().send(sender.createQueue("orders"), "a");
        }
        Consumer consumer = connection.attach("orders", false, null);
        assertNotNull(consumer.receive(NOT_AT_ALL, false));
        connection.close();

        try (JMSContext receiver = factory.createContext(Session.AUTO_ACKNOWLEDGE)) {
            Message again = receiver.createConsumer(receiver.createQueue("orders")).receiveNoWait();
            assertTrue(again.getJMSRedelivered());
        }
    }
}
