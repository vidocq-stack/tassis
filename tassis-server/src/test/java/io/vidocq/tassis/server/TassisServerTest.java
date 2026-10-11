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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.amqp.internal.messaging.Accepted;
import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.messaging.AmqpValue;
import io.vidocq.tassis.amqp.internal.messaging.Data;
import io.vidocq.tassis.amqp.internal.messaging.Modified;
import io.vidocq.tassis.amqp.internal.messaging.Properties;
import io.vidocq.tassis.amqp.internal.messaging.Rejected;
import io.vidocq.tassis.amqp.internal.messaging.Released;
import io.vidocq.tassis.amqp.internal.messaging.Source;
import io.vidocq.tassis.amqp.internal.messaging.Target;
import io.vidocq.tassis.amqp.internal.transactions.Coordinator;
import io.vidocq.tassis.amqp.internal.transport.Attach;
import io.vidocq.tassis.amqp.internal.transport.Begin;
import io.vidocq.tassis.amqp.internal.transport.Close;
import io.vidocq.tassis.amqp.internal.transport.Detach;
import io.vidocq.tassis.amqp.internal.transport.Disposition;
import io.vidocq.tassis.amqp.internal.transport.End;
import io.vidocq.tassis.amqp.internal.transport.Flow;
import io.vidocq.tassis.amqp.internal.transport.Frame;
import io.vidocq.tassis.amqp.internal.transport.Open;
import io.vidocq.tassis.amqp.internal.transport.ProtocolHeader;
import io.vidocq.tassis.amqp.internal.transport.Transfer;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.Described;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.ULong;
import io.vidocq.tassis.core.TassisBroker;
import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.core.internal.broker.Broker;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A broker opened to the network with AMQP 1.0, driven frame by frame: the opening of a
 * connection (part 2 §2.2 and §2.4, part 5 §5.3), sessions (§2.5), links, credit and the
 * transfer of messages (§2.6), and what the broker does of a client that goes away.
 */
class TassisServerTest {

    private static final long SOON = 5_000;

    private String brokerName;
    private TassisServer server;
    private TassisConnectionFactory factory;

    @BeforeEach
    void startServer() {
        brokerName = "server-" + UUID.randomUUID();
        server = TassisServer.forBroker(brokerName).port(0).start();
        factory = new TassisConnectionFactory(brokerName);
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    private Wire connect() throws Exception {
        return new Wire(server.port());
    }

    private static AmqpMessage text(String text) {
        return AmqpMessage.ofBody(new AmqpValue(text));
    }

    private static String textOf(AmqpMessage message) {
        return (String) ((AmqpValue) message.body().get(0)).value();
    }

    /** Sends through the broker of the JVM, as a client of the same application would. */
    private void sendInVm(String queue, String... texts) {
        try (JMSContext context = factory.createContext()) {
            for (String text : texts) {
                context.createProducer().send(context.createQueue(queue), text);
            }
        }
    }

    private String receiveInVm(String queue) {
        try (JMSContext context = factory.createContext()) {
            return context.createConsumer(context.createQueue(queue)).receiveBody(String.class, SOON);
        }
    }

    private void awaitMessageCount(String queue, int expected) throws InterruptedException {
        Broker broker = Broker.named(brokerName);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(SOON);
        while (broker.messageCount(queue) != expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(expected, broker.messageCount(queue), "messages waiting in " + queue);
    }

    // ---- opening a connection --------------------------------------------------

    @Test
    void aConnectionIsOpenedAndClosed() throws Exception {
        try (Wire wire = connect()) {
            List<Symbol> offered = wire.saslAnonymous();
            assertEquals(List.of(Symbol.of("PLAIN"), Symbol.of("ANONYMOUS")), offered);

            Open open = wire.open("client-1");

            assertEquals("tassis-" + brokerName, open.containerId());
            assertNotNull(open.maxFrameSize());
            assertNotNull(open.idleTimeOut());
            assertTrue(open.offeredCapabilities().contains(Symbol.of("ANONYMOUS-RELAY")));

            wire.send(new Close(null));
            assertNull(wire.expect(Close.class).error());
            assertTrue(wire.closedByServer());
        }
    }

    @Test
    void theServerListensOnAPortOfItsOwnAndStopsWhenClosed() throws Exception {
        assertTrue(server.port() > 0);
        assertEquals(brokerName, server.brokerName());

        server.close();

        assertThrows(java.io.IOException.class, () -> new Wire(server.port()).saslAnonymous());
        assertThrows(IllegalArgumentException.class, () -> TassisServer.forBroker(""));
        assertThrows(IllegalArgumentException.class, () -> TassisServer.forBroker("x").port(70_000));
    }

    @Test
    void aSecuredBrokerAsksForADeclaredUser() throws Exception {
        TassisBroker.named(brokerName).addUser("alice", "secret");

        try (Wire wire = connect()) {
            assertEquals(0, wire.saslPlain("alice", "secret"));
            assertNotNull(wire.open("client").containerId());
        }
        try (Wire wire = connect()) {
            // §5.3.3.6: code 1, the credentials were refused; and nothing says which was wrong
            assertEquals(1, wire.saslPlain("alice", "wrong"));
            assertTrue(wire.closedByServer());
        }
        try (Wire wire = connect()) {
            assertEquals(1, wire.saslPlain("nobody", "secret"));
        }
    }

    @Test
    void aSecuredBrokerDoesNotOfferAnonymousUnlessItAllowsIt() throws Exception {
        TassisBroker.named(brokerName).addUser("alice", "secret");
        try (Wire wire = connect()) {
            assertEquals(ProtocolHeader.SASL, wire.exchange(ProtocolHeader.SASL));
            assertEquals(List.of(Symbol.of("PLAIN")),
                    wire.expect(io.vidocq.tassis.amqp.internal.security.SaslMechanisms.class).saslServerMechanisms());
            wire.send(Frame.sasl(new io.vidocq.tassis.amqp.internal.security.SaslInit(Symbol.of("ANONYMOUS"), null, null)));
            assertEquals(1, wire.expect(io.vidocq.tassis.amqp.internal.security.SaslOutcome.class).code());
        }

        TassisBroker.named(brokerName).allowAnonymous(true);
        try (Wire wire = connect()) {
            assertTrue(wire.saslAnonymous().contains(Symbol.of("ANONYMOUS")));
        }
    }

    @Test
    void aMechanismTheServerDoesNotKnowIsRefused() throws Exception {
        try (Wire wire = connect()) {
            wire.exchange(ProtocolHeader.SASL);
            wire.expect(io.vidocq.tassis.amqp.internal.security.SaslMechanisms.class);
            wire.send(Frame.sasl(new io.vidocq.tassis.amqp.internal.security.SaslInit(Symbol.of("SCRAM-SHA-1"), null, null)));

            assertEquals(1, wire.expect(io.vidocq.tassis.amqp.internal.security.SaslOutcome.class).code());
        }
    }

    @Test
    void aClientWithoutASecurityLayerIsAcceptedOnlyByABrokerThatAcceptsAnyone() throws Exception {
        try (Wire wire = connect()) {
            assertNotNull(wire.open("bare").containerId());
        }

        TassisBroker.named(brokerName).addUser("alice", "secret");
        try (Wire wire = connect()) {
            // §2.2: the server answers with the header of what it requires, and closes
            assertEquals(ProtocolHeader.SASL, wire.exchange(ProtocolHeader.AMQP));
            assertTrue(wire.closedByServer());
        }
    }

    @Test
    void anotherVersionOfTheProtocolIsAnsweredWithTheOneTheServerSpeaks() throws Exception {
        try (Wire wire = connect()) {
            assertEquals(ProtocolHeader.AMQP, wire.exchange(new ProtocolHeader(0, 0, 9, 1)));
            assertTrue(wire.closedByServer());
        }
        try (Wire wire = connect()) {
            assertEquals(ProtocolHeader.AMQP, wire.exchange(ProtocolHeader.TLS));
            assertTrue(wire.closedByServer());
        }
    }

    @Test
    void whatIsNotAmqpEndsTheConnection() throws Exception {
        try (Wire wire = connect()) {
            wire.write("GET / HTTP/1.1\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            assertTrue(wire.closedByServer());
        }
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            wire.open("client");
            // a frame whose body is no AMQP value
            wire.write(new byte[] {0, 0, 0, 9, 2, 0, 0, 0, (byte) 0x99});

            Close close = wire.expect(Close.class);
            assertEquals(Symbol.of("amqp:decode-error"), close.error().condition());
            assertTrue(wire.closedByServer());
        }
    }

    @Test
    void aFrameOutOfPlaceEndsTheConnectionWithTheReason() throws Exception {
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            wire.open("client");
            // an attach on a channel where no session was begun
            wire.send(new Attach("l", 0L, true, null, null, null, null, null, null, null, null, null, null, null));

            assertEquals(Symbol.of("amqp:not-allowed"), wire.expect(Close.class).error().condition());
        }
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            assertEquals(ProtocolHeader.AMQP, wire.exchange(ProtocolHeader.AMQP));
            // begin before open
            wire.send(new Begin(null, 0L, 10L, 10L, null, null, null, null));

            assertEquals(Symbol.of("amqp:not-allowed"), wire.expect(Close.class).error().condition());
        }
    }

    @Test
    void theServerSendsHeartbeatsAsOftenAsTheClientAsks() throws Exception {
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            wire.open(new Open("client", null, null, null, 200L, null, null, null, null, null));

            // §2.4.5: half the client's time-out; three of them in well under a second
            long start = System.nanoTime();
            int empty = 0;
            while (empty < 3) {
                if (wire.receive().isEmpty()) {
                    empty++;
                }
            }
            assertTrue(System.nanoTime() - start < 2_000_000_000L);
        }
    }

    @Test
    void onlyOneConnectionMayHoldAContainerIdentifierThatAsksForIt() throws Exception {
        Open sole = new Open("unique", null, null, null, null, null, null, null,
                List.of(Symbol.of("sole-connection-for-container")), null);
        try (Wire first = connect(); Wire second = connect()) {
            first.saslAnonymous();
            assertNull(first.open(sole).properties());

            second.saslAnonymous();
            Open refused = second.open(sole);

            // what a JMS client reads as InvalidClientIDException (§6.1.2)
            assertEquals(true, refused.properties().get(Symbol.of("amqp:connection-establishment-failed")));
            Close close = second.expect(Close.class);
            assertEquals(Symbol.of("amqp:invalid-field"), close.error().condition());
            assertEquals(Symbol.of("container-id"), close.error().info().get(Symbol.of("invalid-field")));
        }
        // and it is free again once its connection is gone
        try (Wire again = connect()) {
            again.saslAnonymous();
            Thread.sleep(100);
            assertNull(again.open(sole).properties());
        }
    }

    // ---- sending ----------------------------------------------------------------

    @Test
    void aMessageSentByAmqpIsReceivedInTheJvm() throws Exception {
        try (Wire wire = connect().ready()) {
            assertTrue(wire.attachSender(0, "orders") > 0, "the server grants credit");

            wire.transfer(0, 0, text("hello"));

            Disposition disposition = wire.expect(Disposition.class);
            assertEquals(new Accepted(), disposition.state());
            assertEquals(true, disposition.settled());
            assertEquals(0L, disposition.first());
            assertEquals(true, disposition.role(), "from the receiving end of the link");
        }
        try (JMSContext context = factory.createContext()) {
            Message received = context.createConsumer(context.createQueue("orders")).receive(SOON);
            assertEquals("hello", ((TextMessage) received).getText());
            assertNotNull(received.getJMSMessageID(), "the broker names a message that has no identifier");
            assertEquals(context.createQueue("orders"), received.getJMSDestination());
        }
    }

    @Test
    void aSettledTransferIsNotAnswered() throws Exception {
        try (Wire wire = connect().ready()) {
            wire.attachSender(0, "orders");

            wire.send(Frame.amqp(0, new Transfer(0L, 0L, Binary.of(1), 0L, true, null, null, null, null, null, null),
                    new Binary(text("fire and forget").encode())));

            assertTrue(wire.silent());
        }
        assertEquals("fire and forget", receiveInVm("orders"));
    }

    @Test
    void aMessageInSeveralFramesIsPutBackTogether() throws Exception {
        byte[] big = new byte[150_000];
        java.util.Arrays.fill(big, (byte) 7);
        byte[] encoded = AmqpMessage.ofBody(new Data(new Binary(big))).encode();
        try (Wire wire = connect().ready()) {
            wire.attachSender(0, "orders");

            int half = encoded.length / 2;
            wire.send(Frame.amqp(0, new Transfer(0L, 5L, Binary.of(1), 0L, false, true, null, null, null, null, null),
                    new Binary(java.util.Arrays.copyOfRange(encoded, 0, half))));
            wire.send(Frame.amqp(0, new Transfer(0L, null, null, null, null, false, null, null, null, null, null),
                    new Binary(java.util.Arrays.copyOfRange(encoded, half, encoded.length))));

            assertEquals(5L, wire.expect(Disposition.class).first());
        }
        try (JMSContext context = factory.createContext()) {
            BytesMessage received = (BytesMessage) context.createConsumer(context.createQueue("orders")).receive(SOON);
            assertEquals(150_000, received.getBodyLength());
        }
    }

    @Test
    void theServerKeepsGrantingCreditToAProducer() throws Exception {
        try (Wire wire = connect().ready()) {
            long credit = wire.attachSender(0, "orders");

            long sent = 0;
            for (int round = 0; round < 3; round++) {
                // a producer never sends more than it has credit for (§2.6.7)
                for (long i = 0; i < credit; i++, sent++) {
                    wire.send(Frame.amqp(0, new Transfer(0L, sent, Binary.of((int) sent), 0L, true, null, null, null,
                            null, null, null), new Binary(text("m" + sent).encode())));
                }
                Flow more = wire.expect(Flow.class);
                credit = more.deliveryCount() + more.linkCredit() - sent;
                assertTrue(credit > 0, "more credit before the grant ran out");
            }
            assertEquals(200, sent, "100, then twice what each new grant left");
        }
        awaitMessageCount("orders", 200);
    }

    @Test
    void aMessageWithoutALinkAddressSaysWhereItGoes() throws Exception {
        try (Wire wire = connect().ready()) {
            wire.attachSender(0, null);

            wire.transfer(0, 0, new AmqpMessage(null, null, null,
                    new Properties(null, null, "relayed", null, null, null, null, null, null, null, null, null, null), null,
                    List.of(new AmqpValue("by its to field")), null));
            assertEquals(new Accepted(), wire.expect(Disposition.class).state());

            wire.transfer(0, 1, text("to nowhere"));
            Rejected rejected = (Rejected) wire.expect(Disposition.class).state();
            assertEquals(Symbol.of("amqp:invalid-field"), rejected.error().condition());
        }
        assertEquals("by its to field", receiveInVm("relayed"));
    }

    @Test
    void aMessageNoJmsMessageCanHoldIsRejected() throws Exception {
        try (Wire wire = connect().ready()) {
            wire.attachSender(0, "orders");

            wire.transfer(0, 0, AmqpMessage.ofBody(new AmqpValue(1.5)));

            assertInstanceOf(Rejected.class, wire.expect(Disposition.class).state());
            // and the link goes on
            wire.transfer(0, 1, text("fine"));
            assertEquals(new Accepted(), wire.expect(Disposition.class).state());
        }
    }

    // ---- receiving ----------------------------------------------------------------

    @Test
    void aMessageSentInTheJvmIsReceivedByAmqp() throws Exception {
        sendInVm("orders", "hello");
        try (Wire wire = connect().ready()) {
            Attach attached = wire.attachReceiver(0, "orders");
            assertEquals(false, attached.role(), "the server is the sending end of this link");
            assertEquals(0L, attached.initialDeliveryCount());

            wire.credit(0, 0, 1, null);
            Wire.Received received = wire.delivery();

            assertEquals("hello", textOf(received.message()));
            assertEquals(0L, received.transfer().deliveryId());
            assertEquals("orders", received.message().properties().to());
            assertEquals(0L, received.message().header().deliveryCount());

            wire.send(new Disposition(true, 0L, 0L, true, new Accepted(), null));
            wire.send(new Close(null));
            wire.expect(Close.class);
        }
        awaitMessageCount("orders", 0);
        try (JMSContext context = factory.createContext()) {
            assertNull(context.createConsumer(context.createQueue("orders")).receiveNoWait(),
                    "an accepted message is consumed for good");
        }
    }

    @Test
    void nothingIsDeliveredWithoutCreditAndNoMoreThanTheCredit() throws Exception {
        sendInVm("orders", "a", "b", "c");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            assertTrue(wire.silent(), "no credit, no message");

            wire.credit(0, 0, 2, null);

            assertEquals("a", textOf(wire.delivery().message()));
            assertEquals("b", textOf(wire.delivery().message()));
            assertTrue(wire.silent(), "two credits, two messages");
        }
    }

    @Test
    void aMessageThatArrivesLaterIsDeliveredToAWaitingLink() throws Exception {
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 10, null);
            assertTrue(wire.silent());

            sendInVm("orders", "late");

            assertEquals("late", textOf(wire.delivery().message()));
        }
    }

    @Test
    void aDrainUsesTheCreditOnWhatIsThereAndGivesUpTheRest() throws Exception {
        sendInVm("orders", "only one");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");

            // §2.6.7: credit for five, to be used now
            wire.credit(0, 0, 5, true);

            assertEquals("only one", textOf(wire.delivery().message()));
            Flow drained = wire.expect(Flow.class);
            assertEquals(0L, drained.linkCredit());
            assertEquals(5L, drained.deliveryCount(), "the delivery-count moved by the whole credit");
            assertEquals(true, drained.drain());

            // a drain on a waiting link, with nothing to deliver
            wire.credit(0, 5, 3, null);
            assertTrue(wire.silent());
            wire.credit(0, 5, 3, true);
            Flow again = wire.expect(Flow.class);
            assertEquals(0L, again.linkCredit());
            assertEquals(8L, again.deliveryCount());
        }
    }

    @Test
    void creditTakenBackStopsTheDelivery() throws Exception {
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 10, null);
            assertTrue(wire.silent());

            wire.credit(0, 0, 0, null);
            Thread.sleep(100);
            sendInVm("orders", "not for this link");

            assertTrue(wire.silent());
        }
        assertEquals("not for this link", receiveInVm("orders"));
    }

    @Test
    void aLongMessageIsSentInFramesTheClientAccepts() throws Exception {
        try (JMSContext context = factory.createContext()) {
            context.createProducer().send(context.createQueue("orders"), "x".repeat(3_000));
        }
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            wire.maxFrameSize = 512;
            wire.open("small-frames");
            wire.send(new Begin(null, 0L, 1000L, 1000L, null, null, null, null));
            wire.expect(Begin.class);
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);

            // Wire.receive refuses any frame above 512 octets
            Wire.Received received = wire.delivery();

            assertEquals(3_000, textOf(received.message()).length());
            assertTrue(received.frames() >= 6, "in " + received.frames() + " frames");
        }
    }

    // ---- what becomes of a delivery ------------------------------------------------

    @Test
    void aReleasedMessageComesBackAsIfItHadNotBeenDelivered() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);
            wire.delivery();

            wire.send(new Disposition(true, 0L, 0L, true, new Released(), null));
            wire.credit(0, 1, 1, null);

            Wire.Received again = wire.delivery();
            assertEquals(0L, again.message().header().deliveryCount(), "not counted as a delivery");
            assertEquals(1L, again.transfer().deliveryId());
        }
    }

    @Test
    void aMessageWhoseDeliveryFailedIsRedelivered() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);
            wire.delivery();

            wire.send(new Disposition(true, 0L, 0L, true, new Modified(true, null, null), null));
            wire.credit(0, 1, 1, null);

            assertEquals(1L, wire.delivery().message().header().deliveryCount(), "one redelivery");
        }
    }

    @Test
    void aRejectedOrUndeliverableMessageIsNotDeliveredAgain() throws Exception {
        sendInVm("orders", "rejected", "undeliverable", "kept");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 2, null);
            wire.delivery();
            wire.delivery();

            wire.send(new Disposition(true, 0L, 0L, true,
                    new Rejected(new io.vidocq.tassis.amqp.internal.transport.AmqpError(Symbol.of("x"), null, null)), null));
            wire.send(new Disposition(true, 1L, 1L, true, new Modified(true, true, null), null));
            wire.credit(0, 2, 5, null);

            assertEquals("kept", textOf(wire.delivery().message()));
            assertTrue(wire.silent());
        }
    }

    @Test
    void aDispositionNamesARangeOfDeliveries() throws Exception {
        sendInVm("orders", "a", "b", "c");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 3, null);
            wire.delivery();
            wire.delivery();
            wire.delivery();

            // far beyond what was delivered: the server looks at what it has, not at the range
            wire.send(new Disposition(true, 0L, 4_000_000_000L, true, new Accepted(), null));
            wire.send(new Close(null));
            wire.expect(Close.class);
        }
        awaitMessageCount("orders", 0);
        assertNull(receiveNoWaitInVm("orders"));
    }

    private String receiveNoWaitInVm(String queue) {
        try (JMSContext context = factory.createContext()) {
            return context.createConsumer(context.createQueue(queue)).receiveBodyNoWait(String.class);
        }
    }

    @Test
    void aClientThatWaitsForTheServerToSettleIsAnswered() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);
            wire.delivery();

            // rcv-settle-mode second: the receiver states the outcome and waits (§2.6.12)
            wire.send(new Disposition(true, 0L, 0L, false, new Accepted(), null));

            Disposition settled = wire.expect(Disposition.class);
            assertEquals(false, settled.role(), "from the sending end");
            assertEquals(true, settled.settled());
            assertEquals(new Accepted(), settled.state());
        }
    }

    @Test
    void aClientThatAsksForSettledDeliveriesAcknowledgesNothing() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            // snd-settle-mode settled (§2.8.2)
            wire.send(new Attach("presettled", 0L, true, 1, null,
                    new Source("orders", null, null, null, null, null, null, null, null, null, null),
                    new Target(null, null, null, null, null, null, null), null, null, null, null, null, null, null));
            wire.expect(Attach.class);
            wire.credit(0, 0, 1, null);

            assertEquals(true, wire.delivery().transfer().settled());
        }
        awaitMessageCount("orders", 0);
        assertNull(receiveNoWaitInVm("orders"), "consumed as it was sent");
    }

    // ---- a client that goes away ---------------------------------------------------

    @Test
    void whatAClientThatVanishesHadNotSettledIsDeliveredAgain() throws Exception {
        sendInVm("orders", "a", "b");
        Wire wire = connect().ready();
        wire.attachReceiver(0, "orders");
        wire.credit(0, 0, 2, null);
        wire.delivery();
        wire.delivery();
        wire.send(new Disposition(true, 0L, 0L, true, new Accepted(), null));
        Thread.sleep(100);

        wire.vanish();

        awaitMessageCount("orders", 1);
        try (JMSContext context = factory.createContext()) {
            Message again = context.createConsumer(context.createQueue("orders")).receive(SOON);
            assertEquals("b", ((TextMessage) again).getText());
            assertTrue(again.getJMSRedelivered());
        }
    }

    @Test
    void detachingALinkGivesBackWhatItHadNotSettled() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);
            wire.delivery();

            wire.send(new Detach(0L, true, null));

            assertEquals(true, wire.expect(Detach.class).closed());
            awaitMessageCount("orders", 1);
            assertEquals(0, Broker.named(brokerName).consumerCount("orders"));

            // the handle is free again
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);
            assertEquals(1L, wire.delivery().message().header().deliveryCount());
        }
    }

    @Test
    void endingASessionGivesBackWhatItHadNotSettled() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);
            wire.delivery();

            wire.send(new End(null));

            wire.expect(End.class);
            awaitMessageCount("orders", 1);
            assertEquals(0, Broker.named(brokerName).consumerCount("orders"));
        }
    }

    @Test
    void stoppingTheServerEndsTheConnectionsAndKeepsTheMessages() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);
            wire.delivery();

            server.close();

            assertTrue(wire.closedByServer());
        }
        awaitMessageCount("orders", 1);
        assertEquals("a", receiveInVm("orders"));
    }

    // ---- two clients -------------------------------------------------------------

    @Test
    void twoClientsOfTheNetworkExchangeAMessage() throws Exception {
        try (Wire producer = connect().ready(); Wire consumer = connect().ready()) {
            consumer.attachReceiver(0, "orders");
            consumer.credit(0, 0, 1, null);
            producer.attachSender(0, "orders");

            producer.transfer(0, 0, text("across"));

            assertEquals("across", textOf(consumer.delivery().message()));
        }
    }

    @Test
    void severalSessionsAndLinksShareAConnection() throws Exception {
        sendInVm("first", "1");
        sendInVm("second", "2");
        try (Wire wire = connect().ready()) {
            wire.send(Frame.amqp(3, new Begin(null, 0L, 1000L, 1000L, null, null, null, null)));
            Frame begun = wire.next();
            assertEquals(3, begun.channel(), "the server answers on the client's channel");
            assertEquals(3, ((Begin) begun.body()).remoteChannel());

            wire.attachReceiver(0, "first");
            wire.attachReceiver(1, "second");
            wire.credit(1, 0, 1, null);
            assertEquals("2", textOf(wire.delivery().message()));
            wire.credit(0, 0, 1, null);
            assertEquals("1", textOf(wire.delivery().message()));

            // a second begin on a channel already in use
            wire.send(Frame.amqp(3, new Begin(null, 0L, 1000L, 1000L, null, null, null, null)));
            assertEquals(Symbol.of("amqp:not-allowed"), wire.expect(Close.class).error().condition());
        }
    }

    @Test
    void deliveriesThatWaitedForTheSessionWindowEachGetTheirOwnIdentifier() throws Exception {
        sendInVm("first", "1");
        sendInVm("second", "2");
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            wire.open("narrow-window");
            // §2.5.6: a session that accepts no transfer for now
            wire.send(new Begin(null, 0L, 0L, 1000L, null, null, null, null));
            wire.expect(Begin.class);
            wire.attachReceiver(0, "first");
            wire.attachReceiver(1, "second");
            wire.send(new Flow(0L, 0L, 0L, 1000L, 0L, 0L, 1L, null, null, null, null));
            wire.send(new Flow(0L, 0L, 0L, 1000L, 1L, 0L, 1L, null, null, null, null));
            assertTrue(wire.silent(), "credit on both links, and no window to send in");

            // the window opens: both deliveries, which were waiting, go at once
            wire.send(new Flow(0L, 10L, 0L, 1000L, null, null, null, null, null, null, null));

            Wire.Received one = wire.delivery();
            Wire.Received other = wire.delivery();
            assertEquals(java.util.Set.of(0L, 1L),
                    java.util.Set.of(one.transfer().deliveryId(), other.transfer().deliveryId()));
            assertEquals(0L, one.transfer().deliveryId(), "in the order of their identifiers");

            // and each is settled by its own identifier
            wire.send(new Disposition(true, 0L, 1L, true, new Accepted(), null));
            wire.send(new Close(null));
            wire.expect(Close.class);
        }
        awaitMessageCount("first", 0);
        awaitMessageCount("second", 0);
        assertNull(receiveNoWaitInVm("first"));
        assertNull(receiveNoWaitInVm("second"));
    }

    @Test
    void aMessageLongerThanTheSessionWindowIsSentAsTheWindowOpens() throws Exception {
        try (JMSContext context = factory.createContext()) {
            context.createProducer().send(context.createQueue("orders"), "x".repeat(3_000));
        }
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            wire.maxFrameSize = 512;
            wire.open("small-frames-small-window");
            // a window of two frames, for a message that takes more
            wire.send(new Begin(null, 0L, 2L, 1000L, null, null, null, null));
            wire.expect(Begin.class);
            wire.attachReceiver(0, "orders");
            wire.send(new Flow(0L, 2L, 0L, 1000L, 0L, 0L, 1L, null, null, null, null));

            java.io.ByteArrayOutputStream octets = new java.io.ByteArrayOutputStream();
            long received = 0;
            Transfer last;
            do {
                Frame frame = wire.next();
                last = (Transfer) frame.body();
                octets.writeBytes(frame.payload().bytes());
                received++;
                if (received % 2 == 0) {
                    assertTrue(Boolean.TRUE.equals(last.more()) ? wire.silent() : true, "no more than the window allows");
                    wire.send(new Flow(received, 2L, 0L, 1000L, null, null, null, null, null, null, null));
                }
            } while (Boolean.TRUE.equals(last.more()));

            assertTrue(received > 2);
            assertEquals(3_000, textOf(AmqpMessage.decode(octets.toByteArray())).length());
        }
    }

    // ---- beyond queues: the frames of what Qpid JMS exercises end to end in tassis-interop -----

    private static final Target NOWHERE = new Target(null, null, null, null, null, null, null);
    private static final Source NOTHING = new Source(null, null, null, null, null, null, null, null, null, null, null);

    /** Sends an attach the server must refuse, and returns the condition it gives. */
    private static Symbol refusal(Wire wire, Attach attach) throws Exception {
        wire.send(attach);
        Attach answer = wire.expect(Attach.class);
        assertEquals(attach.name(), answer.name());
        assertNull(attach.role() ? answer.source() : answer.target(), "nothing at the server's end");
        Detach detach = wire.expect(Detach.class);
        assertEquals(true, detach.closed());
        // §2.6.5: the client answers the detach, and what it had sent meanwhile is not held against it
        wire.credit(attach.handle(), 0, 1, null);
        wire.send(new Detach(attach.handle(), true, null));
        return detach.error().condition();
    }

    @Test
    void aLinkTheServerCannotServeIsRefusedAndTheSessionGoesOn() throws Exception {
        Map<Symbol, Object> badSelector = new LinkedHashMap<>();
        badSelector.put(Symbol.of("jms-selector"), new Described(ULong.of(0x0000468C00000004L), "color = = 'red'"));
        try (Wire wire = connect().ready()) {
            // a selector that is not one (Jakarta Messaging 3.1 §3.8.1)
            assertEquals(Symbol.of("amqp:invalid-field"), refusal(wire, new Attach("bad-selector", 0L, true, null, null,
                    new Source("orders", null, null, null, null, null, null, badSelector, null, null, null), NOWHERE, null,
                    null, null, null, null, null, null)));
            // a temporary destination that does not exist
            assertEquals(Symbol.of("amqp:not-found"), refusal(wire, new Attach("gone", 0L, false, null, null, NOTHING,
                    new Target("temporary-queue:nowhere", null, null, null, null, null,
                            List.of(Symbol.of("temporary-queue"))), null, null, 0L, null, null, null, null)));
            // a durable subscription nobody created, looked up to be deleted (§8.3.5)
            assertEquals(Symbol.of("amqp:not-found"), refusal(wire, new Attach("no-such-subscription", 0L, true, null,
                    null, null, NOWHERE, null, null, null, null, null, null, null)));
            // a receiving link cannot ask for a node to be created
            assertEquals(Symbol.of("amqp:not-implemented"), refusal(wire, new Attach("dynamic-source", 0L, true, null,
                    null, new Source(null, null, null, null, true, null, null, null, null, null, null), NOWHERE, null,
                    null, null, null, null, null, null)));

            // the session, and the handle, are still usable
            wire.attachReceiver(0, "orders");
        }
    }

    @Test
    void aTransactionIsDeclaredAndDischargedOnACoordinatorLink() throws Exception {
        try (Wire wire = connect().ready()) {
            // part 4, §4.2: a link to the coordinator
            wire.send(new Attach("coordinator", 0L, false, null, null, NOTHING,
                    new Coordinator(List.of(Symbol.of("amqp:local-transactions"))), null, null, 0L, null, null, null,
                    null));
            Attach attached = wire.expect(Attach.class);
            assertEquals(List.of(Symbol.of("amqp:local-transactions")), ((Coordinator) attached.target()).capabilities());
            wire.expect(Flow.class);
            wire.attachSender(1, "orders");

            // §4.3: declare, answered with the identifier of the transaction
            wire.transfer(0, 0, AmqpMessage.ofBody(new AmqpValue(new io.vidocq.tassis.amqp.internal.transactions.Declare(null))));
            Binary transaction = ((io.vidocq.tassis.amqp.internal.transactions.Declared) wire.expect(Disposition.class)
                    .state()).txnId();

            // §4.4.1: a message posted in the transaction is not there until it commits
            wire.send(Frame.amqp(0, new Transfer(1L, 1L, Binary.of(1), 0L, false, null, null,
                    new io.vidocq.tassis.amqp.internal.transactions.TransactionalState(transaction, null), null, null,
                    null), new Binary(text("in a transaction").encode())));
            assertEquals(new io.vidocq.tassis.amqp.internal.transactions.TransactionalState(transaction, new Accepted()),
                    wire.expect(Disposition.class).state());
            assertEquals(0, Broker.named(brokerName).messageCount("orders"));

            wire.transfer(0, 2, AmqpMessage.ofBody(new AmqpValue(
                    new io.vidocq.tassis.amqp.internal.transactions.Discharge(transaction, false))));
            assertEquals(new Accepted(), wire.expect(Disposition.class).state());
            assertEquals(1, Broker.named(brokerName).messageCount("orders"));

            // discharged once: its identifier means nothing any more
            wire.transfer(0, 3, AmqpMessage.ofBody(new AmqpValue(
                    new io.vidocq.tassis.amqp.internal.transactions.Discharge(transaction, false))));
            Rejected unknown = (Rejected) wire.expect(Disposition.class).state();
            assertEquals(Symbol.of("amqp:transaction:unknown-id"), unknown.error().condition());
        }
    }

    @Test
    void aTransactionWhoseCoordinatorLinkGoesIsRolledBack() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            wire.send(new Attach("coordinator", 0L, false, null, null, NOTHING, new Coordinator(null), null, null, 0L,
                    null, null, null, null));
            wire.expect(Attach.class);
            wire.expect(Flow.class);
            wire.transfer(0, 0, AmqpMessage.ofBody(new AmqpValue(new io.vidocq.tassis.amqp.internal.transactions.Declare(null))));
            Binary transaction = ((io.vidocq.tassis.amqp.internal.transactions.Declared) wire.expect(Disposition.class)
                    .state()).txnId();
            wire.attachReceiver(1, "orders");
            wire.credit(1, 0, 1, null);
            long delivered = wire.delivery().transfer().deliveryId();
            // §4.4.2: accepted, if the transaction commits
            wire.send(new Disposition(true, delivered, delivered, true,
                    new io.vidocq.tassis.amqp.internal.transactions.TransactionalState(transaction, new Accepted()), null));
            Thread.sleep(100);
            assertEquals(0, Broker.named(brokerName).messageCount("orders"));

            // §4.2: the link closes with the transaction undischarged
            wire.send(new Detach(0L, true, null));
            wire.expect(Detach.class);

            awaitMessageCount("orders", 1);
        }
    }

    @Test
    void aBrowserIsSentTheQueueAndToldWhenItHasSeenItAll() throws Exception {
        sendInVm("orders", "a", "b");
        try (Wire wire = connect().ready()) {
            wire.send(new Attach("browser", 0L, true, null, null, new Source("orders", null, null, null, null, null,
                    Symbol.of("copy"), null, null, null, null), NOWHERE, null, null, null, null, null, null, null));
            wire.expect(Attach.class);

            wire.credit(0, 0, 10, true);

            Wire.Received first = wire.delivery();
            assertEquals("a", textOf(first.message()));
            assertEquals(true, first.transfer().settled(), "nothing to acknowledge in what is only looked at");
            assertEquals("b", textOf(wire.delivery().message()));
            Flow drained = wire.expect(Flow.class);
            assertEquals(0L, drained.linkCredit());
            assertEquals(10L, drained.deliveryCount());
        }
        assertEquals(2, Broker.named(brokerName).messageCount("orders"), "a browser consumes nothing");
    }

    @Test
    void aBrowserSendsNoMoreFramesThanTheWindowAllowsAndRemembersADrain() throws Exception {
        sendInVm("orders", "x".repeat(3_000), "b");
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            wire.maxFrameSize = 512;
            wire.open("browser-small-window");
            // a window of two frames, for a first message that takes more (§2.5.6)
            wire.send(new Begin(null, 0L, 2L, 1000L, null, null, null, null));
            wire.expect(Begin.class);
            wire.send(new Attach("browser", 0L, true, null, null, new Source("orders", null, null, null, null, null,
                    Symbol.of("copy"), null, null, null, null), NOWHERE, null, null, null, null, null, null, null));
            wire.expect(Attach.class);
            // the drain is asked once, long before the queue has been seen to its end (§2.6.7)
            wire.send(new Flow(0L, 2L, 0L, 1000L, 0L, 0L, 10L, null, true, null, null));

            java.io.ByteArrayOutputStream octets = new java.io.ByteArrayOutputStream();
            long received = 0;
            Transfer last;
            do {
                Frame frame = wire.next();
                last = (Transfer) frame.body();
                octets.writeBytes(frame.payload().bytes());
                received++;
                if (received % 2 == 0) {
                    assertTrue(wire.silent(), "no more than the window allows");
                    wire.send(new Flow(received, 2L, 0L, 1000L, null, null, null, null, null, null, null));
                }
            } while (Boolean.TRUE.equals(last.more()));
            assertTrue(received > 2);
            assertEquals(3_000, textOf(AmqpMessage.decode(octets.toByteArray())).length());

            wire.send(new Flow(received, 100L, 0L, 1000L, null, null, null, null, null, null, null));
            assertEquals("b", textOf(wire.delivery().message()));
            Flow drained = wire.expect(Flow.class);
            assertEquals(0L, drained.linkCredit());
            assertEquals(10L, drained.deliveryCount());
        }
    }

    @Test
    void aTemporaryDestinationThatIsGoneIsNotMadeAQueue() throws Exception {
        try (Wire wire = connect().ready()) {
            // §3.5.4: a dynamic target creates it, and closing the link deletes it
            wire.send(new Attach("temporary", 0L, false, null, null, NOTHING,
                    new Target(null, null, null, null, true, null, List.of(Symbol.of("temporary-queue"))), null, null,
                    0L, null, null, null, null));
            String created = (String) ((Target) wire.expect(Attach.class).target()).address();
            wire.expect(Flow.class);
            wire.send(new Detach(0L, true, null));
            assertNull(wire.expect(Detach.class).error());

            // named by a link that does not say it is a temporary destination
            assertEquals(Symbol.of("amqp:not-found"), refusal(wire, new Attach("late", 0L, false, null, null, NOTHING,
                    new Target(created, null, null, null, null, null, null), null, null, 0L, null, null, null, null)));
            assertEquals(Symbol.of("amqp:not-found"), refusal(wire, new Attach("late-reader", 0L, true, null, null,
                    new Source(created, null, null, null, null, null, null, null, null, null, null), NOWHERE, null, null,
                    null, null, null, null, null)));

            // named by a message, as the reply to a request whose sender is gone (§6.2.2)
            wire.attachSender(1, null);
            wire.transfer(1, 0, new AmqpMessage(null, null, null,
                    new Properties(null, null, created, null, null, null, null, null, null, null, null, null, null), null,
                    List.of(new AmqpValue("a late reply")), null));
            Rejected rejected = (Rejected) wire.expect(Disposition.class).state();
            assertEquals(Symbol.of("amqp:not-found"), rejected.error().condition());
            assertEquals(0, Broker.named(brokerName).messageCount(created));
        }
    }

    @Test
    void anOutcomeInATransactionThatIsOverConsumesNothing() throws Exception {
        sendInVm("orders", "a");
        try (Wire wire = connect().ready()) {
            wire.attachReceiver(0, "orders");
            wire.credit(0, 0, 1, null);
            long delivered = wire.delivery().transfer().deliveryId();

            // part 4, §4.4.2: accepted in a transaction nobody declared
            wire.send(new Disposition(true, delivered, delivered, true,
                    new io.vidocq.tassis.amqp.internal.transactions.TransactionalState(Binary.of(9, 9), new Accepted()),
                    null));

            awaitMessageCount("orders", 1);
        }
    }

    @Test
    void aDurableSubscriptionIsLookedUpInTheScopeItWasMadeIn() throws Exception {
        try (Wire wire = connect()) {
            wire.saslAnonymous();
            wire.open(new Open("a-client-identifier", null, null, null, null, null, null, null,
                    List.of(Symbol.of("sole-connection-for-container")), null));
            wire.send(new Begin(null, 0L, 1000L, 1000L, null, null, null, null));
            wire.expect(Begin.class);
            // durable, and not global: it is the subscription of this client identifier (§8.3.3)
            wire.send(new Attach("news-for-me", 0L, true, null, null, new Source("news", 2L, Symbol.of("never"), null,
                    null, null, null, null, null, null, List.of(Symbol.of("topic"))), NOWHERE, null, null, null, null,
                    null, null, null));
            wire.expect(Attach.class);
            wire.send(new Detach(0L, false, null));
            wire.expect(Detach.class);
            assertEquals(1, Broker.named(brokerName).subscriptionCount("news"));

            // what follows the name of the subscription in the name of a link does not change its scope
            wire.send(new Attach("news-for-me|2", 0L, true, null, null, null, NOWHERE, null, null, null, null, null,
                    null, null));
            assertEquals("news", ((Source) wire.expect(Attach.class).source()).address());
            wire.send(new Detach(0L, true, null));
            assertNull(wire.expect(Detach.class).error());
            assertEquals(0, Broker.named(brokerName).subscriptionCount("news"));
        }
    }

    @Test
    void aClientMayNotHoldTransactionsWithoutEnd() throws Exception {
        try (Wire wire = connect().ready()) {
            wire.send(new Attach("coordinator", 0L, false, null, null, NOTHING, new Coordinator(null), null, null, 0L,
                    null, null, null, null));
            wire.expect(Attach.class);
            wire.expect(Flow.class);
            Object outcome = null;
            for (int declared = 0; declared <= 1024; declared++) {
                wire.transfer(0, declared,
                        AmqpMessage.ofBody(new AmqpValue(new io.vidocq.tassis.amqp.internal.transactions.Declare(null))));
                outcome = outcome(wire);
            }
            Rejected rejected = (Rejected) outcome;
            assertEquals(Symbol.of("amqp:resource-limit-exceeded"), rejected.error().condition());
        }
    }

    /** What the server says of the message it was just sent, past the credit it may grant meanwhile. */
    private static Object outcome(Wire wire) throws Exception {
        Frame answer = wire.next();
        while (answer.body() instanceof Flow) {
            answer = wire.next();
        }
        return ((Disposition) answer.body()).state();
    }

    @Test
    void aTransactionHoldsNoMoreThanItsLimitOfMessages() throws Exception {
        byte[] encoded = AmqpMessage.ofBody(new Data(new Binary(new byte[15 * 1024 * 1024]))).encode();
        try (Wire wire = connect().ready()) {
            wire.send(new Attach("coordinator", 0L, false, null, null, NOTHING, new Coordinator(null), null, null, 0L,
                    null, null, null, null));
            wire.expect(Attach.class);
            wire.expect(Flow.class);
            wire.attachSender(1, "orders");
            wire.transfer(0, 0, AmqpMessage.ofBody(new AmqpValue(new io.vidocq.tassis.amqp.internal.transactions.Declare(null))));
            Binary transaction = ((io.vidocq.tassis.amqp.internal.transactions.Declared) outcome(wire)).txnId();

            // five messages of 15 MiB, for the 64 MiB a transaction may hold
            Object outcome = null;
            for (int sent = 1; sent <= 5; sent++) {
                for (int from = 0; from < encoded.length; from += 200_000) {
                    int to = Math.min(encoded.length, from + 200_000);
                    boolean first = from == 0;
                    wire.send(Frame.amqp(0, new Transfer(1L, first ? (long) sent : null, first ? Binary.of(sent) : null,
                            first ? 0L : null, first ? false : null, to < encoded.length, null,
                            new io.vidocq.tassis.amqp.internal.transactions.TransactionalState(transaction, null), null,
                            null, null), new Binary(java.util.Arrays.copyOfRange(encoded, from, to))));
                }
                Object answered = outcome(wire);
                if (sent < 5) {
                    assertEquals(new io.vidocq.tassis.amqp.internal.transactions.TransactionalState(transaction,
                            new Accepted()), answered);
                }
                outcome = answered;
            }
            Rejected rejected = (Rejected) outcome;
            assertEquals(Symbol.of("amqp:resource-limit-exceeded"), rejected.error().condition());
        }
    }
}
