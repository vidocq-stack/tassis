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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.vidocq.tassis.amqp.internal.AmqpTypes;
import io.vidocq.tassis.amqp.internal.messaging.Accepted;
import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.messaging.AmqpSequence;
import io.vidocq.tassis.amqp.internal.messaging.AmqpValue;
import io.vidocq.tassis.amqp.internal.messaging.ApplicationProperties;
import io.vidocq.tassis.amqp.internal.messaging.Data;
import io.vidocq.tassis.amqp.internal.messaging.DeleteOnClose;
import io.vidocq.tassis.amqp.internal.messaging.DeleteOnNoLinks;
import io.vidocq.tassis.amqp.internal.messaging.DeleteOnNoLinksOrMessages;
import io.vidocq.tassis.amqp.internal.messaging.DeleteOnNoMessages;
import io.vidocq.tassis.amqp.internal.messaging.DeliveryAnnotations;
import io.vidocq.tassis.amqp.internal.messaging.Footer;
import io.vidocq.tassis.amqp.internal.messaging.Header;
import io.vidocq.tassis.amqp.internal.messaging.MessageAnnotations;
import io.vidocq.tassis.amqp.internal.messaging.Modified;
import io.vidocq.tassis.amqp.internal.messaging.Properties;
import io.vidocq.tassis.amqp.internal.messaging.Received;
import io.vidocq.tassis.amqp.internal.messaging.Rejected;
import io.vidocq.tassis.amqp.internal.messaging.Released;
import io.vidocq.tassis.amqp.internal.messaging.Source;
import io.vidocq.tassis.amqp.internal.messaging.Target;
import io.vidocq.tassis.amqp.internal.security.SaslChallenge;
import io.vidocq.tassis.amqp.internal.security.SaslInit;
import io.vidocq.tassis.amqp.internal.security.SaslMechanisms;
import io.vidocq.tassis.amqp.internal.security.SaslOutcome;
import io.vidocq.tassis.amqp.internal.security.SaslResponse;
import io.vidocq.tassis.amqp.internal.transactions.Coordinator;
import io.vidocq.tassis.amqp.internal.transactions.Declare;
import io.vidocq.tassis.amqp.internal.transactions.Declared;
import io.vidocq.tassis.amqp.internal.transactions.Discharge;
import io.vidocq.tassis.amqp.internal.transactions.TransactionalState;
import io.vidocq.tassis.amqp.internal.transport.AmqpError;
import io.vidocq.tassis.amqp.internal.transport.Attach;
import io.vidocq.tassis.amqp.internal.transport.Begin;
import io.vidocq.tassis.amqp.internal.transport.Close;
import io.vidocq.tassis.amqp.internal.transport.Detach;
import io.vidocq.tassis.amqp.internal.transport.Disposition;
import io.vidocq.tassis.amqp.internal.transport.End;
import io.vidocq.tassis.amqp.internal.transport.Flow;
import io.vidocq.tassis.amqp.internal.transport.Open;
import io.vidocq.tassis.amqp.internal.transport.Transfer;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.AmqpDecoder;
import io.vidocq.tassis.amqp.internal.types.AmqpEncoder;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.Timestamp;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.security.SaslCode;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.Role;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.junit.jupiter.api.Test;

/**
 * The described types of AMQP 1.0, ours against Proton-J's. For each type, a value with every
 * field set is written by Tassis and read by Proton-J, whose accessors, <em>named after the
 * fields of the specification</em>, must each return what Tassis put at that position: this is
 * what catches two fields in the wrong order, which a round trip through Tassis alone cannot
 * see. Then Proton-J writes the value back and Tassis must read what it started from.
 */
class DescribedTypesInteropTest {

    private static final Map<Symbol, Object> SOME_FIELDS = fields("k", "v");
    private static final List<Symbol> SYMBOLS = List.of(Symbol.of("one"), Symbol.of("two"));
    private static final AmqpError ERROR =
            new AmqpError(Symbol.of("amqp:not-found"), "no such node", fields("where", "here"));

    private static Map<Symbol, Object> fields(String key, Object value) {
        Map<Symbol, Object> fields = new LinkedHashMap<>();
        fields.put(Symbol.of(key), value);
        return fields;
    }

    private static Map<Object, Object> map(Object key, Object value) {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    /** What Proton-J returns as one of its own enumerations, as the value that travels. */
    private static Object wireValue(Object fromProton) {
        return switch (fromProton) {
            case Role role -> role == Role.RECEIVER;
            case SenderSettleMode mode -> mode.getValue();
            case ReceiverSettleMode mode -> mode.getValue();
            case TerminusDurability durability -> durability.getValue();
            case TerminusExpiryPolicy policy -> policy.getPolicy();
            case SaslCode code -> code.getValue();
            case null, default -> fromProton;
        };
    }

    /**
     * Checks one type.
     *
     * @param ours the value, every field set
     * @param accessors the accessors of Proton-J's type, in the order the specification lists
     *        the fields: accessor {@code i} must return what Tassis wrote at position {@code i}
     */
    @SafeVarargs
    private static <P> void check(DescribedType ours, Class<P> protonType, Function<P, Object>... accessors)
            throws AmqpDecodeException {
        String name = ours.descriptorName().toString();
        Object read = Proton.decode(AmqpEncoder.encode(ours));
        P proton = protonType.cast(assertInstanceOf(protonType, read, name));

        List<?> written = (List<?>) ours.describedValue();
        assertEquals(written.size(), accessors.length, name + ": one accessor per field");
        for (int i = 0; i < accessors.length; i++) {
            Object field = wireValue(accessors[i].apply(proton));
            assertEquals(written.get(i), AmqpDecoder.decode(Proton.encode(field), AmqpTypes.ALL),
                    name + ", field " + i);
        }
        assertEquals(ours, AmqpDecoder.decode(Proton.encode(proton), AmqpTypes.ALL), name + " written back by Proton-J");
    }

    /** For the types that are not a list of fields: one value, which Proton-J calls {@code getValue}. */
    private static <P> void checkValue(DescribedType ours, Class<P> protonType, Function<P, Object> value)
            throws AmqpDecodeException {
        String name = ours.descriptorName().toString();
        P proton = protonType.cast(assertInstanceOf(protonType, Proton.decode(AmqpEncoder.encode(ours)), name));

        assertEquals(ours.describedValue(), AmqpDecoder.decode(Proton.encode(value.apply(proton)), AmqpTypes.ALL), name);
        assertEquals(ours, AmqpDecoder.decode(Proton.encode(proton), AmqpTypes.ALL), name + " written back by Proton-J");
    }

    private static Source source() {
        return new Source("orders", 2L, Symbol.of("never"), 30L, true, fields("lifetime-policy", new DeleteOnClose()),
                Symbol.of("copy"), fields("f", "x"), new Released(), SYMBOLS, List.of(Symbol.of("queue")));
    }

    private static Target target() {
        return new Target("replies", 1L, Symbol.of("link-detach"), 31L, false, SOME_FIELDS, List.of(Symbol.of("topic")));
    }

    // ---- part 2: transport ---------------------------------------------------

    @Test
    void transportPerformatives() throws AmqpDecodeException {
        check(new Open("container", "host", 65_536L, 255, 30_000L, SYMBOLS, List.of(Symbol.of("en-US")),
                List.of(Symbol.of("ANONYMOUS-RELAY")), List.of(Symbol.of("sole-connection-for-container")), SOME_FIELDS),
                org.apache.qpid.proton.amqp.transport.Open.class, p -> p.getContainerId(), p -> p.getHostname(),
                p -> p.getMaxFrameSize(), p -> p.getChannelMax(), p -> p.getIdleTimeOut(), p -> p.getOutgoingLocales(),
                p -> p.getIncomingLocales(), p -> p.getOfferedCapabilities(), p -> p.getDesiredCapabilities(),
                p -> p.getProperties());
        check(new Begin(7, 1L, 2L, 3L, 4L, SYMBOLS, List.of(Symbol.of("d")), SOME_FIELDS),
                org.apache.qpid.proton.amqp.transport.Begin.class, p -> p.getRemoteChannel(), p -> p.getNextOutgoingId(),
                p -> p.getIncomingWindow(), p -> p.getOutgoingWindow(), p -> p.getHandleMax(),
                p -> p.getOfferedCapabilities(), p -> p.getDesiredCapabilities(), p -> p.getProperties());
        check(new Attach("link", 5L, true, 1, 1, source(), target(), map(Binary.of(1), new Accepted()), true, 9L,
                ULong.of(1_000_000), SYMBOLS, List.of(Symbol.of("d")), SOME_FIELDS),
                org.apache.qpid.proton.amqp.transport.Attach.class, p -> p.getName(), p -> p.getHandle(),
                p -> p.getRole(), p -> p.getSndSettleMode(), p -> p.getRcvSettleMode(), p -> p.getSource(),
                p -> p.getTarget(), p -> p.getUnsettled(), p -> p.getIncompleteUnsettled(),
                p -> p.getInitialDeliveryCount(), p -> p.getMaxMessageSize(), p -> p.getOfferedCapabilities(),
                p -> p.getDesiredCapabilities(), p -> p.getProperties());
        check(new Flow(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, true, true, SOME_FIELDS),
                org.apache.qpid.proton.amqp.transport.Flow.class, p -> p.getNextIncomingId(), p -> p.getIncomingWindow(),
                p -> p.getNextOutgoingId(), p -> p.getOutgoingWindow(), p -> p.getHandle(), p -> p.getDeliveryCount(),
                p -> p.getLinkCredit(), p -> p.getAvailable(), p -> p.getDrain(), p -> p.getEcho(),
                p -> p.getProperties());
        check(new Transfer(1L, 2L, Binary.of(9, 8), 3L, true, true, 1, new Accepted(), true, true, true),
                org.apache.qpid.proton.amqp.transport.Transfer.class, p -> p.getHandle(), p -> p.getDeliveryId(),
                p -> p.getDeliveryTag(), p -> p.getMessageFormat(), p -> p.getSettled(), p -> p.getMore(),
                p -> p.getRcvSettleMode(), p -> p.getState(), p -> p.getResume(), p -> p.getAborted(),
                p -> p.getBatchable());
        check(new Disposition(true, 3L, 4L, true, new Rejected(ERROR), true),
                org.apache.qpid.proton.amqp.transport.Disposition.class, p -> p.getRole(), p -> p.getFirst(),
                p -> p.getLast(), p -> p.getSettled(), p -> p.getState(), p -> p.getBatchable());
        check(new Detach(6L, true, ERROR), org.apache.qpid.proton.amqp.transport.Detach.class, p -> p.getHandle(),
                p -> p.getClosed(), p -> p.getError());
        check(new End(ERROR), org.apache.qpid.proton.amqp.transport.End.class, p -> p.getError());
        check(new Close(ERROR), org.apache.qpid.proton.amqp.transport.Close.class, p -> p.getError());
        check(ERROR, org.apache.qpid.proton.amqp.transport.ErrorCondition.class, p -> p.getCondition(),
                p -> p.getDescription(), p -> p.getInfo());
    }

    // ---- part 3: messaging ---------------------------------------------------

    @Test
    void deliveryStates() throws AmqpDecodeException {
        check(new Received(1L, ULong.of(300)), org.apache.qpid.proton.amqp.messaging.Received.class,
                p -> p.getSectionNumber(), p -> p.getSectionOffset());
        check(new Accepted(), org.apache.qpid.proton.amqp.messaging.Accepted.class);
        check(new Rejected(ERROR), org.apache.qpid.proton.amqp.messaging.Rejected.class, p -> p.getError());
        check(new Released(), org.apache.qpid.proton.amqp.messaging.Released.class);
        check(new Modified(true, true, SOME_FIELDS), org.apache.qpid.proton.amqp.messaging.Modified.class,
                p -> p.getDeliveryFailed(), p -> p.getUndeliverableHere(), p -> p.getMessageAnnotations());
    }

    @Test
    void sourcesTargetsAndLifetimePolicies() throws AmqpDecodeException {
        check(source(), org.apache.qpid.proton.amqp.messaging.Source.class, p -> p.getAddress(), p -> p.getDurable(),
                p -> p.getExpiryPolicy(), p -> p.getTimeout(), p -> p.getDynamic(), p -> p.getDynamicNodeProperties(),
                p -> p.getDistributionMode(), p -> p.getFilter(), p -> p.getDefaultOutcome(), p -> p.getOutcomes(),
                p -> p.getCapabilities());
        check(target(), org.apache.qpid.proton.amqp.messaging.Target.class, p -> p.getAddress(), p -> p.getDurable(),
                p -> p.getExpiryPolicy(), p -> p.getTimeout(), p -> p.getDynamic(), p -> p.getDynamicNodeProperties(),
                p -> p.getCapabilities());
        check(new DeleteOnClose(), org.apache.qpid.proton.amqp.messaging.DeleteOnClose.class);
        check(new DeleteOnNoLinks(), org.apache.qpid.proton.amqp.messaging.DeleteOnNoLinks.class);
        check(new DeleteOnNoMessages(), org.apache.qpid.proton.amqp.messaging.DeleteOnNoMessages.class);
        check(new DeleteOnNoLinksOrMessages(), org.apache.qpid.proton.amqp.messaging.DeleteOnNoLinksOrMessages.class);
    }

    @Test
    void messageSections() throws AmqpDecodeException {
        check(new Header(true, 7, 60_000L, true, 2L), org.apache.qpid.proton.amqp.messaging.Header.class,
                p -> p.getDurable(), p -> p.getPriority(), p -> p.getTtl(), p -> p.getFirstAcquirer(),
                p -> p.getDeliveryCount());
        check(new Properties(UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"), Binary.of(1), "to", "subject",
                "reply-to", ULong.of(77), Symbol.of("text/plain"), Symbol.of("gzip"), new Timestamp(2_000),
                new Timestamp(1_000), "group", 3L, "reply-group"),
                org.apache.qpid.proton.amqp.messaging.Properties.class, p -> p.getMessageId(), p -> p.getUserId(),
                p -> p.getTo(), p -> p.getSubject(), p -> p.getReplyTo(), p -> p.getCorrelationId(),
                p -> p.getContentType(), p -> p.getContentEncoding(), p -> p.getAbsoluteExpiryTime(),
                p -> p.getCreationTime(), p -> p.getGroupId(), p -> p.getGroupSequence(), p -> p.getReplyToGroupId());
        checkValue(new DeliveryAnnotations(map(Symbol.of("x-opt-a"), 1)),
                org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations.class, p -> p.getValue());
        checkValue(new MessageAnnotations(map(Symbol.of("x-opt-jms-msg-type"), (byte) 5)),
                org.apache.qpid.proton.amqp.messaging.MessageAnnotations.class, p -> p.getValue());
        checkValue(new ApplicationProperties(map("color", "red")),
                org.apache.qpid.proton.amqp.messaging.ApplicationProperties.class, p -> p.getValue());
        checkValue(new Data(Binary.of(1, 2, 3)), org.apache.qpid.proton.amqp.messaging.Data.class, p -> p.getValue());
        checkValue(new AmqpSequence(List.of(1, "two")), org.apache.qpid.proton.amqp.messaging.AmqpSequence.class,
                p -> p.getValue());
        checkValue(new AmqpValue("text"), org.apache.qpid.proton.amqp.messaging.AmqpValue.class, p -> p.getValue());
        checkValue(new Footer(map(Symbol.of("x-opt-sig"), Binary.of(4))),
                org.apache.qpid.proton.amqp.messaging.Footer.class, p -> p.getValue());
    }

    // ---- part 4: transactions ------------------------------------------------

    @Test
    void transactionTypes() throws AmqpDecodeException {
        check(new Coordinator(List.of(Symbol.of("amqp:local-transactions"))),
                org.apache.qpid.proton.amqp.transaction.Coordinator.class, p -> p.getCapabilities());
        // global-id is left out: it belongs to distributed transactions, which 1.0 does not define
        check(new Declare(null), org.apache.qpid.proton.amqp.transaction.Declare.class);
        check(new Discharge(Binary.of(1), true), org.apache.qpid.proton.amqp.transaction.Discharge.class,
                p -> p.getTxnId(), p -> p.getFail());
        check(new Declared(Binary.of(2)), org.apache.qpid.proton.amqp.transaction.Declared.class, p -> p.getTxnId());
        check(new TransactionalState(Binary.of(3), new Accepted()),
                org.apache.qpid.proton.amqp.transaction.TransactionalState.class, p -> p.getTxnId(),
                p -> p.getOutcome());
    }

    // ---- part 5: security ----------------------------------------------------

    @Test
    void saslFrameBodies() throws AmqpDecodeException {
        check(new SaslMechanisms(List.of(Symbol.of("PLAIN"), Symbol.of("ANONYMOUS"))),
                org.apache.qpid.proton.amqp.security.SaslMechanisms.class, p -> p.getSaslServerMechanisms());
        check(new SaslInit(Symbol.of("PLAIN"), Binary.of(0, 'u', 0, 'p'), "host"),
                org.apache.qpid.proton.amqp.security.SaslInit.class, p -> p.getMechanism(),
                p -> p.getInitialResponse(), p -> p.getHostname());
        check(new SaslChallenge(Binary.of(1)), org.apache.qpid.proton.amqp.security.SaslChallenge.class,
                p -> p.getChallenge());
        check(new SaslResponse(Binary.of(2)), org.apache.qpid.proton.amqp.security.SaslResponse.class,
                p -> p.getResponse());
        check(new SaslOutcome(0, Binary.of(3)), org.apache.qpid.proton.amqp.security.SaslOutcome.class,
                p -> p.getCode(), p -> p.getAdditionalData());
    }

    // ---- a whole message -----------------------------------------------------

    @Test
    void aMessageBuiltByProtonIsReadByTassisAndBack() throws AmqpDecodeException {
        org.apache.qpid.proton.message.Message proton = org.apache.qpid.proton.message.Message.Factory.create();
        proton.setDurable(true);
        proton.setPriority((short) 7);
        proton.setMessageId("id-1");
        proton.setAddress("orders");
        proton.setSubject("subject");
        Map<String, Object> application = new LinkedHashMap<>();
        application.put("color", "red");
        application.put("weight", 12);
        proton.setApplicationProperties(new org.apache.qpid.proton.amqp.messaging.ApplicationProperties(application));
        proton.setBody(new org.apache.qpid.proton.amqp.messaging.AmqpValue("hello"));
        byte[] buffer = new byte[4096];
        int length = proton.encode(buffer, 0, buffer.length);
        byte[] octets = java.util.Arrays.copyOf(buffer, length);

        AmqpMessage ours = AmqpMessage.decode(octets);

        assertEquals(true, ours.header().durable());
        assertEquals(7, ours.header().priority());
        assertEquals("id-1", ours.properties().messageId());
        assertEquals("orders", ours.properties().to());
        assertEquals("subject", ours.properties().subject());
        assertEquals("red", ours.applicationProperties().value().get("color"));
        assertEquals(12, ours.applicationProperties().value().get("weight"));
        assertEquals(List.of(new AmqpValue("hello")), ours.body());

        org.apache.qpid.proton.message.Message back = org.apache.qpid.proton.message.Message.Factory.create();
        byte[] written = ours.encode();
        back.decode(written, 0, written.length);
        assertEquals("orders", back.getAddress());
        assertEquals((short) 7, back.getPriority());
        assertEquals("hello", ((org.apache.qpid.proton.amqp.messaging.AmqpValue) back.getBody()).getValue());
        assertEquals(new ArrayList<>(application.keySet()),
                new ArrayList<>(back.getApplicationProperties().getValue().keySet()));
    }
}
