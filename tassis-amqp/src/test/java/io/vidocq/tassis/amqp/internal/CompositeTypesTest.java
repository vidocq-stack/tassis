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
package io.vidocq.tassis.amqp.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vidocq.tassis.amqp.internal.messaging.Accepted;
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
import io.vidocq.tassis.amqp.internal.types.Described;
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.Timestamp;
import io.vidocq.tassis.amqp.internal.types.UInt;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The described types of AMQP 1.0 that Tassis knows: the performatives of the transport (part
 * 2, §2.7), the message sections, the termini and the delivery states (part 3), the
 * transaction types (part 4, §4.5) and the SASL frame bodies (part 5, §5.3.3).
 */
class CompositeTypesTest {

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

    private static byte[] hex(String octets) {
        return HexFormat.of().parseHex(octets.replace(" ", ""));
    }

    private static String hex(byte[] octets) {
        return HexFormat.ofDelimiter(" ").formatHex(octets);
    }

    /** One value of each type with every field set, and to a value of its own where it can be. */
    static List<DescribedType> everyTypeFullySet() {
        Source source = new Source("orders", 2L, Symbol.of("never"), 30L, true, fields("lifetime-policy",
                new DeleteOnClose()), Symbol.of("copy"), fields("jms-selector", new Described(
                        ULong.of(0x0000468C00000004L), "color = 'red'")), new Released(), SYMBOLS, List.of(Symbol.of("queue")));
        Target target = new Target("replies", 1L, Symbol.of("link-detach"), 31L, false, SOME_FIELDS,
                List.of(Symbol.of("topic")));
        List<DescribedType> all = new ArrayList<>();
        all.add(new Open("container", "host", 65_536L, 255, 30_000L, SYMBOLS, List.of(Symbol.of("en-US")),
                List.of(Symbol.of("ANONYMOUS-RELAY")), List.of(Symbol.of("sole-connection-for-container")), SOME_FIELDS));
        all.add(new Begin(7, 1L, 2L, 3L, 4L, SYMBOLS, List.of(Symbol.of("d")), SOME_FIELDS));
        all.add(new Attach("link", 5L, true, 1, 0, source, target, map(Binary.of(1), new Accepted()), true, 9L,
                ULong.of(1_000_000), SYMBOLS, List.of(Symbol.of("d")), SOME_FIELDS));
        all.add(new Flow(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, true, false, SOME_FIELDS));
        all.add(new Transfer(1L, 2L, Binary.of(9, 8), 0L, false, true, 1, new Accepted(), false, true, false));
        all.add(new Disposition(true, 3L, 4L, true, new Rejected(ERROR), false));
        all.add(new Detach(6L, true, ERROR));
        all.add(new End(ERROR));
        all.add(new Close(ERROR));
        all.add(ERROR);
        all.add(new Received(1L, ULong.of(300)));
        all.add(new Accepted());
        all.add(new Rejected(ERROR));
        all.add(new Released());
        all.add(new Modified(true, false, SOME_FIELDS));
        all.add(source);
        all.add(target);
        all.add(new DeleteOnClose());
        all.add(new DeleteOnNoLinks());
        all.add(new DeleteOnNoMessages());
        all.add(new DeleteOnNoLinksOrMessages());
        all.add(new Header(true, 7, 60_000L, false, 2L));
        all.add(new DeliveryAnnotations(map(Symbol.of("x-opt-a"), 1)));
        all.add(new MessageAnnotations(map(Symbol.of("x-opt-jms-msg-type"), (byte) 5)));
        all.add(new Properties(UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"), Binary.of(1), "to", "subject",
                "reply-to", ULong.of(77), Symbol.of("text/plain"), Symbol.of("gzip"), new Timestamp(2_000),
                new Timestamp(1_000), "group", 3L, "reply-group"));
        all.add(new ApplicationProperties(map("color", "red")));
        all.add(new Data(Binary.of(1, 2, 3)));
        all.add(new AmqpSequence(List.of(1, "two")));
        all.add(new AmqpValue("text"));
        all.add(new Footer(map(Symbol.of("x-opt-sig"), Binary.of(4))));
        all.add(new Coordinator(List.of(Symbol.of("amqp:local-transactions"))));
        all.add(new Declare(Binary.of(1, 1)));
        all.add(new Discharge(Binary.of(1), true));
        all.add(new Declared(Binary.of(2)));
        all.add(new TransactionalState(Binary.of(3), new Accepted()));
        all.add(new SaslMechanisms(List.of(Symbol.of("PLAIN"), Symbol.of("ANONYMOUS"))));
        all.add(new SaslInit(Symbol.of("PLAIN"), Binary.of(0, 'u', 0, 'p'), "host"));
        all.add(new SaslChallenge(Binary.of(1)));
        all.add(new SaslResponse(Binary.of(2)));
        all.add(new SaslOutcome(0, Binary.of(3)));
        return all;
    }

    @Test
    void everyTypeIsReadBackAsItWasWritten() throws AmqpDecodeException {
        List<DescribedType> all = everyTypeFullySet();
        assertEquals(40, all.size(), "one value per type the registry knows");

        for (DescribedType value : all) {
            byte[] encoded = AmqpEncoder.encode(value);

            assertEquals(value, AmqpDecoder.decode(encoded, AmqpTypes.ALL), value.getClass().getSimpleName());
        }
    }

    @Test
    void everyTypeIsAlsoReadWhenAPeerNamesItBySymbol() throws AmqpDecodeException {
        for (DescribedType value : everyTypeFullySet()) {
            byte[] encoded = AmqpEncoder.encode(new Described(value.descriptorName(), value.describedValue()));

            assertEquals(value, AmqpDecoder.decode(encoded, AmqpTypes.ALL), value.descriptorName().toString());
        }
    }

    @Test
    void theDescriptorsAreThoseOfTheSpecification() {
        // part 2 §2.7, part 3 §3.2, §3.4 and §3.5, part 4 §4.5, part 5 §5.3.3
        Map<String, Long> expected = new LinkedHashMap<>();
        expected.put("amqp:open:list", 0x10L);
        expected.put("amqp:begin:list", 0x11L);
        expected.put("amqp:attach:list", 0x12L);
        expected.put("amqp:flow:list", 0x13L);
        expected.put("amqp:transfer:list", 0x14L);
        expected.put("amqp:disposition:list", 0x15L);
        expected.put("amqp:detach:list", 0x16L);
        expected.put("amqp:end:list", 0x17L);
        expected.put("amqp:close:list", 0x18L);
        expected.put("amqp:error:list", 0x1dL);
        expected.put("amqp:received:list", 0x23L);
        expected.put("amqp:accepted:list", 0x24L);
        expected.put("amqp:rejected:list", 0x25L);
        expected.put("amqp:released:list", 0x26L);
        expected.put("amqp:modified:list", 0x27L);
        expected.put("amqp:source:list", 0x28L);
        expected.put("amqp:target:list", 0x29L);
        expected.put("amqp:delete-on-close:list", 0x2bL);
        expected.put("amqp:delete-on-no-links:list", 0x2cL);
        expected.put("amqp:delete-on-no-messages:list", 0x2dL);
        expected.put("amqp:delete-on-no-links-or-messages:list", 0x2eL);
        expected.put("amqp:header:list", 0x70L);
        expected.put("amqp:delivery-annotations:map", 0x71L);
        expected.put("amqp:message-annotations:map", 0x72L);
        expected.put("amqp:properties:list", 0x73L);
        expected.put("amqp:application-properties:map", 0x74L);
        expected.put("amqp:data:binary", 0x75L);
        expected.put("amqp:amqp-sequence:list", 0x76L);
        expected.put("amqp:amqp-value:*", 0x77L);
        expected.put("amqp:footer:map", 0x78L);
        expected.put("amqp:coordinator:list", 0x30L);
        expected.put("amqp:declare:list", 0x31L);
        expected.put("amqp:discharge:list", 0x32L);
        expected.put("amqp:declared:list", 0x33L);
        expected.put("amqp:transactional-state:list", 0x34L);
        expected.put("amqp:sasl-mechanisms:list", 0x40L);
        expected.put("amqp:sasl-init:list", 0x41L);
        expected.put("amqp:sasl-challenge:list", 0x42L);
        expected.put("amqp:sasl-response:list", 0x43L);
        expected.put("amqp:sasl-outcome:list", 0x44L);

        Map<String, Long> actual = new LinkedHashMap<>();
        for (DescribedType value : everyTypeFullySet()) {
            actual.put(value.descriptorName().value(), value.descriptorCode().bits());
        }

        assertEquals(new java.util.TreeMap<>(expected), new java.util.TreeMap<>(actual));
    }

    @Test
    void fieldsAreWrittenInTheOrderOfTheSpecification() {
        // open: container-id, hostname, max-frame-size, channel-max, idle-time-out (§2.7.1)
        assertEquals("00 53 10 c0 14 05 a1 01 63 a1 01 68 70 00 01 00 00 60 00 ff 70 00 00 75 30",
                hex(AmqpEncoder.encode(new Open("c", "h", 65_536L, 255, 30_000L, null, null, null, null, null))));
        // flow: next-incoming-id, incoming-window, next-outgoing-id, outgoing-window, handle,
        // delivery-count, link-credit, available, drain (§2.7.4)
        assertEquals("00 53 13 c0 12 09 52 01 52 02 52 03 52 04 52 05 52 06 52 07 52 08 41",
                hex(AmqpEncoder.encode(new Flow(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, true, null, null))));
        // transfer: handle, delivery-id, delivery-tag, message-format, settled (§2.7.5)
        assertEquals("00 53 14 c0 09 05 43 52 01 a0 01 09 43 42",
                hex(AmqpEncoder.encode(new Transfer(0L, 1L, Binary.of(9), 0L, false, null, null, null, null, null, null))));
        // sasl-init: mechanism, initial-response (§5.3.3.2)
        assertEquals("00 53 41 c0 0e 02 a3 05 50 4c 41 49 4e a0 04 00 75 00 70",
                hex(AmqpEncoder.encode(new SaslInit(Symbol.of("PLAIN"), Binary.of(0, 'u', 0, 'p'), null))));
    }

    @Test
    void fieldsLeftOutAtTheEndAreNotWritten() throws AmqpDecodeException {
        assertEquals("00 53 24 45", hex(AmqpEncoder.encode(new Accepted())));
        assertEquals("00 53 18 45", hex(AmqpEncoder.encode(new Close(null))));
        assertEquals("00 53 16 c0 02 01 43", hex(AmqpEncoder.encode(new Detach(0L, null, null))));
        // a null between two values is written: fields are known by their position
        assertEquals("00 53 16 c0 0c 03 43 40 00 53 1d c0 04 01 a3 01 65",
                hex(AmqpEncoder.encode(new Detach(0L, null, new AmqpError(Symbol.of("e"), null, null)))));

        Detach read = (Detach) AmqpDecoder.decode(hex("00 53 16 c0 02 01 43"), AmqpTypes.ALL);
        assertEquals(0L, read.handle());
        assertNull(read.closed());
        assertNull(read.error());
    }

    @Test
    void aFieldSentAsNullAndAFieldLeftOutAreTheSame() throws AmqpDecodeException {
        assertEquals(new Detach(1L, null, null), AmqpDecoder.decode(hex("00 53 16 c0 05 03 52 01 40 40"), AmqpTypes.ALL));
        assertEquals(new Close(null), AmqpDecoder.decode(hex("00 53 18 c0 02 01 40"), AmqpTypes.ALL));
    }

    @Test
    void aFieldTakingSeveralSymbolsAlsoTakesOneAlone() throws AmqpDecodeException {
        // sasl-mechanisms with a single symbol instead of an array (§1.4)
        assertEquals(new SaslMechanisms(List.of(Symbol.of("PLAIN"))),
                AmqpDecoder.decode(hex("00 53 40 c0 08 01 a3 05 50 4c 41 49 4e"), AmqpTypes.ALL));
        // an empty array of capabilities is no capability
        Coordinator coordinator = (Coordinator) AmqpDecoder.decode(hex("00 53 30 c0 05 01 e0 02 00 a3"), AmqpTypes.ALL);
        assertNull(coordinator.capabilities());
    }

    @Test
    void aMandatoryFieldMustBeThere() {
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("00 53 10 45"), AmqpTypes.ALL));
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("00 53 10 c0 02 01 40"), AmqpTypes.ALL));
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("00 53 16 45"), AmqpTypes.ALL));
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("00 53 40 45"), AmqpTypes.ALL));
        assertThrows(IllegalArgumentException.class, () -> new Open(null, null, null, null, null, null, null, null, null, null));
        assertThrows(IllegalArgumentException.class, () -> new Discharge(null, true));
    }

    @Test
    void aFieldOfTheWrongTypeIsRefused() {
        // open with a uint where the container-id string is expected
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("00 53 10 c0 03 01 52 01"), AmqpTypes.ALL));
        // detach whose handle is a string
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("00 53 16 c0 04 01 a1 01 61"), AmqpTypes.ALL));
        // open that is not a list at all
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("00 53 10 a1 01 61"), AmqpTypes.ALL));
        // properties of a begin whose keys are strings, not symbols
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(
                hex("00 53 11 c0 13 08 40 43 43 43 40 40 40 c1 07 02 a1 01 6b a1 01 76"), AmqpTypes.ALL));
        // data that is a string
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("00 53 75 a1 01 61"), AmqpTypes.ALL));
    }

    @Test
    void fieldsAddedByALaterRevisionAreIgnored() throws AmqpDecodeException {
        // close with a second field no revision of 1.0 defines
        assertEquals(new Close(null), AmqpDecoder.decode(hex("00 53 18 c0 03 02 40 41"), AmqpTypes.ALL));
    }

    @Test
    void anUnknownDescriptorIsKeptAsItIs() throws AmqpDecodeException {
        Object value = AmqpDecoder.decode(hex("00 80 00 00 46 8c 00 00 00 04 a1 01 78"), AmqpTypes.ALL);

        assertEquals(new Described(ULong.of(0x0000468C00000004L), "x"), value);
        assertInstanceOf(Described.class,
                ((Source) AmqpDecoder.decode(AmqpEncoder.encode(everyTypeFullySet().get(15)), AmqpTypes.ALL)).filter()
                        .get(Symbol.of("jms-selector")));
        assertEquals(UInt.of(1), AmqpDecoder.decode(hex("52 01"), AmqpTypes.ALL));
    }
}
