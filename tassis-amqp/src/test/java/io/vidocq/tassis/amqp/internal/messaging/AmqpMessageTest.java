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
package io.vidocq.tassis.amqp.internal.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The format of a message (AMQP 1.0 part 3, §3.2): its sections, one after the other, in the
 * order the specification gives them, with one of three kinds of body.
 */
class AmqpMessageTest {

    private static byte[] hex(String octets) {
        return HexFormat.of().parseHex(octets.replace(" ", ""));
    }

    private static String hex(byte[] octets) {
        return HexFormat.ofDelimiter(" ").formatHex(octets);
    }

    private static Map<Object, Object> map(Object key, Object value) {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    @Test
    void aMessageIsItsSectionsOneAfterTheOther() throws AmqpDecodeException {
        AmqpMessage message = AmqpMessage.ofBody(new AmqpValue("hi"));

        assertEquals("00 53 77 a1 02 68 69", hex(message.encode()));
        assertEquals(message, AmqpMessage.decode(hex("00 53 77 a1 02 68 69")));
    }

    @Test
    void everySectionIsReadBack() throws AmqpDecodeException {
        AmqpMessage message = new AmqpMessage(new Header(true, 4, null, null, null),
                new DeliveryAnnotations(map(Symbol.of("x-opt-d"), 1)),
                new MessageAnnotations(map(Symbol.of("x-opt-jms-dest"), (byte) 0)),
                new Properties("id-1", null, "orders", null, null, null, null, null, null, null, null, null, null),
                new ApplicationProperties(map("color", "red")), List.of(new Data(Binary.of(1, 2)), new Data(Binary.of(3))),
                new Footer(map(Symbol.of("x-opt-f"), true)));

        AmqpMessage read = AmqpMessage.decode(message.encode());

        assertEquals(message, read);
        assertEquals("orders", read.properties().to());
        assertEquals(2, read.body().size());
    }

    @Test
    void theSectionsAMessageDoesNotHaveAreNull() throws AmqpDecodeException {
        AmqpMessage read = AmqpMessage.decode(hex("00 53 75 a0 01 07"));

        assertNull(read.header());
        assertNull(read.deliveryAnnotations());
        assertNull(read.messageAnnotations());
        assertNull(read.properties());
        assertNull(read.applicationProperties());
        assertNull(read.footer());
        assertEquals(List.of(new Data(Binary.of(7))), read.body());
    }

    @Test
    void theBodyIsDataOrSequencesOrOneValue() throws AmqpDecodeException {
        AmqpMessage sequences = new AmqpMessage(null, null, null, null, null,
                List.of(new AmqpSequence(List.of(1)), new AmqpSequence(List.of(2))), null);
        assertEquals(sequences, AmqpMessage.decode(sequences.encode()));

        // §3.2: "exactly one of" the three forms
        assertThrows(IllegalArgumentException.class, () -> new AmqpMessage(null, null, null, null, null,
                List.of(new AmqpValue(1), new AmqpValue(2)), null));
        assertThrows(IllegalArgumentException.class, () -> new AmqpMessage(null, null, null, null, null,
                List.of(new Data(Binary.of(1)), new AmqpValue(2)), null));
        assertThrows(IllegalArgumentException.class, () -> new AmqpMessage(null, null, null, null, null,
                List.of(new Header(null, null, null, null, null)), null));
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("00 53 77 41 00 53 77 42")));
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("00 53 75 a0 00 00 53 77 42")));
    }

    @Test
    void aMessageWithoutABodyIsRead() throws AmqpDecodeException {
        // not what §3.2 allows a sender, but what some peers send: read, and written back the same
        AmqpMessage read = AmqpMessage.decode(hex("00 53 70 45"));

        assertEquals(new Header(null, null, null, null, null), read.header());
        assertEquals(List.of(), read.body());
        assertEquals("00 53 70 45", hex(read.encode()));
        assertEquals(List.of(), AmqpMessage.decode(new byte[0]).body());
    }

    @Test
    void sectionsOutOfOrderOrRepeatedAreRefused() {
        // properties before the header
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("00 53 73 45 00 53 70 45 00 53 77 40")));
        // two headers
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("00 53 70 45 00 53 70 45 00 53 77 40")));
        // a section after the footer
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("00 53 77 40 00 53 78 c1 01 00 00 53 77 40")));
    }

    @Test
    void whatIsNotASectionIsRefused() {
        // a performative, a plain value, an unknown descriptor
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("00 53 18 45")));
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("a1 01 61")));
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("00 53 79 45")));
        assertThrows(AmqpDecodeException.class, () -> AmqpMessage.decode(hex("00 53 77")));
    }
}
