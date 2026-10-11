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
package io.vidocq.tassis.amqp.internal.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The type system of AMQP 1.0 and its encodings (OASIS AMQP 1.0, part 1: types). Each case gives
 * a value, the octets the specification prescribes for it, and checks both directions. The
 * encoder writes the shortest encoding; the decoder reads them all.
 */
class AmqpTypesTest {

    private static byte[] hex(String octets) {
        return HexFormat.of().parseHex(octets.replace(" ", ""));
    }

    private static String hex(byte[] octets) {
        return HexFormat.ofDelimiter(" ").formatHex(octets);
    }

    /** {@code value} is written as {@code octets}, which read back as {@code value}. */
    private static void both(Object value, String octets) throws AmqpDecodeException {
        assertEquals(hex(hex(octets)), hex(AmqpEncoder.encode(value)), "encoding of " + value);
        assertEquals(value, AmqpDecoder.decode(hex(octets)), "decoding of " + octets);
    }

    /** {@code octets} is a longer encoding of {@code value} than the one the encoder writes. */
    private static void alsoReads(Object value, String octets) throws AmqpDecodeException {
        assertEquals(value, AmqpDecoder.decode(hex(octets)), "decoding of " + octets);
    }

    // ---- §1.6.1 to §1.6.18: fixed width -------------------------------------

    @Test
    void nullAndBooleans() throws AmqpDecodeException {
        both(null, "40");
        both(true, "41");
        both(false, "42");
        alsoReads(true, "56 01");
        alsoReads(false, "56 00");
    }

    @Test
    void unsignedIntegers() throws AmqpDecodeException {
        both(UByte.of(0), "50 00");
        both(UByte.of(255), "50 ff");
        both(UShort.of(65535), "60 ff ff");
        both(UShort.of(258), "60 01 02");
        both(UInt.of(0), "43");
        both(UInt.of(255), "52 ff");
        both(UInt.of(256), "70 00 00 01 00");
        both(UInt.of(UInt.MAX), "70 ff ff ff ff");
        alsoReads(UInt.of(0), "70 00 00 00 00");
        alsoReads(UInt.of(7), "70 00 00 00 07");
        both(ULong.of(0), "44");
        both(ULong.of(255), "53 ff");
        both(ULong.of(256), "80 00 00 00 00 00 00 01 00");
        both(ULong.of(-1), "80 ff ff ff ff ff ff ff ff");
        alsoReads(ULong.of(0x10), "80 00 00 00 00 00 00 00 10");
        assertEquals("18446744073709551615", ULong.of(-1).toString());
        assertEquals(4294967295L, UInt.of(UInt.MAX).value());
    }

    @Test
    void signedIntegers() throws AmqpDecodeException {
        both((byte) -1, "51 ff");
        both((short) -2, "61 ff fe");
        both(1, "54 01");
        both(-1, "54 ff");
        both(127, "54 7f");
        both(128, "71 00 00 00 80");
        both(-129, "71 ff ff ff 7f");
        alsoReads(1, "71 00 00 00 01");
        both(1L, "55 01");
        both(-128L, "55 80");
        both(128L, "81 00 00 00 00 00 00 00 80");
        both(Long.MIN_VALUE, "81 80 00 00 00 00 00 00 00");
        alsoReads(-1L, "81 ff ff ff ff ff ff ff ff");
    }

    @Test
    void floatingPointAndDecimals() throws AmqpDecodeException {
        both(1.5f, "72 3f c0 00 00");
        both(-2.0, "82 c0 00 00 00 00 00 00 00");
        both(new Decimal32(0x22500001), "74 22 50 00 01");
        both(new Decimal64(0x2238000000000001L), "84 22 38 00 00 00 00 00 01");
        both(new Decimal128(0x2208000000000000L, 1L), "94 22 08 00 00 00 00 00 00 00 00 00 00 00 00 00 01");
    }

    @Test
    void charTimestampAndUuid() throws AmqpDecodeException {
        both(new AmqpChar('A'), "73 00 00 00 41");
        both(new AmqpChar(0x1F600), "73 00 01 f6 00");
        both(new Timestamp(0x13166618521L), "83 00 00 01 31 66 61 85 21");
        both(new Timestamp(-1), "83 ff ff ff ff ff ff ff ff");
        both(UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
                "98 f8 1d 4f ae 7d ec 11 d0 a7 65 00 a0 c9 1e 6b f6");
    }

    // ---- §1.6.19 to §1.6.21: variable width ---------------------------------

    @Test
    void binaryStringAndSymbol() throws AmqpDecodeException {
        both(Binary.of(), "a0 00");
        both(Binary.of(1, 2, 255), "a0 03 01 02 ff");
        alsoReads(Binary.of(1, 2), "b0 00 00 00 02 01 02");
        both("", "a1 00");
        both("hé", "a1 03 68 c3 a9");
        alsoReads("ab", "b1 00 00 00 02 61 62");
        both(Symbol.of("amqp:ok"), "a3 07 61 6d 71 70 3a 6f 6b");
        alsoReads(Symbol.of("a"), "b3 00 00 00 01 61");
    }

    @Test
    void valuesLongerThan255OctetsTakeTheWideEncoding() throws AmqpDecodeException {
        byte[] bytes = new byte[256];
        byte[] encoded = AmqpEncoder.encode(new Binary(bytes));
        assertEquals("b0 00 00 01 00", hex(java.util.Arrays.copyOf(encoded, 5)));
        assertEquals(new Binary(bytes), AmqpDecoder.decode(encoded));

        String text = "x".repeat(300);
        encoded = AmqpEncoder.encode(text);
        assertEquals("b1 00 00 01 2c", hex(java.util.Arrays.copyOf(encoded, 5)));
        assertEquals(text, AmqpDecoder.decode(encoded));

        Symbol symbol = Symbol.of("s".repeat(256));
        encoded = AmqpEncoder.encode(symbol);
        assertEquals("b3 00 00 01 00", hex(java.util.Arrays.copyOf(encoded, 5)));
        assertEquals(symbol, AmqpDecoder.decode(encoded));
    }

    @Test
    void javaValuesThatHaveAnObviousAmqpType() {
        assertArrayEquals(hex("a0 02 01 02"), AmqpEncoder.encode(new byte[] {1, 2}));
        assertArrayEquals(hex("73 00 00 00 41"), AmqpEncoder.encode('A'));
        assertThrows(IllegalArgumentException.class, () -> AmqpEncoder.encode(new Object()));
        assertThrows(IllegalArgumentException.class, () -> AmqpEncoder.encode(new StringBuilder("no")));
    }

    // ---- §1.6.22 to §1.6.24: compound and array -----------------------------

    @Test
    void lists() throws AmqpDecodeException {
        both(List.of(), "45");
        alsoReads(List.of(), "c0 01 00");
        alsoReads(List.of(), "d0 00 00 00 04 00 00 00 00");
        // size = the count and the items; count = 2
        both(List.of(1, "a"), "c0 06 02 54 01 a1 01 61");
        alsoReads(List.of(1, "a"), "d0 00 00 00 09 00 00 00 02 54 01 a1 01 61");
        List<Object> withNull = new ArrayList<>();
        withNull.add(null);
        withNull.add(true);
        both(withNull, "c0 03 02 40 41");
        both(List.of(List.of(), List.of(UInt.of(0))), "c0 06 02 45 c0 02 01 43");
    }

    @Test
    void aListTooLongForOneOctetTakesTheWideEncoding() throws AmqpDecodeException {
        List<Object> many = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            many.add(true);
        }
        byte[] encoded = AmqpEncoder.encode(many);
        assertEquals("d0 00 00 01 04 00 00 01 00 41", hex(java.util.Arrays.copyOf(encoded, 10)));
        assertEquals(many, AmqpDecoder.decode(encoded));

        List<Object> big = List.of("x".repeat(254));
        encoded = AmqpEncoder.encode(big);
        // 1 + 1 + 254 octets of item, plus the count: 257, which no longer fits one octet
        assertEquals("d0 00 00 01 04 00 00 00 01 a1 fe", hex(java.util.Arrays.copyOf(encoded, 11)));
        assertEquals(big, AmqpDecoder.decode(encoded));
    }

    @Test
    void maps() throws AmqpDecodeException {
        Map<Object, Object> map = new LinkedHashMap<>();
        both(map, "c1 01 00");
        map.put(Symbol.of("k"), 1);
        map.put("z", null);
        // the count is the number of keys and values, not of pairs
        both(map, "c1 0a 04 a3 01 6b 54 01 a1 01 7a 40");
        alsoReads(map, "d1 00 00 00 0d 00 00 00 04 a3 01 6b 54 01 a1 01 7a 40");
    }

    @Test
    void aMapKeepsTheOrderOfItsPairs() throws AmqpDecodeException {
        @SuppressWarnings("unchecked")
        Map<Object, Object> decoded = (Map<Object, Object>) AmqpDecoder.decode(hex("c1 09 04 a1 01 62 41 a1 01 61 42"));

        assertEquals(List.of("b", "a"), List.copyOf(decoded.keySet()));
    }

    @Test
    void arrays() throws AmqpDecodeException {
        // size = the count, the one constructor, the elements
        both(AmqpArray.ofSymbols(List.of(Symbol.of("a"), Symbol.of("bc"))), "e0 07 02 a3 01 61 02 62 63");
        both(AmqpArray.of(AmqpType.INT, List.of(1, -1)), "e0 0a 02 71 00 00 00 01 ff ff ff ff");
        both(AmqpArray.of(AmqpType.BOOLEAN, List.of(true, false)), "e0 04 02 56 01 00");
        both(AmqpArray.of(AmqpType.UINT, List.of()), "e0 02 00 70");
        both(AmqpArray.of(AmqpType.NULL, List.of()), "e0 02 00 40");
        alsoReads(AmqpArray.of(AmqpType.INT, List.of(5)), "e0 03 01 54 05");
        alsoReads(AmqpArray.of(AmqpType.INT, List.of(5)), "f0 00 00 00 09 00 00 00 01 71 00 00 00 05");
        alsoReads(AmqpArray.of(AmqpType.BOOLEAN, List.of(true, true)), "e0 02 02 41");
        both(AmqpArray.of(AmqpType.LIST, List.of(List.of(), List.of(true))),
                "e0 13 02 d0 00 00 00 04 00 00 00 00 00 00 00 05 00 00 00 01 41");
    }

    @Test
    void anArrayOfDescribedValuesWritesTheDescriptorOnce() throws AmqpDecodeException {
        AmqpArray array = new AmqpArray(Symbol.of("d"), AmqpType.UBYTE, List.of(UByte.of(1), UByte.of(2)));

        both(array, "e0 08 02 00 a3 01 64 50 01 02");
    }

    @Test
    void anArrayRefusesAnElementOfAnotherType() {
        AmqpArray mixed = AmqpArray.of(AmqpType.INT, List.of(1, "two"));

        assertThrows(IllegalArgumentException.class, () -> AmqpEncoder.encode(mixed));
    }

    // ---- §1.2: described types ----------------------------------------------

    @Test
    void describedValues() throws AmqpDecodeException {
        both(new Described(Symbol.of("u:x"), "v"), "00 a3 03 75 3a 78 a1 01 76");
        both(new Described(ULong.of(0x77), List.of()), "00 53 77 45");
        both(new Described(ULong.of(1), new Described(ULong.of(2), null)), "00 53 01 00 53 02 40");
    }

    @Test
    void aKnownDescribedTypeIsWrittenWithItsNumericDescriptorAndReadByEither() throws AmqpDecodeException {
        record Example(String text) implements DescribedType {

            @Override
            public ULong descriptorCode() {
                return ULong.of(0x99);
            }

            @Override
            public Symbol descriptorName() {
                return Symbol.of("test:example:list");
            }

            @Override
            public Object describedValue() {
                return List.of(text);
            }
        }
        DescribedTypes types = (descriptor, value) -> descriptor.equals(ULong.of(0x99))
                || descriptor.equals(Symbol.of("test:example:list"))
                        ? new Example((String) ((List<?>) value).get(0)) : new Described(descriptor, value);

        assertEquals("00 53 99 c0 04 01 a1 01 68", hex(AmqpEncoder.encode(new Example("h"))));
        assertEquals(new Example("h"), AmqpDecoder.decode(hex("00 53 99 c0 04 01 a1 01 68"), types));
        assertEquals(new Example("h"), AmqpDecoder.decode(
                hex("00 a3 11 74 65 73 74 3a 65 78 61 6d 70 6c 65 3a 6c 69 73 74 c0 04 01 a1 01 68"), types));
        assertEquals(new Described(ULong.of(0x98), null), AmqpDecoder.decode(hex("00 53 98 40"), types));
    }

    // ---- reading several values ----------------------------------------------

    @Test
    void aDecoderReadsTheValuesOfABufferOneAfterTheOther() throws AmqpDecodeException {
        AmqpDecoder decoder = new AmqpDecoder(java.nio.ByteBuffer.wrap(hex("41 a1 01 61 40")), DescribedTypes.NONE);

        assertEquals(true, decoder.read());
        assertEquals("a", decoder.read());
        assertEquals(true, decoder.hasRemaining());
        assertNull(decoder.read());
        assertEquals(false, decoder.hasRemaining());
    }

    @Test
    void decodeRefusesOctetsLeftAfterTheValue() {
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex("41 41")));
    }

    @Test
    void textIsUtf8() throws AmqpDecodeException {
        String text = "日本語 🎉";
        byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);

        byte[] encoded = AmqpEncoder.encode(text);

        assertEquals(utf8.length, encoded[1]);
        assertEquals(text, AmqpDecoder.decode(encoded));
    }
}
