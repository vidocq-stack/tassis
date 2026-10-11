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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.vidocq.tassis.amqp.internal.types.AmqpArray;
import io.vidocq.tassis.amqp.internal.types.AmqpChar;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.AmqpDecoder;
import io.vidocq.tassis.amqp.internal.types.AmqpEncoder;
import io.vidocq.tassis.amqp.internal.types.AmqpType;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.Decimal128;
import io.vidocq.tassis.amqp.internal.types.Decimal32;
import io.vidocq.tassis.amqp.internal.types.Decimal64;
import io.vidocq.tassis.amqp.internal.types.Described;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.Timestamp;
import io.vidocq.tassis.amqp.internal.types.UByte;
import io.vidocq.tassis.amqp.internal.types.UInt;
import io.vidocq.tassis.amqp.internal.types.ULong;
import io.vidocq.tassis.amqp.internal.types.UShort;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.junit.jupiter.api.Test;

/**
 * The primitive types of AMQP 1.0 (part 1, §1.6), ours against Proton-J's: what Tassis writes
 * Proton-J reads as the same value, and what Proton-J writes Tassis reads as the same value.
 */
class PrimitiveTypesInteropTest {

    /** A Tassis value as Proton-J represents it. */
    static Object toProton(Object value) {
        return switch (value) {
            case null -> null;
            case UByte u -> UnsignedByte.valueOf((byte) u.value());
            case UShort u -> UnsignedShort.valueOf((short) u.value());
            case UInt u -> UnsignedInteger.valueOf(u.value());
            case ULong u -> UnsignedLong.valueOf(Long.toUnsignedString(u.bits()));
            case Symbol s -> org.apache.qpid.proton.amqp.Symbol.valueOf(s.value());
            case Binary b -> new org.apache.qpid.proton.amqp.Binary(b.bytes());
            case Timestamp t -> new Date(t.millis());
            case AmqpChar c -> (char) c.codePoint();
            case List<?> list -> {
                List<Object> converted = new ArrayList<>();
                for (Object item : list) {
                    converted.add(toProton(item));
                }
                yield converted;
            }
            case Map<?, ?> map -> {
                Map<Object, Object> converted = new LinkedHashMap<>();
                map.forEach((key, item) -> converted.put(toProton(key), toProton(item)));
                yield converted;
            }
            default -> value;
        };
    }

    private static List<Object> values() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(Symbol.of("key"), "value");
        map.put("other", UInt.of(7));
        map.put(3, null);
        List<Object> nested = new ArrayList<>();
        nested.add(null);
        nested.add(List.of(1, 2L, List.of()));
        nested.add(map);
        List<Object> values = new ArrayList<>(List.of(true, false, UByte.of(0), UByte.of(200), UShort.of(0),
                UShort.of(65_535), UInt.of(0), UInt.of(1), UInt.of(255), UInt.of(256), UInt.of(UInt.MAX), ULong.of(0),
                ULong.of(255), ULong.of(256), ULong.of(-1), ULong.of(Long.MIN_VALUE), (byte) -5, (short) -300, 0, 127,
                -128, 128, -129, Integer.MAX_VALUE, Integer.MIN_VALUE, 0L, 127L, -128L, 128L, Long.MAX_VALUE,
                Long.MIN_VALUE, 1.5f, -0.0f, Float.MAX_VALUE, 2.25, Double.MIN_VALUE, new AmqpChar('A'),
                new AmqpChar('é'), new Timestamp(0), new Timestamp(1_760_000_000_000L), new Timestamp(-1),
                UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"), Binary.of(), Binary.of(1, 2, 3),
                new Binary(new byte[300]), "", "text", "日本語 🎉", "x".repeat(300), Symbol.of(""), Symbol.of("amqp:ok"),
                Symbol.of("s".repeat(300)), List.of(), List.of("a", 1), nested, new LinkedHashMap<>(), map));
        values.add(null);
        List<Object> many = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            many.add(i);
        }
        values.add(many);
        return values;
    }

    @Test
    void protonReadsWhatTassisWrites() {
        for (Object value : values()) {
            assertEquals(toProton(value), Proton.decode(AmqpEncoder.encode(value)), "written by Tassis: " + value);
        }
    }

    @Test
    void tassisReadsWhatProtonWrites() throws AmqpDecodeException {
        for (Object value : values()) {
            assertEquals(value, AmqpDecoder.decode(Proton.encode(toProton(value))), "written by Proton-J: " + value);
        }
    }

    @Test
    void bothWriteTheSameOctets() {
        // Both pick the shortest encoding, so the octets themselves can be compared.
        for (Object value : values()) {
            assertArrayEquals(Proton.encode(toProton(value)), AmqpEncoder.encode(value), "octets of " + value);
        }
    }

    @Test
    void decimalsCarryTheBitsProtonGivesThem() throws AmqpDecodeException {
        org.apache.qpid.proton.amqp.Decimal32 d32 = new org.apache.qpid.proton.amqp.Decimal32(new BigDecimal("12.5"));
        org.apache.qpid.proton.amqp.Decimal64 d64 = new org.apache.qpid.proton.amqp.Decimal64(new BigDecimal("12.5"));
        org.apache.qpid.proton.amqp.Decimal128 d128 =
                new org.apache.qpid.proton.amqp.Decimal128(new BigDecimal("12.5"));

        assertEquals(new Decimal32(d32.getBits()), AmqpDecoder.decode(Proton.encode(d32)));
        assertEquals(new Decimal64(d64.getBits()), AmqpDecoder.decode(Proton.encode(d64)));
        assertEquals(new Decimal128(d128.getMostSignificantBits(), d128.getLeastSignificantBits()),
                AmqpDecoder.decode(Proton.encode(d128)));
        assertEquals(d32, Proton.decode(AmqpEncoder.encode(new Decimal32(d32.getBits()))));
        assertEquals(d64, Proton.decode(AmqpEncoder.encode(new Decimal64(d64.getBits()))));
        assertEquals(d128, Proton.decode(AmqpEncoder.encode(
                new Decimal128(d128.getMostSignificantBits(), d128.getLeastSignificantBits()))));
    }

    @Test
    void arrays() throws AmqpDecodeException {
        org.apache.qpid.proton.amqp.Symbol[] symbols = {org.apache.qpid.proton.amqp.Symbol.valueOf("PLAIN"),
                org.apache.qpid.proton.amqp.Symbol.valueOf("ANONYMOUS")};
        AmqpArray ours = AmqpArray.ofSymbols(List.of(Symbol.of("PLAIN"), Symbol.of("ANONYMOUS")));

        assertEquals(ours, AmqpDecoder.decode(Proton.encode(symbols)));
        assertArrayEquals(symbols, (Object[]) Proton.decode(AmqpEncoder.encode(ours)));

        assertEquals(AmqpArray.of(AmqpType.INT, List.of(1, -70_000, 3)),
                AmqpDecoder.decode(Proton.encode(new int[] {1, -70_000, 3})));
        assertArrayEquals(new int[] {1, -70_000, 3},
                (int[]) Proton.decode(AmqpEncoder.encode(AmqpArray.of(AmqpType.INT, List.of(1, -70_000, 3)))));
        assertArrayEquals(new long[] {5L, Long.MIN_VALUE},
                (long[]) Proton.decode(AmqpEncoder.encode(AmqpArray.of(AmqpType.LONG, List.of(5L, Long.MIN_VALUE)))));
        assertArrayEquals(new boolean[] {true, false},
                (boolean[]) Proton.decode(AmqpEncoder.encode(AmqpArray.of(AmqpType.BOOLEAN, List.of(true, false)))));
        assertArrayEquals(new double[] {1.5, -2.0},
                (double[]) Proton.decode(AmqpEncoder.encode(AmqpArray.of(AmqpType.DOUBLE, List.of(1.5, -2.0)))));
        assertArrayEquals(new Object[] {"a", "bc"},
                (Object[]) Proton.decode(AmqpEncoder.encode(AmqpArray.of(AmqpType.STRING, List.of("a", "bc")))));
        assertEquals(AmqpArray.of(AmqpType.STRING, List.of("a", "bc")),
                AmqpDecoder.decode(Proton.encode(new String[] {"a", "bc"})));
        assertEquals(AmqpArray.of(AmqpType.UUID, List.of(new UUID(1, 2))),
                AmqpDecoder.decode(Proton.encode(new UUID[] {new UUID(1, 2)})));
    }

    @Test
    void describedValuesWithADescriptorNeitherKnows() throws AmqpDecodeException {
        Described ours = new Described(Symbol.of("example:thing"), List.of("a", UInt.of(1)));

        DescribedType read = (DescribedType) Proton.decode(AmqpEncoder.encode(ours));

        assertEquals(org.apache.qpid.proton.amqp.Symbol.valueOf("example:thing"), read.getDescriptor());
        assertEquals(toProton(ours.value()), read.getDescribed());
        assertEquals(ours, AmqpDecoder.decode(Proton.encode(read)));
    }
}
