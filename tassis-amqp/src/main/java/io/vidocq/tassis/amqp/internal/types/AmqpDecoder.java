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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Reads values in any of the encodings of AMQP 1.0 (part 1, §1.6).
 *
 * <p>The octets come from a peer and are trusted for nothing. A size is checked against what is
 * left to read before anything is allocated for it, nesting is bounded, and whatever is wrong
 * is reported as an {@link AmqpDecodeException}: no other exception leaves this class because
 * of what the octets are.
 */
public final class AmqpDecoder {

    /** How deep values may nest. A performative nests five or six levels at most. */
    public static final int MAX_DEPTH = 32;

    /**
     * How many elements an array may hold when its elements take no octet (an array of nulls,
     * of {@code true}): there the count is not bounded by the size of the array.
     */
    static final int MAX_ELEMENTS_WITHOUT_WIDTH = 65_536;

    private final ByteBuffer in;
    private final DescribedTypes types;
    private int depth;

    /** A decoder for the remaining octets of {@code in}, which it reads in network byte order. */
    public AmqpDecoder(ByteBuffer in, DescribedTypes types) {
        this.in = in.slice();
        this.types = types;
    }

    /** Reads the one value {@code octets} must be the encoding of. */
    public static Object decode(byte[] octets) throws AmqpDecodeException {
        return decode(octets, DescribedTypes.NONE);
    }

    /** Like {@link #decode(byte[])}, giving the described values the types {@code types} knows. */
    public static Object decode(byte[] octets, DescribedTypes types) throws AmqpDecodeException {
        AmqpDecoder decoder = new AmqpDecoder(ByteBuffer.wrap(octets), types);
        Object value = decoder.read();
        if (decoder.hasRemaining()) {
            throw new AmqpDecodeException(decoder.in.remaining() + " octets left after the value");
        }
        return value;
    }

    public boolean hasRemaining() {
        return in.hasRemaining();
    }

    /** How many octets have been read so far. */
    public int position() {
        return in.position();
    }

    /** Reads the next value. */
    public Object read() throws AmqpDecodeException {
        int code = u8();
        if (code != 0x00) {
            return primitive(code);
        }
        enter();
        Object descriptor = read();
        Object value = read();
        depth--;
        return types.resolve(descriptor, value);
    }

    private void enter() throws AmqpDecodeException {
        if (++depth > MAX_DEPTH) {
            throw new AmqpDecodeException("values nested more than " + MAX_DEPTH + " deep");
        }
    }

    private void need(long octets) throws AmqpDecodeException {
        if (octets > in.remaining()) {
            throw new AmqpDecodeException(
                    "the value needs " + octets + " more octets and " + in.remaining() + " are left");
        }
    }

    private int u8() throws AmqpDecodeException {
        need(1);
        return in.get() & 0xFF;
    }

    private long u32() throws AmqpDecodeException {
        need(4);
        return in.getInt() & 0xFFFF_FFFFL;
    }

    private long i64() throws AmqpDecodeException {
        need(8);
        return in.getLong();
    }

    /** Reads the data of a value whose format code is {@code code} (§1.6). */
    private Object primitive(int code) throws AmqpDecodeException {
        switch (code) {
            case 0x40:
                return null;
            case 0x41:
                return Boolean.TRUE;
            case 0x42:
                return Boolean.FALSE;
            case 0x56:
                int flag = u8();
                if (flag > 1) {
                    throw new AmqpDecodeException("a boolean is 0 or 1, not " + flag);
                }
                return flag == 1;
            case 0x50:
                return new UByte((byte) u8());
            case 0x60:
                need(2);
                return new UShort(in.getShort());
            case 0x70:
                return new UInt((int) u32());
            case 0x52:
                return new UInt(u8());
            case 0x43:
                return new UInt(0);
            case 0x80:
                return new ULong(i64());
            case 0x53:
                return new ULong(u8());
            case 0x44:
                return new ULong(0);
            case 0x51:
                return (byte) u8();
            case 0x61:
                need(2);
                return in.getShort();
            case 0x71:
                return (int) u32();
            case 0x54:
                return (int) (byte) u8();
            case 0x81:
                return i64();
            case 0x55:
                return (long) (byte) u8();
            case 0x72:
                return Float.intBitsToFloat((int) u32());
            case 0x82:
                return Double.longBitsToDouble(i64());
            case 0x74:
                return new Decimal32((int) u32());
            case 0x84:
                return new Decimal64(i64());
            case 0x94:
                return new Decimal128(i64(), i64());
            case 0x73:
                long codePoint = u32();
                if (!Character.isValidCodePoint((int) codePoint) || codePoint > Character.MAX_CODE_POINT) {
                    throw new AmqpDecodeException(codePoint + " is not a Unicode code point");
                }
                return new AmqpChar((int) codePoint);
            case 0x83:
                return new Timestamp(i64());
            case 0x98:
                return new UUID(i64(), i64());
            case 0xA0:
                return new Binary(octets(u8()));
            case 0xB0:
                return new Binary(octets(u32()));
            case 0xA1:
                return utf8(octets(u8()));
            case 0xB1:
                return utf8(octets(u32()));
            case 0xA3:
                return symbol(octets(u8()));
            case 0xB3:
                return symbol(octets(u32()));
            case 0x45:
                return new ArrayList<>(0);
            case 0xC0:
                return list(compound(1));
            case 0xD0:
                return list(compound(4));
            case 0xC1:
                return map(compound(1));
            case 0xD1:
                return map(compound(4));
            case 0xE0:
                return array(compound(1));
            case 0xF0:
                return array(compound(4));
            default:
                throw new AmqpDecodeException(String.format("unknown format code 0x%02x", code));
        }
    }

    private byte[] octets(long length) throws AmqpDecodeException {
        need(length);
        byte[] octets = new byte[(int) length];
        in.get(octets);
        return octets;
    }

    private static String utf8(byte[] octets) throws AmqpDecodeException {
        try {
            CharBuffer text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(octets));
            return text.toString();
        } catch (CharacterCodingException e) {
            throw new AmqpDecodeException("a string that is not UTF-8");
        }
    }

    private static Symbol symbol(byte[] octets) throws AmqpDecodeException {
        for (byte octet : octets) {
            if (octet < 0) {
                throw new AmqpDecodeException("a symbol that is not ASCII");
            }
        }
        return new Symbol(new String(octets, StandardCharsets.US_ASCII));
    }

    /** What the header of a list, a map or an array says: how many items, in a region of its own. */
    private record Compound(long count, AmqpDecoder content) {
    }

    /**
     * Reads the size and the count of a compound value, each on {@code width} octets, and
     * returns a decoder limited to its content (§1.6.22).
     */
    private Compound compound(int width) throws AmqpDecodeException {
        long size = width == 1 ? u8() : u32();
        if (size < width) {
            throw new AmqpDecodeException("a compound value of size " + size + " cannot hold its own count");
        }
        need(size);
        long count = width == 1 ? u8() : u32();
        int contentLength = (int) (size - width);
        ByteBuffer region = in.slice(in.position(), contentLength);
        in.position(in.position() + contentLength);
        AmqpDecoder content = new AmqpDecoder(region, types);
        content.depth = depth;
        content.enter();
        return new Compound(count, content);
    }

    private static void finished(AmqpDecoder content, String what) throws AmqpDecodeException {
        if (content.hasRemaining()) {
            throw new AmqpDecodeException(content.in.remaining() + " octets left after the items of " + what);
        }
    }

    private static List<Object> list(Compound compound) throws AmqpDecodeException {
        AmqpDecoder content = compound.content();
        // Each item takes one octet at least: a larger count is a lie, caught before allocating.
        content.need(compound.count());
        List<Object> list = new ArrayList<>((int) compound.count());
        for (long i = 0; i < compound.count(); i++) {
            list.add(content.read());
        }
        finished(content, "a list");
        return list;
    }

    private static Map<Object, Object> map(Compound compound) throws AmqpDecodeException {
        AmqpDecoder content = compound.content();
        if (compound.count() % 2 != 0) {
            throw new AmqpDecodeException("a map with a key and no value: its count is " + compound.count());
        }
        content.need(compound.count());
        Map<Object, Object> map = new LinkedHashMap<>();
        for (long i = 0; i < compound.count(); i += 2) {
            Object key = content.read();
            Object value = content.read();
            if (map.containsKey(key)) {
                throw new AmqpDecodeException("a map with the key " + key + " twice");
            }
            map.put(key, value);
        }
        finished(content, "a map");
        return map;
    }

    private static AmqpArray array(Compound compound) throws AmqpDecodeException {
        AmqpDecoder content = compound.content();
        int code = content.u8();
        Object descriptor = null;
        if (code == 0x00) {
            content.enter();
            descriptor = content.read();
            content.depth--;
            code = content.u8();
        }
        AmqpType type = typeOf(code);
        long count = compound.count();
        if (widthless(code)) {
            if (count > MAX_ELEMENTS_WITHOUT_WIDTH) {
                throw new AmqpDecodeException("an array of " + count + " elements that take no octet");
            }
        } else {
            content.need(count);
        }
        List<Object> elements = new ArrayList<>((int) count);
        for (long i = 0; i < count; i++) {
            elements.add(content.primitive(code));
        }
        finished(content, "an array");
        return new AmqpArray(descriptor, type, elements);
    }

    private static boolean widthless(int code) {
        return code >= 0x40 && code <= 0x45;
    }

    private static AmqpType typeOf(int code) throws AmqpDecodeException {
        return switch (code) {
            case 0x40 -> AmqpType.NULL;
            case 0x41, 0x42, 0x56 -> AmqpType.BOOLEAN;
            case 0x50 -> AmqpType.UBYTE;
            case 0x60 -> AmqpType.USHORT;
            case 0x70, 0x52, 0x43 -> AmqpType.UINT;
            case 0x80, 0x53, 0x44 -> AmqpType.ULONG;
            case 0x51 -> AmqpType.BYTE;
            case 0x61 -> AmqpType.SHORT;
            case 0x71, 0x54 -> AmqpType.INT;
            case 0x81, 0x55 -> AmqpType.LONG;
            case 0x72 -> AmqpType.FLOAT;
            case 0x82 -> AmqpType.DOUBLE;
            case 0x74 -> AmqpType.DECIMAL32;
            case 0x84 -> AmqpType.DECIMAL64;
            case 0x94 -> AmqpType.DECIMAL128;
            case 0x73 -> AmqpType.CHAR;
            case 0x83 -> AmqpType.TIMESTAMP;
            case 0x98 -> AmqpType.UUID;
            case 0xA0, 0xB0 -> AmqpType.BINARY;
            case 0xA1, 0xB1 -> AmqpType.STRING;
            case 0xA3, 0xB3 -> AmqpType.SYMBOL;
            case 0x45, 0xC0, 0xD0 -> AmqpType.LIST;
            case 0xC1, 0xD1 -> AmqpType.MAP;
            case 0xE0, 0xF0 -> AmqpType.ARRAY;
            default -> throw new AmqpDecodeException(String.format("unknown format code 0x%02x in an array", code));
        };
    }
}
