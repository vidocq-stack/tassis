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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Writes values in the encodings of AMQP 1.0 (part 1, §1.6), always the shortest one the value
 * allows. What it accepts is the Java representation of each AMQP type: see {@link AmqpType}.
 */
public final class AmqpEncoder {

    private final Output out;

    private AmqpEncoder(Output out) {
        this.out = out;
    }

    /** Returns the encoding of {@code value}. */
    public static byte[] encode(Object value) {
        Output out = new Output(64);
        new AmqpEncoder(out).write(value);
        return out.octets();
    }

    /**
     * Returns the encodings of the values, one after the other, written to one array.
     *
     * @param expected about how many octets that will be: an array of that size is not grown
     */
    public static byte[] encodeAll(List<?> values, int expected) {
        Output out = new Output(expected);
        AmqpEncoder encoder = new AmqpEncoder(out);
        for (Object value : values) {
            encoder.write(value);
        }
        return out.octets();
    }

    /** A byte array that grows, written in network byte order. */
    private static final class Output extends ByteArrayOutputStream {

        Output(int expected) {
            super(expected);
        }

        /** What was written: the array itself when it is exactly full, which spares a copy. */
        byte[] octets() {
            return count == buf.length ? buf : toByteArray();
        }

        /** Appends what was written to another output, without an array in between. */
        void appendTo(Output other) {
            other.write(buf, 0, count);
        }

        void u8(int value) {
            write(value);
        }

        void u16(int value) {
            write(value >>> 8);
            write(value);
        }

        void u32(long value) {
            write((int) (value >>> 24));
            write((int) (value >>> 16));
            write((int) (value >>> 8));
            write((int) value);
        }

        void u64(long value) {
            u32(value >>> 32);
            u32(value);
        }

        void bytes(byte[] bytes) {
            write(bytes, 0, bytes.length);
        }
    }

    private void write(Object value) {
        switch (value) {
            case null -> out.u8(0x40);
            case Boolean b -> out.u8(b ? 0x41 : 0x42);
            case UByte u -> {
                out.u8(0x50);
                out.u8(u.bits());
            }
            case UShort u -> {
                out.u8(0x60);
                out.u16(u.bits());
            }
            case UInt u -> {
                long v = u.value();
                if (v == 0) {
                    out.u8(0x43);
                } else if (v <= 0xFF) {
                    out.u8(0x52);
                    out.u8((int) v);
                } else {
                    out.u8(0x70);
                    out.u32(v);
                }
            }
            case ULong u -> {
                long v = u.bits();
                if (v == 0) {
                    out.u8(0x44);
                } else if (v > 0 && v <= 0xFF) {
                    out.u8(0x53);
                    out.u8((int) v);
                } else {
                    out.u8(0x80);
                    out.u64(v);
                }
            }
            case Byte b -> {
                out.u8(0x51);
                out.u8(b);
            }
            case Short s -> {
                out.u8(0x61);
                out.u16(s);
            }
            case Integer i -> {
                if (i >= Byte.MIN_VALUE && i <= Byte.MAX_VALUE) {
                    out.u8(0x54);
                    out.u8(i);
                } else {
                    out.u8(0x71);
                    out.u32(i);
                }
            }
            case Long l -> {
                if (l >= Byte.MIN_VALUE && l <= Byte.MAX_VALUE) {
                    out.u8(0x55);
                    out.u8((int) (long) l);
                } else {
                    out.u8(0x81);
                    out.u64(l);
                }
            }
            case Float f -> {
                out.u8(0x72);
                out.u32(Float.floatToRawIntBits(f));
            }
            case Double d -> {
                out.u8(0x82);
                out.u64(Double.doubleToRawLongBits(d));
            }
            case Decimal32 d -> {
                out.u8(0x74);
                out.u32(d.bits());
            }
            case Decimal64 d -> {
                out.u8(0x84);
                out.u64(d.bits());
            }
            case Decimal128 d -> {
                out.u8(0x94);
                out.u64(d.mostSignificantBits());
                out.u64(d.leastSignificantBits());
            }
            case AmqpChar c -> {
                out.u8(0x73);
                out.u32(c.codePoint());
            }
            case Character c -> {
                out.u8(0x73);
                out.u32(c);
            }
            case Timestamp t -> {
                out.u8(0x83);
                out.u64(t.millis());
            }
            case UUID u -> {
                out.u8(0x98);
                out.u64(u.getMostSignificantBits());
                out.u64(u.getLeastSignificantBits());
            }
            case Binary b -> variable(0xA0, b.array());
            case byte[] b -> variable(0xA0, b);
            case String s -> variable(0xA1, s.getBytes(StandardCharsets.UTF_8));
            case Symbol s -> variable(0xA3, s.value().getBytes(StandardCharsets.US_ASCII));
            case List<?> list -> list(list);
            case Map<?, ?> map -> map(map);
            case AmqpArray array -> array(array);
            case Described described -> {
                out.u8(0x00);
                write(described.descriptor());
                write(described.value());
            }
            case DescribedType described -> {
                out.u8(0x00);
                write(described.descriptorCode());
                write(described.describedValue());
            }
            default -> throw new IllegalArgumentException(
                    value.getClass().getName() + " has no AMQP encoding");
        }
    }

    /** {@code code} is the one-octet-length form; the four-octet form is {@code code + 0x10} (§1.6.19). */
    private void variable(int code, byte[] bytes) {
        if (bytes.length <= 0xFF) {
            out.u8(code);
            out.u8(bytes.length);
        } else {
            out.u8(code + 0x10);
            out.u32(bytes.length);
        }
        out.bytes(bytes);
    }

    private void list(List<?> list) {
        if (list.isEmpty()) {
            out.u8(0x45);
            return;
        }
        Output items = new Output(64);
        AmqpEncoder nested = new AmqpEncoder(items);
        for (Object item : list) {
            nested.write(item);
        }
        compound(0xC0, list.size(), items);
    }

    private void map(Map<?, ?> map) {
        Output items = new Output(64);
        AmqpEncoder nested = new AmqpEncoder(items);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            nested.write(entry.getKey());
            nested.write(entry.getValue());
        }
        compound(0xC1, map.size() * 2, items);
    }

    /**
     * {@code code} is the one-octet form; the four-octet form is {@code code + 0x10}. The size
     * counts the count field and the items (§1.6.22).
     */
    private void compound(int code, int count, Output items) {
        if (items.size() + 1 <= 0xFF && count <= 0xFF) {
            out.u8(code);
            out.u8(items.size() + 1);
            out.u8(count);
        } else {
            out.u8(code + 0x10);
            out.u32(items.size() + 4L);
            out.u32(count);
        }
        items.appendTo(out);
    }

    /** The constructor is written once, then each element without one (§1.6.24). */
    private void array(AmqpArray array) {
        Output body = new Output(64);
        AmqpEncoder nested = new AmqpEncoder(body);
        if (array.descriptor() != null) {
            body.u8(0x00);
            nested.write(array.descriptor());
        }
        nested.elements(array.elementType(), array.elements());
        compound(0xE0, array.elements().size(), body);
    }

    /**
     * Writes the constructor shared by the elements, then the elements. All of them take the
     * same encoding, hence the full-width one for the types that have a shorter form.
     */
    private void elements(AmqpType type, List<?> elements) {
        switch (type) {
            case NULL -> {
                out.u8(0x40);
                for (Object element : elements) {
                    if (element != null) {
                        throw mismatch(type, element);
                    }
                }
            }
            case BOOLEAN -> {
                out.u8(0x56);
                for (Object element : elements) {
                    out.u8(as(Boolean.class, type, element) ? 1 : 0);
                }
            }
            case UBYTE -> {
                out.u8(0x50);
                for (Object element : elements) {
                    out.u8(as(UByte.class, type, element).bits());
                }
            }
            case USHORT -> {
                out.u8(0x60);
                for (Object element : elements) {
                    out.u16(as(UShort.class, type, element).bits());
                }
            }
            case UINT -> {
                out.u8(0x70);
                for (Object element : elements) {
                    out.u32(as(UInt.class, type, element).bits());
                }
            }
            case ULONG -> {
                out.u8(0x80);
                for (Object element : elements) {
                    out.u64(as(ULong.class, type, element).bits());
                }
            }
            case BYTE -> {
                out.u8(0x51);
                for (Object element : elements) {
                    out.u8(as(Byte.class, type, element));
                }
            }
            case SHORT -> {
                out.u8(0x61);
                for (Object element : elements) {
                    out.u16(as(Short.class, type, element));
                }
            }
            case INT -> {
                out.u8(0x71);
                for (Object element : elements) {
                    out.u32(as(Integer.class, type, element));
                }
            }
            case LONG -> {
                out.u8(0x81);
                for (Object element : elements) {
                    out.u64(as(Long.class, type, element));
                }
            }
            case FLOAT -> {
                out.u8(0x72);
                for (Object element : elements) {
                    out.u32(Float.floatToRawIntBits(as(Float.class, type, element)));
                }
            }
            case DOUBLE -> {
                out.u8(0x82);
                for (Object element : elements) {
                    out.u64(Double.doubleToRawLongBits(as(Double.class, type, element)));
                }
            }
            case DECIMAL32 -> {
                out.u8(0x74);
                for (Object element : elements) {
                    out.u32(as(Decimal32.class, type, element).bits());
                }
            }
            case DECIMAL64 -> {
                out.u8(0x84);
                for (Object element : elements) {
                    out.u64(as(Decimal64.class, type, element).bits());
                }
            }
            case DECIMAL128 -> {
                out.u8(0x94);
                for (Object element : elements) {
                    Decimal128 decimal = as(Decimal128.class, type, element);
                    out.u64(decimal.mostSignificantBits());
                    out.u64(decimal.leastSignificantBits());
                }
            }
            case CHAR -> {
                out.u8(0x73);
                for (Object element : elements) {
                    out.u32(as(AmqpChar.class, type, element).codePoint());
                }
            }
            case TIMESTAMP -> {
                out.u8(0x83);
                for (Object element : elements) {
                    out.u64(as(Timestamp.class, type, element).millis());
                }
            }
            case UUID -> {
                out.u8(0x98);
                for (Object element : elements) {
                    UUID uuid = as(UUID.class, type, element);
                    out.u64(uuid.getMostSignificantBits());
                    out.u64(uuid.getLeastSignificantBits());
                }
            }
            case BINARY -> variableElements(0xA0,
                    elements.stream().map(element -> as(Binary.class, type, element).array()).toList());
            case STRING -> variableElements(0xA1, elements.stream()
                    .map(element -> as(String.class, type, element).getBytes(StandardCharsets.UTF_8)).toList());
            case SYMBOL -> variableElements(0xA3, elements.stream()
                    .map(element -> as(Symbol.class, type, element).value().getBytes(StandardCharsets.US_ASCII))
                    .toList());
            case LIST -> {
                out.u8(0xD0);
                for (Object element : elements) {
                    Output items = new Output(64);
                    AmqpEncoder nested = new AmqpEncoder(items);
                    List<?> list = as(List.class, type, element);
                    for (Object item : list) {
                        nested.write(item);
                    }
                    wide(list.size(), items);
                }
            }
            case MAP -> {
                out.u8(0xD1);
                for (Object element : elements) {
                    Output items = new Output(64);
                    AmqpEncoder nested = new AmqpEncoder(items);
                    Map<?, ?> map = as(Map.class, type, element);
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        nested.write(entry.getKey());
                        nested.write(entry.getValue());
                    }
                    wide(map.size() * 2, items);
                }
            }
            case ARRAY -> {
                out.u8(0xF0);
                for (Object element : elements) {
                    AmqpArray array = as(AmqpArray.class, type, element);
                    Output body = new Output(64);
                    AmqpEncoder nested = new AmqpEncoder(body);
                    if (array.descriptor() != null) {
                        body.u8(0x00);
                        nested.write(array.descriptor());
                    }
                    nested.elements(array.elementType(), array.elements());
                    wide(array.elements().size(), body);
                }
            }
            default -> throw new IllegalArgumentException("no encoding for an array of " + type);
        }
    }

    /** The size and count of a compound in their four-octet form, then its content. */
    private void wide(int count, Output content) {
        out.u32(content.size() + 4L);
        out.u32(count);
        out.bytes(content.toByteArray());
    }

    private void variableElements(int code, List<byte[]> values) {
        boolean narrow = values.stream().allMatch(value -> value.length <= 0xFF);
        out.u8(narrow ? code : code + 0x10);
        for (byte[] value : values) {
            if (narrow) {
                out.u8(value.length);
            } else {
                out.u32(value.length);
            }
            out.bytes(value);
        }
    }

    private static <T> T as(Class<T> expected, AmqpType type, Object element) {
        if (!expected.isInstance(element)) {
            throw mismatch(type, element);
        }
        return expected.cast(element);
    }

    private static IllegalArgumentException mismatch(AmqpType type, Object element) {
        return new IllegalArgumentException("an array of " + type + " cannot hold " + element);
    }
}
