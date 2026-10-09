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
package io.vidocq.tassis.core.internal.message;

import jakarta.jms.MessageFormatException;

/**
 * The value conversion table shared by message properties (Jakarta Messaging 3.1
 * §3.5.4) and by {@code MapMessage} / {@code StreamMessage} bodies (§3.11.3). A
 * value written as one type can be read as the types the table marks; every
 * other combination is a {@link MessageFormatException}.
 *
 * <p>A {@code null} value is read as if the target type's {@code valueOf(String)}
 * had been called with {@code null}, which is why the numeric conversions let
 * {@link NumberFormatException} and {@link NullPointerException} escape.
 */
final class ValueConversions {

    private ValueConversions() {
    }

    static boolean toBoolean(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> Boolean.valueOf((String) null);
            case Boolean b -> b;
            case String s -> Boolean.valueOf(s);
            default -> throw unsupported(value, "boolean");
        };
    }

    static byte toByte(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> Byte.valueOf((String) null);
            case Byte b -> b;
            case String s -> Byte.valueOf(s);
            default -> throw unsupported(value, "byte");
        };
    }

    static short toShort(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> Short.valueOf((String) null);
            case Byte b -> b;
            case Short s -> s;
            case String s -> Short.valueOf(s);
            default -> throw unsupported(value, "short");
        };
    }

    static char toChar(Object value) throws MessageFormatException {
        return switch (value) {
            // char has no String conversion to delegate the null case to.
            case null -> throw new NullPointerException("null cannot be read as a char");
            case Character c -> c;
            default -> throw unsupported(value, "char");
        };
    }

    static int toInt(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> Integer.valueOf((String) null);
            case Byte b -> b;
            case Short s -> s;
            case Integer i -> i;
            case String s -> Integer.valueOf(s);
            default -> throw unsupported(value, "int");
        };
    }

    static long toLong(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> Long.valueOf((String) null);
            case Byte b -> b;
            case Short s -> s;
            case Integer i -> i;
            case Long l -> l;
            case String s -> Long.valueOf(s);
            default -> throw unsupported(value, "long");
        };
    }

    static float toFloat(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> Float.valueOf((String) null);
            case Float f -> f;
            case String s -> Float.valueOf(s);
            default -> throw unsupported(value, "float");
        };
    }

    static double toDouble(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> Double.valueOf((String) null);
            case Float f -> f;
            case Double d -> d;
            case String s -> Double.valueOf(s);
            default -> throw unsupported(value, "double");
        };
    }

    static String toString(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> null;
            case byte[] bytes -> throw unsupported(bytes, "String");
            default -> value.toString();
        };
    }

    static byte[] toBytes(Object value) throws MessageFormatException {
        return switch (value) {
            case null -> null;
            case byte[] bytes -> bytes.clone();
            default -> throw unsupported(value, "byte[]");
        };
    }

    /** Returns whether {@code value} is of a type a message property may hold. */
    static boolean isPropertyValue(Object value) {
        return value instanceof Boolean || value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long || value instanceof Float
                || value instanceof Double || value instanceof String;
    }

    /**
     * Returns {@code value} as a map or stream body may store it: unchanged, or a
     * private copy for a byte array.
     */
    static Object bodyValue(Object value) throws MessageFormatException {
        if (value == null || value instanceof Character || isPropertyValue(value)) {
            return value;
        }
        if (value instanceof byte[] bytes) {
            return bytes.clone();
        }
        throw new MessageFormatException(value.getClass().getName() + " is not a supported value type");
    }

    private static MessageFormatException unsupported(Object value, String target) {
        return new MessageFormatException(
                "a " + value.getClass().getSimpleName() + " value cannot be read as " + target);
    }
}
