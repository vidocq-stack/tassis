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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The fields of a composite type, which travels as a described list (§1.3): reading them by
 * position with the checks the definition of each field calls for, and writing them back.
 * Fields left out at the end of the list are null, like those sent as null.
 */
public final class Fields {

    private final String type;
    private final List<?> values;

    private Fields(String type, List<?> values) {
        this.type = type;
        this.values = values;
    }

    /**
     * @param type the name of the composite type, for the messages
     * @param value what its descriptor describes, which must be a list
     */
    public static Fields of(String type, Object value) throws AmqpDecodeException {
        if (!(value instanceof List<?> list)) {
            throw new AmqpDecodeException(type + " is a list of fields, not " + describe(value));
        }
        return new Fields(type, list);
    }

    private static String describe(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName();
    }

    /** The field at that position, whatever its type, or {@code null} if it is absent. */
    public Object any(int index) {
        return index < values.size() ? values.get(index) : null;
    }

    public <T> T get(int index, String name, Class<T> expected) throws AmqpDecodeException {
        Object value = any(index);
        if (value != null && !expected.isInstance(value)) {
            throw new AmqpDecodeException("the field " + name + " of " + type + " is a "
                    + expected.getSimpleName() + ", not " + describe(value));
        }
        return expected.cast(value);
    }

    /** Checks a mandatory field, which a peer may neither leave out nor send as null (§1.3). */
    public <T> T required(String name, T value) throws AmqpDecodeException {
        if (value == null) {
            throw new AmqpDecodeException("the mandatory field " + name + " of " + type + " is missing");
        }
        return value;
    }

    public Long uint(int index, String name) throws AmqpDecodeException {
        UInt value = get(index, name, UInt.class);
        return value == null ? null : value.value();
    }

    public Integer ubyte(int index, String name) throws AmqpDecodeException {
        UByte value = get(index, name, UByte.class);
        return value == null ? null : value.value();
    }

    public Integer ushort(int index, String name) throws AmqpDecodeException {
        UShort value = get(index, name, UShort.class);
        return value == null ? null : value.value();
    }

    /**
     * A field that takes several symbols: an array of them, or one alone. No value and an empty
     * array mean the same, and both are returned as {@code null} (§1.4).
     */
    public List<Symbol> symbols(int index, String name) throws AmqpDecodeException {
        Object value = any(index);
        switch (value) {
            case null -> {
                return null;
            }
            case Symbol symbol -> {
                return List.of(symbol);
            }
            case AmqpArray array when array.elementType() == AmqpType.SYMBOL && array.descriptor() == null -> {
                List<Symbol> symbols = new ArrayList<>();
                for (Object element : array.elements()) {
                    symbols.add((Symbol) element);
                }
                return symbols.isEmpty() ? null : List.copyOf(symbols);
            }
            default -> throw new AmqpDecodeException(
                    "the field " + name + " of " + type + " takes symbols, not " + describe(value));
        }
    }

    /** A field of type {@code fields}: a map whose keys are symbols (§2.8.13). */
    public Map<Symbol, Object> symbolKeyed(int index, String name) throws AmqpDecodeException {
        Map<?, ?> map = get(index, name, Map.class);
        if (map == null) {
            return null;
        }
        Map<Symbol, Object> fields = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof Symbol key)) {
                throw new AmqpDecodeException("the keys of the field " + name + " of " + type
                        + " are symbols, unlike " + describe(entry.getKey()));
            }
            fields.put(key, entry.getValue());
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    public Map<Object, Object> map(int index, String name) throws AmqpDecodeException {
        return (Map<Object, Object>) get(index, name, Map.class);
    }

    // ---- writing -----------------------------------------------------------

    /** The list of fields to write: the nulls at its end are left out (§1.3). */
    public static List<Object> list(Object... fields) {
        int length = fields.length;
        while (length > 0 && fields[length - 1] == null) {
            length--;
        }
        return new ArrayList<>(Arrays.asList(fields).subList(0, length));
    }

    public static UInt uint(Long value) {
        return value == null ? null : UInt.of(value);
    }

    public static UByte ubyte(Integer value) {
        return value == null ? null : UByte.of(value);
    }

    public static UShort ushort(Integer value) {
        return value == null ? null : UShort.of(value);
    }

    /** Several symbols are written as an array, none as null. */
    public static AmqpArray symbols(List<Symbol> symbols) {
        return symbols == null || symbols.isEmpty() ? null : AmqpArray.ofSymbols(symbols);
    }
}
