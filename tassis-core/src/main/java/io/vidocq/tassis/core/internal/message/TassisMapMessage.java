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

import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;

/** A message whose body is a set of name-value pairs (Jakarta Messaging 3.1 §3.11). */
public final class TassisMapMessage extends TassisMessage implements MapMessage {

    private final Map<String, Object> entries = new LinkedHashMap<>();

    @Override
    public boolean getBoolean(String name) throws JMSException {
        return ValueConversions.toBoolean(entries.get(name));
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return ValueConversions.toByte(entries.get(name));
    }

    @Override
    public short getShort(String name) throws JMSException {
        return ValueConversions.toShort(entries.get(name));
    }

    @Override
    public char getChar(String name) throws JMSException {
        return ValueConversions.toChar(entries.get(name));
    }

    @Override
    public int getInt(String name) throws JMSException {
        return ValueConversions.toInt(entries.get(name));
    }

    @Override
    public long getLong(String name) throws JMSException {
        return ValueConversions.toLong(entries.get(name));
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return ValueConversions.toFloat(entries.get(name));
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return ValueConversions.toDouble(entries.get(name));
    }

    @Override
    public String getString(String name) throws JMSException {
        return ValueConversions.toString(entries.get(name));
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return ValueConversions.toBytes(entries.get(name));
    }

    @Override
    public Object getObject(String name) {
        return entries.get(name) instanceof byte[] bytes ? bytes.clone() : entries.get(name);
    }

    @Override
    public Enumeration<String> getMapNames() {
        return Collections.enumeration(new ArrayList<>(entries.keySet()));
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setByte(String name, byte value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setShort(String name, short value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setChar(String name, char value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setInt(String name, int value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setLong(String name, long value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setFloat(String name, float value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setDouble(String name, double value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setString(String name, String value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {
        put(name, value == null ? null : value.clone());
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
        put(name, Arrays.copyOfRange(value, offset, offset + length));
    }

    @Override
    public void setObject(String name, Object value) throws JMSException {
        put(name, ValueConversions.bodyValue(value));
    }

    @Override
    public boolean itemExists(String name) {
        return entries.containsKey(name);
    }

    private void put(String name, Object value) throws JMSException {
        checkBodyWritable();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("a map entry name must not be null or empty");
        }
        entries.put(name, value);
    }

    @Override
    Object body() {
        Map<String, Object> copy = new LinkedHashMap<>(entries);
        copy.replaceAll((name, value) -> value instanceof byte[] bytes ? bytes.clone() : value);
        return copy;
    }

    @Override
    Class<?> bodyType(Object body) {
        return Map.class;
    }

    @Override
    void clearBodyContent() {
        entries.clear();
    }
}
