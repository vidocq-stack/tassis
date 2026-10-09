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

import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.MessageNotWriteableException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A message without a body, and the base of every other message type: header
 * fields (Jakarta Messaging 3.1 §3.4) and properties (§3.5).
 */
public class TassisMessage implements Message {

    /** Names a selector reserves, which therefore cannot be identifiers (§3.8.1.1). */
    private static final Set<String> RESERVED_NAMES =
            Set.of("NULL", "TRUE", "FALSE", "NOT", "AND", "OR", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE");

    private String messageId;
    private long timestamp;
    private String correlationId;
    private byte[] correlationIdBytes;
    private Destination replyTo;
    private Destination destination;
    private int deliveryMode = DEFAULT_DELIVERY_MODE;
    private boolean redelivered;
    private String type;
    private long expiration;
    private long deliveryTime;
    private int priority = DEFAULT_PRIORITY;

    private final Map<String, Object> properties = new LinkedHashMap<>();
    private boolean propertiesReadOnly;
    private boolean bodyReadOnly;

    /**
     * Puts this message in the state a client receives it in: properties and
     * body read-only, body positioned at its start.
     */
    public final void markReceived() {
        propertiesReadOnly = true;
        makeBodyReadOnly();
    }

    // ---- header fields -------------------------------------------------

    @Override
    public String getJMSMessageID() {
        return messageId;
    }

    @Override
    public void setJMSMessageID(String id) {
        messageId = id;
    }

    @Override
    public long getJMSTimestamp() {
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        if (correlationIdBytes != null) {
            return correlationIdBytes.clone();
        }
        return correlationId == null ? null : correlationId.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) {
        correlationIdBytes = correlationID == null ? null : correlationID.clone();
        correlationId = correlationID == null ? null : new String(correlationID, StandardCharsets.UTF_8);
    }

    @Override
    public void setJMSCorrelationID(String correlationID) {
        correlationId = correlationID;
        correlationIdBytes = null;
    }

    @Override
    public String getJMSCorrelationID() {
        return correlationId;
    }

    @Override
    public Destination getJMSReplyTo() {
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) {
        this.replyTo = replyTo;
    }

    @Override
    public Destination getJMSDestination() {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) {
        this.destination = destination;
    }

    @Override
    public int getJMSDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getJMSRedelivered() {
        return redelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) {
        this.redelivered = redelivered;
    }

    @Override
    public String getJMSType() {
        return type;
    }

    @Override
    public void setJMSType(String type) {
        this.type = type;
    }

    @Override
    public long getJMSExpiration() {
        return expiration;
    }

    @Override
    public void setJMSExpiration(long expiration) {
        this.expiration = expiration;
    }

    @Override
    public long getJMSDeliveryTime() {
        return deliveryTime;
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) {
        this.deliveryTime = deliveryTime;
    }

    @Override
    public int getJMSPriority() {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) {
        this.priority = priority;
    }

    // ---- properties ----------------------------------------------------

    @Override
    public void clearProperties() {
        properties.clear();
        propertiesReadOnly = false;
    }

    @Override
    public boolean propertyExists(String name) {
        return properties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return ValueConversions.toBoolean(properties.get(name));
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return ValueConversions.toByte(properties.get(name));
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return ValueConversions.toShort(properties.get(name));
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return ValueConversions.toInt(properties.get(name));
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return ValueConversions.toLong(properties.get(name));
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return ValueConversions.toFloat(properties.get(name));
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return ValueConversions.toDouble(properties.get(name));
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return ValueConversions.toString(properties.get(name));
    }

    @Override
    public Object getObjectProperty(String name) {
        return properties.get(name);
    }

    @Override
    public Enumeration<String> getPropertyNames() {
        return Collections.enumeration(new ArrayList<>(properties.keySet()));
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        putProperty(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        putProperty(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        putProperty(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        putProperty(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        putProperty(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        putProperty(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        putProperty(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        putProperty(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        if (value != null && !ValueConversions.isPropertyValue(value)) {
            throw new MessageFormatException(value.getClass().getName() + " is not a supported property type");
        }
        putProperty(name, value);
    }

    private void putProperty(String name, Object value) throws JMSException {
        if (propertiesReadOnly) {
            throw new MessageNotWriteableException("the properties of a received message are read-only");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("a property name must not be null or empty");
        }
        if (!isIdentifier(name)) {
            throw new MessageFormatException("'" + name + "' is not a valid property name");
        }
        properties.put(name, value);
    }

    /** A property name obeys the rules of a message selector identifier (§3.5.1, §3.8.1.1). */
    private static boolean isIdentifier(String name) {
        return Character.isJavaIdentifierStart(name.charAt(0))
                && name.chars().skip(1).allMatch(Character::isJavaIdentifierPart)
                && !RESERVED_NAMES.contains(name.toUpperCase(Locale.ROOT));
    }

    // ---- acknowledgement -----------------------------------------------

    @Override
    public void acknowledge() {
        throw new UnsupportedOperationException("acknowledge(): see ROADMAP M3");
    }

    // ---- body ----------------------------------------------------------

    @Override
    public final void clearBody() throws JMSException {
        clearBodyContent();
        bodyReadOnly = false;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        Object body = body();
        if (body == null) {
            return null;
        }
        if (!c.isAssignableFrom(bodyType(body))) {
            throw new MessageFormatException("the message body cannot be assigned to " + c.getName());
        }
        @SuppressWarnings("unchecked")
        T assigned = (T) body;
        return assigned;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        Object body;
        try {
            body = body();
        } catch (MessageFormatException e) {
            return false;
        }
        return body == null || c.isAssignableFrom(bodyType(body));
    }

    /** Returns the body as {@code getBody} hands it out, or {@code null} when the message has none. */
    Object body() throws JMSException {
        return null;
    }

    /** Returns the type {@code getBody}'s argument is checked against. */
    Class<?> bodyType(Object body) {
        return body.getClass();
    }

    /** Empties the body. */
    void clearBodyContent() {
    }

    /** Switches the body to read-only; stream-like bodies also rewind. */
    void makeBodyReadOnly() {
        setBodyReadOnly();
    }

    final void setBodyReadOnly() {
        bodyReadOnly = true;
    }

    final boolean isBodyReadOnly() {
        return bodyReadOnly;
    }

    final void checkBodyWritable() throws MessageNotWriteableException {
        if (bodyReadOnly) {
            throw new MessageNotWriteableException("the message body is read-only");
        }
    }

    final void checkBodyReadable() throws MessageNotReadableException {
        if (!bodyReadOnly) {
            throw new MessageNotReadableException("the message body is write-only until reset() is called");
        }
    }
}
