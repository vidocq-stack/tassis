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
package io.vidocq.tassis.core.internal.client;

import io.vidocq.tassis.core.internal.message.TassisMessage;
import jakarta.jms.BytesMessage;
import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.TextMessage;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The producer of the simplified API (Jakarta Messaging 3.1 §2.8, §7.10): it carries
 * the send options, headers and properties to apply to what it sends, and names
 * the destination on each send.
 */
final class TassisJMSProducer implements JMSProducer {

    private final TassisContext context;
    // Created with the first send: a context that only receives needs no producer.
    private MessageProducer producer;

    private boolean disableMessageId;
    private boolean disableMessageTimestamp;
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;
    private CompletionListener completionListener;

    // The properties to set on each message, held by a message so that names, types and
    // conversions follow exactly the rules of message properties (§3.5).
    private final TassisMessage properties = new TassisMessage();
    private final TassisMessage headers = new TassisMessage();
    // Whether the correlation ID was given as bytes, which a String would not carry faithfully.
    private boolean correlationIdAsBytes;

    TassisJMSProducer(TassisContext context) {
        this.context = context;
    }

    // ---- sending -----------------------------------------------------------

    @Override
    public JMSProducer send(Destination destination, Message message) {
        if (message == null) {
            throw new MessageFormatRuntimeException("the message is null");
        }
        Unchecked.run(() -> {
            // What was set on the producer overrides what the message already carries.
            if (correlationIdAsBytes) {
                message.setJMSCorrelationIDAsBytes(headers.getJMSCorrelationIDAsBytes());
            } else if (headers.getJMSCorrelationID() != null) {
                message.setJMSCorrelationID(headers.getJMSCorrelationID());
            }
            if (headers.getJMSType() != null) {
                message.setJMSType(headers.getJMSType());
            }
            if (headers.getJMSReplyTo() != null) {
                message.setJMSReplyTo(headers.getJMSReplyTo());
            }
            for (String name : Collections.list(properties.getPropertyNames())) {
                message.setObjectProperty(name, properties.getObjectProperty(name));
            }
            if (producer == null) {
                producer = context.session().createProducer(null);
            }
            producer.setDisableMessageID(disableMessageId);
            producer.setDisableMessageTimestamp(disableMessageTimestamp);
            producer.setDeliveryDelay(deliveryDelay);
            if (completionListener == null) {
                producer.send(destination, message, deliveryMode, priority, timeToLive);
            } else {
                producer.send(destination, message, deliveryMode, priority, timeToLive, completionListener);
            }
        });
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        TextMessage message = context.createTextMessage();
        Unchecked.run(() -> message.setText(body));
        return send(destination, message);
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        MapMessage message = context.createMapMessage();
        if (body != null) {
            Unchecked.run(() -> {
                for (Map.Entry<String, Object> entry : body.entrySet()) {
                    message.setObject(entry.getKey(), entry.getValue());
                }
            });
        }
        return send(destination, message);
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        BytesMessage message = context.createBytesMessage();
        if (body != null) {
            Unchecked.run(() -> message.writeBytes(body));
        }
        return send(destination, message);
    }

    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        ObjectMessage message = context.createObjectMessage();
        Unchecked.run(() -> message.setObject(body));
        return send(destination, message);
    }

    // ---- send options ------------------------------------------------------

    @Override
    public JMSProducer setDisableMessageID(boolean value) {
        disableMessageId = value;
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        return disableMessageId;
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean value) {
        disableMessageTimestamp = value;
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new JMSRuntimeException("unknown delivery mode " + deliveryMode);
        }
        this.deliveryMode = deliveryMode;
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public JMSProducer setPriority(int priority) {
        if (priority < 0 || priority > 9) {
            throw new JMSRuntimeException("priority " + priority + " is outside 0..9");
        }
        this.priority = priority;
        return this;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        this.deliveryDelay = deliveryDelay;
        return this;
    }

    @Override
    public long getDeliveryDelay() {
        return deliveryDelay;
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        this.completionListener = completionListener;
        return this;
    }

    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    // ---- message properties ------------------------------------------------

    @Override
    public JMSProducer setProperty(String name, boolean value) {
        Unchecked.run(() -> properties.setBooleanProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        Unchecked.run(() -> properties.setByteProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        Unchecked.run(() -> properties.setShortProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        Unchecked.run(() -> properties.setIntProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        Unchecked.run(() -> properties.setLongProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        Unchecked.run(() -> properties.setFloatProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        Unchecked.run(() -> properties.setDoubleProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        Unchecked.run(() -> properties.setStringProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        Unchecked.run(() -> properties.setObjectProperty(name, value));
        return this;
    }

    @Override
    public JMSProducer clearProperties() {
        properties.clearProperties();
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        return properties.propertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return Unchecked.get(() -> properties.getBooleanProperty(name));
    }

    @Override
    public byte getByteProperty(String name) {
        return Unchecked.get(() -> properties.getByteProperty(name));
    }

    @Override
    public short getShortProperty(String name) {
        return Unchecked.get(() -> properties.getShortProperty(name));
    }

    @Override
    public int getIntProperty(String name) {
        return Unchecked.get(() -> properties.getIntProperty(name));
    }

    @Override
    public long getLongProperty(String name) {
        return Unchecked.get(() -> properties.getLongProperty(name));
    }

    @Override
    public float getFloatProperty(String name) {
        return Unchecked.get(() -> properties.getFloatProperty(name));
    }

    @Override
    public double getDoubleProperty(String name) {
        return Unchecked.get(() -> properties.getDoubleProperty(name));
    }

    @Override
    public String getStringProperty(String name) {
        return Unchecked.get(() -> properties.getStringProperty(name));
    }

    @Override
    public Object getObjectProperty(String name) {
        return properties.getObjectProperty(name);
    }

    @Override
    public Set<String> getPropertyNames() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(Collections.list(properties.getPropertyNames())));
    }

    // ---- message headers ---------------------------------------------------

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        headers.setJMSCorrelationIDAsBytes(correlationID);
        correlationIdAsBytes = correlationID != null;
        return this;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return headers.getJMSCorrelationIDAsBytes();
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        headers.setJMSCorrelationID(correlationID);
        correlationIdAsBytes = false;
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return headers.getJMSCorrelationID();
    }

    @Override
    public JMSProducer setJMSType(String type) {
        headers.setJMSType(type);
        return this;
    }

    @Override
    public String getJMSType() {
        return headers.getJMSType();
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        headers.setJMSReplyTo(replyTo);
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return headers.getJMSReplyTo();
    }
}
