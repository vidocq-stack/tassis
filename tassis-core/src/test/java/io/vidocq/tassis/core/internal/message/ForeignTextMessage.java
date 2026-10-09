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
import jakarta.jms.TextMessage;
import java.util.Enumeration;

/**
 * A {@code TextMessage} that is not a Tassis message, as another provider's
 * would be: the provider can only reach it through the {@code jakarta.jms}
 * interfaces.
 */
public final class ForeignTextMessage implements TextMessage {

    private final TassisTextMessage delegate = new TassisTextMessage();

    @Override
    public String getJMSMessageID() throws JMSException {
        return delegate.getJMSMessageID();
    }

    @Override
    public void setJMSMessageID(String v) throws JMSException {
        delegate.setJMSMessageID(v);
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return delegate.getJMSTimestamp();
    }

    @Override
    public void setJMSTimestamp(long v) throws JMSException {
        delegate.setJMSTimestamp(v);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return delegate.getJMSCorrelationIDAsBytes();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] v) throws JMSException {
        delegate.setJMSCorrelationIDAsBytes(v);
    }

    @Override
    public void setJMSCorrelationID(String v) throws JMSException {
        delegate.setJMSCorrelationID(v);
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return delegate.getJMSCorrelationID();
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return delegate.getJMSReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination v) throws JMSException {
        delegate.setJMSReplyTo(v);
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return delegate.getJMSDestination();
    }

    @Override
    public void setJMSDestination(Destination v) throws JMSException {
        delegate.setJMSDestination(v);
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return delegate.getJMSDeliveryMode();
    }

    @Override
    public void setJMSDeliveryMode(int v) throws JMSException {
        delegate.setJMSDeliveryMode(v);
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return delegate.getJMSRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean v) throws JMSException {
        delegate.setJMSRedelivered(v);
    }

    @Override
    public String getJMSType() throws JMSException {
        return delegate.getJMSType();
    }

    @Override
    public void setJMSType(String v) throws JMSException {
        delegate.setJMSType(v);
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return delegate.getJMSExpiration();
    }

    @Override
    public void setJMSExpiration(long v) throws JMSException {
        delegate.setJMSExpiration(v);
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return delegate.getJMSDeliveryTime();
    }

    @Override
    public void setJMSDeliveryTime(long v) throws JMSException {
        delegate.setJMSDeliveryTime(v);
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return delegate.getJMSPriority();
    }

    @Override
    public void setJMSPriority(int v) throws JMSException {
        delegate.setJMSPriority(v);
    }

    @Override
    public void clearProperties() throws JMSException {
        delegate.clearProperties();
    }

    @Override
    public boolean propertyExists(String n) throws JMSException {
        return delegate.propertyExists(n);
    }

    @Override
    public boolean getBooleanProperty(String n) throws JMSException {
        return delegate.getBooleanProperty(n);
    }

    @Override
    public byte getByteProperty(String n) throws JMSException {
        return delegate.getByteProperty(n);
    }

    @Override
    public short getShortProperty(String n) throws JMSException {
        return delegate.getShortProperty(n);
    }

    @Override
    public int getIntProperty(String n) throws JMSException {
        return delegate.getIntProperty(n);
    }

    @Override
    public long getLongProperty(String n) throws JMSException {
        return delegate.getLongProperty(n);
    }

    @Override
    public float getFloatProperty(String n) throws JMSException {
        return delegate.getFloatProperty(n);
    }

    @Override
    public double getDoubleProperty(String n) throws JMSException {
        return delegate.getDoubleProperty(n);
    }

    @Override
    public String getStringProperty(String n) throws JMSException {
        return delegate.getStringProperty(n);
    }

    @Override
    public Object getObjectProperty(String n) throws JMSException {
        return delegate.getObjectProperty(n);
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException {
        return delegate.getPropertyNames();
    }

    @Override
    public void setBooleanProperty(String n, boolean v) throws JMSException {
        delegate.setBooleanProperty(n, v);
    }

    @Override
    public void setByteProperty(String n, byte v) throws JMSException {
        delegate.setByteProperty(n, v);
    }

    @Override
    public void setShortProperty(String n, short v) throws JMSException {
        delegate.setShortProperty(n, v);
    }

    @Override
    public void setIntProperty(String n, int v) throws JMSException {
        delegate.setIntProperty(n, v);
    }

    @Override
    public void setLongProperty(String n, long v) throws JMSException {
        delegate.setLongProperty(n, v);
    }

    @Override
    public void setFloatProperty(String n, float v) throws JMSException {
        delegate.setFloatProperty(n, v);
    }

    @Override
    public void setDoubleProperty(String n, double v) throws JMSException {
        delegate.setDoubleProperty(n, v);
    }

    @Override
    public void setStringProperty(String n, String v) throws JMSException {
        delegate.setStringProperty(n, v);
    }

    @Override
    public void setObjectProperty(String n, Object v) throws JMSException {
        delegate.setObjectProperty(n, v);
    }

    @Override
    public void acknowledge() throws JMSException {
        delegate.acknowledge();
    }

    @Override
    public void clearBody() throws JMSException {
        delegate.clearBody();
    }

    @Override
    public void setText(String v) throws JMSException {
        delegate.setText(v);
    }

    @Override
    public String getText() throws JMSException {
        return delegate.getText();
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return delegate.getBody(c);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return delegate.isBodyAssignableTo(c);
    }
}
