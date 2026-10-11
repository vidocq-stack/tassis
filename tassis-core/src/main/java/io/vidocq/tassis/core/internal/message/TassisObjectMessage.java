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
import jakarta.jms.MessageFormatException;
import jakarta.jms.ObjectMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A message whose body is a serializable object (Jakarta Messaging 3.1 §3.11).
 * The object is serialized when set, so the message holds a snapshot of it.
 */
public final class TassisObjectMessage extends TassisMessage implements ObjectMessage {

    private byte[] serialized;

    @Override
    public void setObject(Serializable object) throws JMSException {
        checkBodyWritable();
        if (object == null) {
            serialized = null;
            return;
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(object);
        } catch (IOException e) {
            throw formatException("the object could not be serialized", e);
        }
        serialized = bytes.toByteArray();
    }

    @Override
    public Serializable getObject() throws JMSException {
        if (serialized == null) {
            return null;
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            return (Serializable) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw formatException("the object could not be deserialized", e);
        }
    }

    /**
     * The body as it is held: the serialized form of the object, or {@code null} for none.
     * This is what travels: a broker carries an object message without ever deserializing it.
     */
    public byte[] serialized() {
        return serialized == null ? null : serialized.clone();
    }

    /** Sets the body from the serialized form of an object, which is not looked into. */
    public void setSerialized(byte[] serializedObject) throws JMSException {
        checkBodyWritable();
        serialized = serializedObject == null ? null : serializedObject.clone();
    }

    @Override
    TassisMessage newInstance() {
        return new TassisObjectMessage();
    }

    @Override
    void copyBodyTo(TassisMessage target) {
        // The serialized form is never modified in place, so it can be shared.
        ((TassisObjectMessage) target).serialized = serialized;
    }

    @Override
    Object body() throws JMSException {
        return getObject();
    }

    @Override
    void clearBodyContent() {
        serialized = null;
    }

    private static MessageFormatException formatException(String reason, Exception cause) {
        MessageFormatException e = new MessageFormatException(reason + ": " + cause);
        e.setLinkedException(cause);
        e.initCause(cause);
        return e;
    }
}
