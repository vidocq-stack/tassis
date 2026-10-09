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

import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageEOFException;
import jakarta.jms.ObjectMessage;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import java.util.Collections;

/** Turns any {@link Message} into the provider's own representation. */
public final class Messages {

    private Messages() {
    }

    /**
     * Returns an independent Tassis copy of {@code message}. A message of
     * another provider's implementation is rebuilt through the
     * {@code jakarta.jms} interfaces alone (Jakarta Messaging 3.1 §3.12).
     */
    public static TassisMessage snapshot(Message message) throws JMSException {
        if (message instanceof TassisMessage own) {
            return own.copy();
        }
        TassisMessage copy = foreignBody(message);
        copy.setJMSMessageID(message.getJMSMessageID());
        copy.setJMSTimestamp(message.getJMSTimestamp());
        copy.setJMSCorrelationID(message.getJMSCorrelationID());
        copy.setJMSReplyTo(message.getJMSReplyTo());
        copy.setJMSDestination(message.getJMSDestination());
        copy.setJMSDeliveryMode(message.getJMSDeliveryMode());
        copy.setJMSRedelivered(message.getJMSRedelivered());
        copy.setJMSType(message.getJMSType());
        copy.setJMSExpiration(message.getJMSExpiration());
        copy.setJMSDeliveryTime(message.getJMSDeliveryTime());
        copy.setJMSPriority(message.getJMSPriority());
        for (Object name : Collections.list(message.getPropertyNames())) {
            copy.setObjectProperty((String) name, message.getObjectProperty((String) name));
        }
        return copy;
    }

    private static TassisMessage foreignBody(Message message) throws JMSException {
        switch (message) {
            case TextMessage text -> {
                TassisTextMessage copy = new TassisTextMessage();
                copy.setText(text.getText());
                return copy;
            }
            case ObjectMessage object -> {
                TassisObjectMessage copy = new TassisObjectMessage();
                copy.setObject(object.getObject());
                return copy;
            }
            case MapMessage map -> {
                TassisMapMessage copy = new TassisMapMessage();
                for (Object name : Collections.list(map.getMapNames())) {
                    copy.setObject((String) name, map.getObject((String) name));
                }
                return copy;
            }
            case BytesMessage bytes -> {
                // Reading requires the read-only mode; reset() is the only portable way in.
                bytes.reset();
                byte[] content = new byte[Math.toIntExact(bytes.getBodyLength())];
                bytes.readBytes(content);
                bytes.reset();
                TassisBytesMessage copy = new TassisBytesMessage();
                copy.writeBytes(content);
                return copy;
            }
            case StreamMessage stream -> {
                stream.reset();
                TassisStreamMessage copy = new TassisStreamMessage();
                try {
                    while (true) {
                        copy.writeObject(stream.readObject());
                    }
                } catch (MessageEOFException endOfStream) {
                    stream.reset();
                }
                return copy;
            }
            default -> {
                return new TassisMessage();
            }
        }
    }
}
