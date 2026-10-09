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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Header fields (Jakarta Messaging 3.1 §3.4), properties (§3.5) and the body
 * access of a body-less message (§3.11, {@code Message.getBody}).
 */
class TassisMessageTest {

    private final TassisMessage message = new TassisMessage();

    @Test
    void headerFieldsRoundTrip() throws JMSException {
        message.setJMSMessageID("ID:1");
        message.setJMSTimestamp(10L);
        message.setJMSCorrelationID("corr");
        message.setJMSType("order");
        message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        message.setJMSRedelivered(true);
        message.setJMSExpiration(20L);
        message.setJMSDeliveryTime(30L);
        message.setJMSPriority(9);

        assertEquals("ID:1", message.getJMSMessageID());
        assertEquals(10L, message.getJMSTimestamp());
        assertEquals("corr", message.getJMSCorrelationID());
        assertEquals("order", message.getJMSType());
        assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());
        assertTrue(message.getJMSRedelivered());
        assertEquals(20L, message.getJMSExpiration());
        assertEquals(30L, message.getJMSDeliveryTime());
        assertEquals(9, message.getJMSPriority());
    }

    @Test
    void newMessageCarriesTheDefaultDeliveryModeAndPriority() throws JMSException {
        assertEquals(Message.DEFAULT_DELIVERY_MODE, message.getJMSDeliveryMode());
        assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());
        assertNull(message.getJMSDestination());
        assertNull(message.getJMSReplyTo());
    }

    @Test
    void correlationIdIsReadableAsBytesAndAsString() throws JMSException {
        byte[] id = "corr".getBytes(StandardCharsets.UTF_8);

        message.setJMSCorrelationIDAsBytes(id);
        id[0] = 'X';

        assertArrayEquals("corr".getBytes(StandardCharsets.UTF_8), message.getJMSCorrelationIDAsBytes(),
                "the message must keep its own copy of the bytes");
        assertEquals("corr", message.getJMSCorrelationID());

        message.setJMSCorrelationID("other");
        assertArrayEquals("other".getBytes(StandardCharsets.UTF_8), message.getJMSCorrelationIDAsBytes());

        message.setJMSCorrelationID(null);
        assertNull(message.getJMSCorrelationIDAsBytes());
    }

    @Test
    void propertiesRoundTripInTheirOwnType() throws JMSException {
        message.setBooleanProperty("b", true);
        message.setByteProperty("by", (byte) 1);
        message.setShortProperty("s", (short) 2);
        message.setIntProperty("i", 3);
        message.setLongProperty("l", 4L);
        message.setFloatProperty("f", 5.5f);
        message.setDoubleProperty("d", 6.5d);
        message.setStringProperty("str", "seven");

        assertTrue(message.getBooleanProperty("b"));
        assertEquals((byte) 1, message.getByteProperty("by"));
        assertEquals((short) 2, message.getShortProperty("s"));
        assertEquals(3, message.getIntProperty("i"));
        assertEquals(4L, message.getLongProperty("l"));
        assertEquals(5.5f, message.getFloatProperty("f"));
        assertEquals(6.5d, message.getDoubleProperty("d"));
        assertEquals("seven", message.getStringProperty("str"));
        assertEquals(Integer.valueOf(3), message.getObjectProperty("i"));
        assertEquals(List.of("b", "by", "s", "i", "l", "f", "d", "str"),
                Collections.list(message.getPropertyNames()));
    }

    @Test
    void propertyIsConvertedAlongTheSpecTable() throws JMSException {
        message.setByteProperty("by", (byte) 1);
        message.setStringProperty("number", "42");

        assertEquals(1L, message.getLongProperty("by"), "byte widens to long");
        assertEquals("1", message.getStringProperty("by"), "every type reads as String");
        assertEquals(42, message.getIntProperty("number"), "String converts through valueOf");
        assertThrows(MessageFormatException.class, () -> message.getBooleanProperty("by"),
                "byte to boolean is not a marked case of the table");
    }

    @Test
    void absentPropertyReadsAsNull() throws JMSException {
        assertFalse(message.propertyExists("missing"));
        assertNull(message.getStringProperty("missing"));
        assertNull(message.getObjectProperty("missing"));
        assertFalse(message.getBooleanProperty("missing"), "Boolean.valueOf(null) is false");
        assertThrows(NumberFormatException.class, () -> message.getIntProperty("missing"));
        assertThrows(NullPointerException.class, () -> message.getFloatProperty("missing"));
    }

    @Test
    void setObjectPropertyOnlyAcceptsBoxedPrimitivesAndString() throws JMSException {
        message.setObjectProperty("i", 3);
        assertEquals(3, message.getIntProperty("i"));

        assertThrows(MessageFormatException.class, () -> message.setObjectProperty("list", List.of()));
        assertThrows(MessageFormatException.class, () -> message.setObjectProperty("c", 'c'));
    }

    @Test
    void propertyNameMustBeANonEmptySelectorIdentifier() {
        assertThrows(IllegalArgumentException.class, () -> message.setIntProperty(null, 1));
        assertThrows(IllegalArgumentException.class, () -> message.setIntProperty("", 1));
        assertThrows(MessageFormatException.class, () -> message.setIntProperty("not-an-identifier", 1));
        assertThrows(MessageFormatException.class, () -> message.setIntProperty("BETWEEN", 1),
                "selector reserved words are not identifiers");
    }

    @Test
    void propertiesOfAReceivedMessageAreReadOnlyUntilCleared() throws JMSException {
        message.setIntProperty("i", 1);
        message.markReceived();

        assertThrows(MessageNotWriteableException.class, () -> message.setIntProperty("i", 2));
        assertEquals(1, message.getIntProperty("i"));

        message.clearProperties();
        assertFalse(message.propertyExists("i"));
        message.setIntProperty("i", 2);
        assertEquals(2, message.getIntProperty("i"));
    }

    @Test
    void headersOfAReceivedMessageStayWritable() throws JMSException {
        message.markReceived();

        message.setJMSType("still writable");

        assertEquals("still writable", message.getJMSType());
    }

    @Test
    void bodylessMessageHasNoBodyWhateverTheType() throws JMSException {
        assertNull(message.getBody(String.class));
        assertTrue(message.isBodyAssignableTo(Integer.class));
    }
}
