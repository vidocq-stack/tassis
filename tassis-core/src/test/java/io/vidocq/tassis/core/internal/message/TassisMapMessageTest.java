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

import jakarta.jms.JMSException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** {@code MapMessage} body and its conversion table (Jakarta Messaging 3.1 §3.11.3). */
class TassisMapMessageTest {

    private final TassisMapMessage message = new TassisMapMessage();

    @Test
    void valuesRoundTripInTheirOwnType() throws JMSException {
        message.setBoolean("b", true);
        message.setByte("by", (byte) 1);
        message.setShort("s", (short) 2);
        message.setChar("c", 'c');
        message.setInt("i", 3);
        message.setLong("l", 4L);
        message.setFloat("f", 5.5f);
        message.setDouble("d", 6.5d);
        message.setString("str", "seven");
        message.setBytes("bytes", new byte[] {8, 9});

        assertTrue(message.getBoolean("b"));
        assertEquals((byte) 1, message.getByte("by"));
        assertEquals((short) 2, message.getShort("s"));
        assertEquals('c', message.getChar("c"));
        assertEquals(3, message.getInt("i"));
        assertEquals(4L, message.getLong("l"));
        assertEquals(5.5f, message.getFloat("f"));
        assertEquals(6.5d, message.getDouble("d"));
        assertEquals("seven", message.getString("str"));
        assertArrayEquals(new byte[] {8, 9}, message.getBytes("bytes"));
        assertEquals(List.of("b", "by", "s", "c", "i", "l", "f", "d", "str", "bytes"),
                Collections.list(message.getMapNames()));
    }

    @Test
    void valueIsConvertedAlongTheSpecTable() throws JMSException {
        message.setChar("c", 'c');
        message.setShort("s", (short) 2);
        message.setBytes("bytes", new byte[] {1});

        assertEquals("c", message.getString("c"), "char reads as String");
        assertEquals(2, message.getInt("s"), "short widens to int");
        assertThrows(MessageFormatException.class, () -> message.getInt("c"), "char reads as char or String only");
        assertThrows(MessageFormatException.class, () -> message.getString("bytes"),
                "byte[] reads as byte[] only");
        assertThrows(MessageFormatException.class, () -> message.getBytes("s"));
    }

    @Test
    void absentValueReadsAsNull() throws JMSException {
        assertFalse(message.itemExists("missing"));
        assertNull(message.getString("missing"));
        assertNull(message.getBytes("missing"));
        assertNull(message.getObject("missing"));
        assertThrows(NullPointerException.class, () -> message.getChar("missing"),
                "char has no String conversion to fall back on");
        assertThrows(NumberFormatException.class, () -> message.getLong("missing"));
    }

    @Test
    void byteArraysAreCopiedInAndOut() throws JMSException {
        byte[] bytes = {1, 2, 3};
        message.setBytes("all", bytes);
        message.setBytes("part", bytes, 1, 2);

        bytes[0] = 9;
        message.getBytes("all")[1] = 9;

        assertArrayEquals(new byte[] {1, 2, 3}, message.getBytes("all"));
        assertArrayEquals(new byte[] {2, 3}, message.getBytes("part"));
    }

    @Test
    void setObjectAcceptsTheMapTypesOnly() throws JMSException {
        message.setObject("i", 3);
        message.setObject("c", 'c');
        message.setObject("bytes", new byte[] {1});

        assertEquals(3, message.getInt("i"));
        assertEquals('c', message.getChar("c"));
        assertArrayEquals(new byte[] {1}, (byte[]) message.getObject("bytes"));
        assertThrows(MessageFormatException.class, () -> message.setObject("list", List.of()));
    }

    @Test
    void nameMustNotBeNullOrEmpty() {
        assertThrows(IllegalArgumentException.class, () -> message.setInt(null, 1));
        assertThrows(IllegalArgumentException.class, () -> message.setInt("", 1));
    }

    @Test
    void bodyIsAMap() throws JMSException {
        message.setInt("i", 3);

        assertEquals(Map.of("i", 3), message.getBody(Map.class));
        assertTrue(message.isBodyAssignableTo(Object.class));
        assertFalse(message.isBodyAssignableTo(String.class));
        assertThrows(MessageFormatException.class, () -> message.getBody(String.class));
    }

    @Test
    void bodyOfAReceivedMessageIsReadOnlyUntilCleared() throws JMSException {
        message.setInt("i", 3);
        message.markReceived();

        assertThrows(MessageNotWriteableException.class, () -> message.setInt("i", 4));
        assertEquals(3, message.getInt("i"));

        message.clearBody();
        assertFalse(message.itemExists("i"));
        message.setInt("i", 4);
        assertEquals(4, message.getInt("i"));
    }
}
