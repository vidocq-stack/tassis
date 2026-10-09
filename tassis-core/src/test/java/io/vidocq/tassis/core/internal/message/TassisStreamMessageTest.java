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
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.MessageNotWriteableException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** {@code StreamMessage} body and its conversion table (Jakarta Messaging 3.1 §3.11.3). */
class TassisStreamMessageTest {

    private final TassisStreamMessage message = new TassisStreamMessage();

    @Test
    void fieldsRoundTripInWriteOrder() throws JMSException {
        message.writeBoolean(true);
        message.writeByte((byte) 1);
        message.writeShort((short) 2);
        message.writeChar('c');
        message.writeInt(3);
        message.writeLong(4L);
        message.writeFloat(5.5f);
        message.writeDouble(6.5d);
        message.writeString("seven");
        message.reset();

        assertTrue(message.readBoolean());
        assertEquals((byte) 1, message.readByte());
        assertEquals((short) 2, message.readShort());
        assertEquals('c', message.readChar());
        assertEquals(3, message.readInt());
        assertEquals(4L, message.readLong());
        assertEquals(5.5f, message.readFloat());
        assertEquals(6.5d, message.readDouble());
        assertEquals("seven", message.readString());
        assertThrows(MessageEOFException.class, message::readInt);
    }

    @Test
    void fieldIsConvertedAlongTheSpecTable() throws JMSException {
        message.writeByte((byte) 1);
        message.writeString("42");
        message.writeChar('c');
        message.reset();

        assertEquals(1L, message.readLong(), "byte widens to long");
        assertEquals(42, message.readInt(), "String converts through valueOf");
        assertEquals("c", message.readString(), "char reads as String");
    }

    @Test
    void failedConversionLeavesTheFieldToBeReadAgain() throws JMSException {
        message.writeInt(3);
        message.writeString("not a number");
        message.reset();

        assertThrows(MessageFormatException.class, message::readBoolean, "int to boolean is not a marked case");
        assertEquals(3, message.readInt());
        assertThrows(NumberFormatException.class, message::readInt);
        assertEquals("not a number", message.readString());
    }

    @Test
    void readObjectReturnsTheBoxedFieldAndAFreshByteArray() throws JMSException {
        byte[] bytes = {1, 2};
        message.writeObject(3);
        message.writeBytes(bytes);
        message.writeObject(null);
        message.reset();

        assertEquals(Integer.valueOf(3), message.readObject());
        assertArrayEquals(bytes, (byte[]) message.readObject());
        assertNull(message.readObject());
    }

    @Test
    void writeObjectRejectsATypeOutsideTheTable() {
        assertThrows(MessageFormatException.class, () -> message.writeObject(List.of()));
    }

    @Test
    void readBytesReturnsAFieldInBufferSizedIncrements() throws JMSException {
        message.writeBytes(new byte[] {0, 1, 2, 3, 9}, 1, 3);
        message.writeInt(7);
        message.reset();
        byte[] buffer = new byte[2];

        assertEquals(2, message.readBytes(buffer));
        assertArrayEquals(new byte[] {1, 2}, buffer);
        assertThrows(MessageFormatException.class, message::readInt,
                "the byte field must be read in full before the next field");
        assertEquals(1, message.readBytes(buffer), "a short read completes the field");
        assertEquals(3, buffer[0]);
        assertEquals(7, message.readInt());
    }

    @Test
    void readBytesNeedsAnExtraCallWhenTheFieldExactlyFillsTheBuffer() throws JMSException {
        message.writeBytes(new byte[] {1, 2});
        message.writeInt(7);
        message.reset();
        byte[] buffer = new byte[2];

        assertEquals(2, message.readBytes(buffer));
        assertEquals(-1, message.readBytes(buffer), "a full buffer does not tell the field is complete");
        assertEquals(7, message.readInt());
    }

    @Test
    void readBytesTellsANullFieldFromAnEmptyOne() throws JMSException {
        message.writeBytes(null);
        message.writeBytes(new byte[0]);
        message.reset();
        byte[] buffer = new byte[2];

        assertEquals(-1, message.readBytes(buffer));
        assertEquals(0, message.readBytes(buffer));
        assertThrows(MessageEOFException.class, () -> message.readBytes(buffer));
    }

    @Test
    void nullFieldReadsAsNullOrFailsLikeValueOfNull() throws JMSException {
        message.writeString(null);
        message.reset();

        assertThrows(NullPointerException.class, message::readChar);
        assertThrows(NumberFormatException.class, message::readInt);
        assertNull(message.readString());
    }

    @Test
    void newMessageIsWriteOnlyAndResetMakesItReadOnly() throws JMSException {
        message.writeInt(3);

        assertThrows(MessageNotReadableException.class, message::readInt);

        message.reset();

        assertThrows(MessageNotWriteableException.class, () -> message.writeInt(4));
        assertEquals(3, message.readInt());
    }

    @Test
    void clearBodyReturnsToAnEmptyWriteOnlyMessage() throws JMSException {
        message.writeInt(3);
        message.reset();

        message.clearBody();
        message.writeInt(4);
        message.reset();

        assertEquals(4, message.readInt());
        assertThrows(MessageEOFException.class, message::readInt);
    }

    @Test
    void receivedMessageIsReadOnlyFromItsStart() throws JMSException {
        message.writeInt(3);
        message.markReceived();

        assertThrows(MessageNotWriteableException.class, () -> message.writeInt(4));
        assertEquals(3, message.readInt());
    }

    @Test
    void streamBodyIsNeverAssignable() throws JMSException {
        message.writeInt(3);
        message.reset();

        assertFalse(message.isBodyAssignableTo(Object.class));
        assertThrows(MessageFormatException.class, () -> message.getBody(Object.class));
    }
}
