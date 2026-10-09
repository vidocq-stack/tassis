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

/** {@code BytesMessage} body: a stream of uninterpreted bytes (Jakarta Messaging 3.1 §3.11). */
class TassisBytesMessageTest {

    private final TassisBytesMessage message = new TassisBytesMessage();

    @Test
    void primitivesRoundTripInWriteOrder() throws JMSException {
        message.writeBoolean(true);
        message.writeByte((byte) -1);
        message.writeShort((short) -2);
        message.writeChar('c');
        message.writeInt(3);
        message.writeLong(4L);
        message.writeFloat(5.5f);
        message.writeDouble(6.5d);
        message.writeUTF("seven");
        message.reset();

        assertTrue(message.readBoolean());
        assertEquals((byte) -1, message.readByte());
        assertEquals((short) -2, message.readShort());
        assertEquals('c', message.readChar());
        assertEquals(3, message.readInt());
        assertEquals(4L, message.readLong());
        assertEquals(5.5f, message.readFloat());
        assertEquals(6.5d, message.readDouble());
        assertEquals("seven", message.readUTF());
    }

    @Test
    void unsignedReadsReinterpretTheSameBytes() throws JMSException {
        message.writeByte((byte) -1);
        message.writeShort((short) -2);
        message.reset();

        assertEquals(255, message.readUnsignedByte());
        assertEquals(65534, message.readUnsignedShort());
    }

    @Test
    void readBytesFillsTheBufferThenSignalsTheEndOfTheStream() throws JMSException {
        message.writeBytes(new byte[] {1, 2, 3, 4, 5}, 1, 3);
        message.reset();
        byte[] buffer = new byte[2];

        assertEquals(2, message.readBytes(buffer));
        assertArrayEquals(new byte[] {2, 3}, buffer);
        assertEquals(1, message.readBytes(buffer), "fewer bytes than the buffer means the stream is exhausted");
        assertEquals(4, buffer[0]);
        assertEquals(-1, message.readBytes(buffer));
    }

    @Test
    void readBytesRejectsALengthOutsideTheBuffer() throws JMSException {
        message.writeBytes(new byte[] {1, 2});
        message.reset();
        byte[] buffer = new byte[2];

        assertThrows(IndexOutOfBoundsException.class, () -> message.readBytes(buffer, 3));
        assertThrows(IndexOutOfBoundsException.class, () -> message.readBytes(buffer, -1));
        assertEquals(1, message.readBytes(buffer, 1), "no byte was consumed by the rejected calls");
        assertEquals(1, buffer[0]);
    }

    @Test
    void readingPastTheEndFailsWithoutConsumingTheRemainingBytes() throws JMSException {
        message.writeByte((byte) 7);
        message.reset();

        assertThrows(MessageEOFException.class, message::readInt);
        assertEquals((byte) 7, message.readByte(), "the failed read must leave the position where it was");
    }

    @Test
    void writeObjectDispatchesOnTheObjectType() throws JMSException {
        message.writeObject(3);
        message.writeObject("seven");
        message.writeObject(new byte[] {8});
        message.reset();

        assertEquals(3, message.readInt());
        assertEquals("seven", message.readUTF());
        assertEquals((byte) 8, message.readByte());
        assertThrows(NullPointerException.class, () -> new TassisBytesMessage().writeObject(null));
        assertThrows(MessageFormatException.class, () -> new TassisBytesMessage().writeObject(List.of()));
    }

    @Test
    void newMessageIsWriteOnlyAndResetMakesItReadOnly() throws JMSException {
        message.writeInt(3);

        assertThrows(MessageNotReadableException.class, message::readInt);
        assertThrows(MessageNotReadableException.class, message::getBodyLength);

        message.reset();

        assertEquals(4L, message.getBodyLength());
        assertThrows(MessageNotWriteableException.class, () -> message.writeInt(4));
        assertEquals(3, message.readInt());
    }

    @Test
    void resetRewindsAReadOnlyMessage() throws JMSException {
        message.writeInt(3);
        message.reset();
        message.readInt();

        message.reset();

        assertEquals(3, message.readInt());
    }

    @Test
    void clearBodyReturnsToAnEmptyWriteOnlyMessage() throws JMSException {
        message.writeInt(3);
        message.reset();

        message.clearBody();
        message.writeByte((byte) 1);
        message.reset();

        assertEquals(1L, message.getBodyLength());
    }

    @Test
    void receivedMessageIsReadOnlyFromItsStart() throws JMSException {
        message.writeInt(3);
        message.markReceived();

        assertThrows(MessageNotWriteableException.class, () -> message.writeInt(4));
        assertEquals(3, message.readInt());
    }

    @Test
    void bodyIsTheWholeByteArrayAndLeavesTheMessageRewound() throws JMSException {
        message.writeBytes(new byte[] {1, 2});
        message.reset();
        message.readByte();

        assertArrayEquals(new byte[] {1, 2}, message.getBody(byte[].class));
        assertEquals((byte) 1, message.readByte(), "getBody resets the message after use");
        assertTrue(message.isBodyAssignableTo(Object.class));
        assertFalse(message.isBodyAssignableTo(String.class));
        assertThrows(MessageFormatException.class, () -> message.getBody(String.class));
    }

    @Test
    void emptyMessageHasNoBody() throws JMSException {
        assertNull(message.getBody(String.class));
        assertTrue(message.isBodyAssignableTo(String.class));
    }
}
