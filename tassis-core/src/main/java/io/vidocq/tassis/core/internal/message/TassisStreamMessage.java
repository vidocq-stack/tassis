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
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.StreamMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A message whose body is a sequence of typed fields, read back in write order
 * (Jakarta Messaging 3.1 §3.11). The body is write-only until {@link #reset()},
 * then read-only until {@link #clearBody()}.
 */
public final class TassisStreamMessage extends TassisMessage implements StreamMessage {

    /** {@link #bytesOffset} value when no byte field is being read in increments. */
    private static final int NOT_READING_BYTES = -1;

    private final List<Object> fields = new ArrayList<>();
    private int position;
    private int bytesOffset = NOT_READING_BYTES;

    @Override
    public boolean readBoolean() throws JMSException {
        boolean value = ValueConversions.toBoolean(currentField());
        position++;
        return value;
    }

    @Override
    public byte readByte() throws JMSException {
        byte value = ValueConversions.toByte(currentField());
        position++;
        return value;
    }

    @Override
    public short readShort() throws JMSException {
        short value = ValueConversions.toShort(currentField());
        position++;
        return value;
    }

    @Override
    public char readChar() throws JMSException {
        char value = ValueConversions.toChar(currentField());
        position++;
        return value;
    }

    @Override
    public int readInt() throws JMSException {
        int value = ValueConversions.toInt(currentField());
        position++;
        return value;
    }

    @Override
    public long readLong() throws JMSException {
        long value = ValueConversions.toLong(currentField());
        position++;
        return value;
    }

    @Override
    public float readFloat() throws JMSException {
        float value = ValueConversions.toFloat(currentField());
        position++;
        return value;
    }

    @Override
    public double readDouble() throws JMSException {
        double value = ValueConversions.toDouble(currentField());
        position++;
        return value;
    }

    @Override
    public String readString() throws JMSException {
        String value = ValueConversions.toString(currentField());
        position++;
        return value;
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        checkBodyReadable();
        boolean firstCall = bytesOffset == NOT_READING_BYTES;
        if (firstCall) {
            Object field = currentField();
            if (field == null) {
                position++;
                return -1;
            }
            if (!(field instanceof byte[])) {
                throw new MessageFormatException(
                        "a " + field.getClass().getSimpleName() + " value cannot be read as byte[]");
            }
            bytesOffset = 0;
        }
        byte[] field = (byte[]) fields.get(position);
        int remaining = field.length - bytesOffset;
        if (remaining == 0 && !firstCall) {
            endOfByteField();
            return -1;
        }
        int read = Math.min(remaining, value.length);
        System.arraycopy(field, bytesOffset, value, 0, read);
        bytesOffset += read;
        // A full buffer does not tell the caller the field is over: one more call returns -1.
        if (read < value.length) {
            endOfByteField();
        }
        return read;
    }

    @Override
    public Object readObject() throws JMSException {
        Object value = currentField();
        position++;
        return value instanceof byte[] bytes ? bytes.clone() : value;
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        append(value);
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        append(value);
    }

    @Override
    public void writeShort(short value) throws JMSException {
        append(value);
    }

    @Override
    public void writeChar(char value) throws JMSException {
        append(value);
    }

    @Override
    public void writeInt(int value) throws JMSException {
        append(value);
    }

    @Override
    public void writeLong(long value) throws JMSException {
        append(value);
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        append(value);
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        append(value);
    }

    @Override
    public void writeString(String value) throws JMSException {
        append(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        append(value == null ? null : value.clone());
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        append(Arrays.copyOfRange(value, offset, offset + length));
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        append(ValueConversions.bodyValue(value));
    }

    @Override
    public void reset() {
        position = 0;
        bytesOffset = NOT_READING_BYTES;
        setBodyReadOnly();
    }

    /** Returns the field at the read position, without moving past it. */
    private Object currentField() throws JMSException {
        checkBodyReadable();
        if (bytesOffset != NOT_READING_BYTES) {
            throw new MessageFormatException("the current byte field must be read in full first");
        }
        if (position >= fields.size()) {
            throw new MessageEOFException("end of the stream message reached");
        }
        return fields.get(position);
    }

    private void endOfByteField() {
        bytesOffset = NOT_READING_BYTES;
        position++;
    }

    private void append(Object value) throws JMSException {
        checkBodyWritable();
        fields.add(value);
    }

    @Override
    TassisMessage newInstance() {
        return new TassisStreamMessage();
    }

    @Override
    void copyBodyTo(TassisMessage target) {
        // Byte array fields are private copies that are never modified in place.
        ((TassisStreamMessage) target).fields.addAll(fields);
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        throw new MessageFormatException("getBody is not available on a StreamMessage");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean isBodyAssignableTo(Class c) {
        return false;
    }

    @Override
    void clearBodyContent() {
        fields.clear();
        position = 0;
        bytesOffset = NOT_READING_BYTES;
    }

    @Override
    void makeBodyReadOnly() {
        reset();
    }
}
