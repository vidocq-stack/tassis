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
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Objects;

/**
 * A message whose body is a stream of uninterpreted bytes (Jakarta Messaging 3.1
 * §3.11), encoded as {@link DataOutputStream} does. The body is write-only
 * until {@link #reset()}, then read-only until {@link #clearBody()}.
 */
public final class TassisBytesMessage extends TassisMessage implements BytesMessage {

    @FunctionalInterface
    private interface Read<T> {
        T from(DataInputStream in) throws IOException;
    }

    @FunctionalInterface
    private interface Write {
        void to(DataOutputStream out) throws IOException;
    }

    private ByteArrayOutputStream written = new ByteArrayOutputStream();
    private DataOutputStream out = new DataOutputStream(written);
    private byte[] content;
    private DataInputStream in;

    /**
     * A message whose body is those octets, ready to be read. They are not copied: the caller
     * gives the array up. For the code that carries messages, which has just read them.
     */
    public static TassisBytesMessage holding(byte[] givenUp) {
        TassisBytesMessage message = new TassisBytesMessage();
        message.written = null;
        message.out = null;
        message.content = givenUp;
        message.in = new DataInputStream(new ByteArrayInputStream(givenUp));
        message.setBodyReadOnly();
        return message;
    }

    /**
     * The body as the message holds it, not a copy, to read: the message becomes read-only, as
     * {@link #reset()} makes it, and its octets are never written to. For the code that carries
     * messages.
     */
    public byte[] content() {
        reset();
        return content;
    }

    @Override
    public long getBodyLength() throws JMSException {
        checkBodyReadable();
        return content.length;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        return read(DataInputStream::readBoolean);
    }

    @Override
    public byte readByte() throws JMSException {
        return read(DataInputStream::readByte);
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        return read(DataInputStream::readUnsignedByte);
    }

    @Override
    public short readShort() throws JMSException {
        return read(DataInputStream::readShort);
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        return read(DataInputStream::readUnsignedShort);
    }

    @Override
    public char readChar() throws JMSException {
        return read(DataInputStream::readChar);
    }

    @Override
    public int readInt() throws JMSException {
        return read(DataInputStream::readInt);
    }

    @Override
    public long readLong() throws JMSException {
        return read(DataInputStream::readLong);
    }

    @Override
    public float readFloat() throws JMSException {
        return read(DataInputStream::readFloat);
    }

    @Override
    public double readDouble() throws JMSException {
        return read(DataInputStream::readDouble);
    }

    @Override
    public String readUTF() throws JMSException {
        return read(stream -> stream.readUTF());
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        return readBytes(value, value.length);
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        checkBodyReadable();
        Objects.checkFromIndexSize(0, length, value.length);
        return read(stream -> stream.read(value, 0, length));
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        write(stream -> stream.writeBoolean(value));
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        write(stream -> stream.writeByte(value));
    }

    @Override
    public void writeShort(short value) throws JMSException {
        write(stream -> stream.writeShort(value));
    }

    @Override
    public void writeChar(char value) throws JMSException {
        write(stream -> stream.writeChar(value));
    }

    @Override
    public void writeInt(int value) throws JMSException {
        write(stream -> stream.writeInt(value));
    }

    @Override
    public void writeLong(long value) throws JMSException {
        write(stream -> stream.writeLong(value));
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        write(stream -> stream.writeFloat(value));
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        write(stream -> stream.writeDouble(value));
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        write(stream -> stream.writeUTF(value));
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        write(stream -> stream.write(value));
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        write(stream -> stream.write(value, offset, length));
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        switch (Objects.requireNonNull(value, "value")) {
            case Boolean b -> writeBoolean(b);
            case Byte b -> writeByte(b);
            case Short s -> writeShort(s);
            case Character c -> writeChar(c);
            case Integer i -> writeInt(i);
            case Long l -> writeLong(l);
            case Float f -> writeFloat(f);
            case Double d -> writeDouble(d);
            case String s -> writeUTF(s);
            case byte[] bytes -> writeBytes(bytes);
            default -> throw new MessageFormatException(
                    value.getClass().getName() + " cannot be written to a bytes message");
        }
    }

    @Override
    public void reset() {
        if (!isBodyReadOnly()) {
            content = written.toByteArray();
            written = null;
            out = null;
        }
        in = new DataInputStream(new ByteArrayInputStream(content));
        setBodyReadOnly();
    }

    private <T> T read(Read<T> read) throws JMSException {
        checkBodyReadable();
        // A read that runs past the end must not consume the bytes it did find.
        in.mark(0);
        try {
            return read.from(in);
        } catch (EOFException e) {
            rewindToMark();
            throw new MessageEOFException("end of the bytes message reached");
        } catch (UTFDataFormatException e) {
            rewindToMark();
            throw new MessageFormatException("the bytes at this position are not a modified UTF-8 string");
        } catch (IOException e) {
            throw new JMSException("could not read the bytes message: " + e);
        }
    }

    private void rewindToMark() throws JMSException {
        try {
            in.reset();
        } catch (IOException e) {
            throw new JMSException("could not rewind the bytes message: " + e);
        }
    }

    private void write(Write write) throws JMSException {
        checkBodyWritable();
        try {
            write.to(out);
        } catch (IOException e) {
            throw new JMSException("could not write the bytes message: " + e);
        }
    }

    @Override
    TassisMessage newInstance() {
        return new TassisBytesMessage();
    }

    @Override
    void copyBodyTo(TassisMessage target) {
        // Copies whatever was written so far, without switching this message to read-only, into
        // an array of the size of it: one copy, where an array that grows makes several.
        TassisBytesMessage copy = (TassisBytesMessage) target;
        int length = isBodyReadOnly() ? content.length : written.size();
        copy.written = new ByteArrayOutputStream(Math.max(32, length));
        copy.out = new DataOutputStream(copy.written);
        if (isBodyReadOnly()) {
            copy.written.writeBytes(content);
        } else {
            try {
                written.writeTo(copy.written);
            } catch (IOException e) {
                // An array in memory: nothing it writes to can fail.
                throw new java.io.UncheckedIOException(e);
            }
        }
    }

    @Override
    Object body() {
        // Message.getBody: "This method will reset the BytesMessage before and after use."
        reset();
        return content.length == 0 ? null : content.clone();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean isBodyAssignableTo(Class c) {
        int length = isBodyReadOnly() ? content.length : written.size();
        return length == 0 || c.isAssignableFrom(byte[].class);
    }

    @Override
    void clearBodyContent() {
        written = new ByteArrayOutputStream();
        out = new DataOutputStream(written);
        content = null;
        in = null;
    }

    @Override
    void makeBodyReadOnly() {
        reset();
    }
}
