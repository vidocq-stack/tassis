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
package io.vidocq.tassis.amqp.internal.transport;

import io.vidocq.tassis.amqp.internal.AmqpTypes;
import io.vidocq.tassis.amqp.internal.security.SaslBody;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.AmqpDecoder;
import io.vidocq.tassis.amqp.internal.types.AmqpEncoder;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import java.nio.ByteBuffer;

/**
 * Writes and reads frames (part 2, §2.3): an 8-octet header (the size of the frame, the offset
 * of its body in 4-octet words, its type, its channel), an extended header 1.0 gives no use
 * to, and the body.
 */
public final class FrameCodec {

    /** The largest frame a peer may send before the two have said what they accept (§2.7.1). */
    public static final int MIN_MAX_FRAME_SIZE = 512;

    private static final int HEADER_LENGTH = 8;
    /** The data offset of a frame without an extended header: two words. */
    private static final int MIN_DOFF = 2;

    private FrameCodec() {
    }

    /**
     * @param maxFrameSize what the peer accepts (its {@code max-frame-size}, or 512 before open)
     * @throws IllegalArgumentException if the frame is larger: it is for the caller to split a
     *         transfer into frames that fit
     */
    public static byte[] encode(Frame frame, long maxFrameSize) {
        byte[] body = frame.body() == null ? new byte[0] : AmqpEncoder.encode(frame.body());
        return encode(frame.type(), frame.channel(), body, frame.payload().array(), maxFrameSize);
    }

    /**
     * The frame of a body that is encoded already, for whoever had to encode it to know its size.
     *
     * @throws IllegalArgumentException if the frame is larger than the peer accepts
     */
    static byte[] encode(int type, int channel, byte[] body, byte[] payload, long maxFrameSize) {
        long size = (long) HEADER_LENGTH + body.length + payload.length;
        if (size > maxFrameSize) {
            throw new IllegalArgumentException(
                    "a frame of " + size + " octets, when the peer accepts " + maxFrameSize + " at most");
        }
        ByteBuffer out = ByteBuffer.allocate((int) size);
        out.putInt((int) size);
        out.put((byte) MIN_DOFF);
        out.put((byte) type);
        out.putShort((short) channel);
        out.put(body);
        out.put(payload);
        return out.array();
    }

    /**
     * Tells how long the frame that starts at the position of {@code in} is, without reading
     * anything, so that a caller knows how much to wait for.
     *
     * @param maxFrameSize what this side accepts
     * @return the size of the frame, or -1 if fewer than four octets are there
     * @throws AmqpDecodeException if the size announced is not one a frame can have: nothing
     *         should be buffered for it
     */
    public static int size(ByteBuffer in, long maxFrameSize) throws AmqpDecodeException {
        if (in.remaining() < 4) {
            return -1;
        }
        long size = in.getInt(in.position()) & 0xFFFF_FFFFL;
        if (size < HEADER_LENGTH) {
            throw new AmqpDecodeException("a frame of " + size + " octets is smaller than its own header");
        }
        if (size > maxFrameSize) {
            throw new AmqpDecodeException(
                    "a frame of " + size + " octets, when " + maxFrameSize + " at most were agreed");
        }
        return (int) size;
    }

    /**
     * Reads the frame that the remaining octets of {@code in} are, all of them.
     *
     * @param maxFrameSize what this side accepts
     */
    public static Frame decode(ByteBuffer in, long maxFrameSize) throws AmqpDecodeException {
        int size = size(in, maxFrameSize);
        if (size != in.remaining()) {
            throw new AmqpDecodeException(
                    "a frame that says it is " + size + " octets, in " + in.remaining() + " octets");
        }
        ByteBuffer frame = in.slice();
        int doff = frame.get(4) & 0xFF;
        int type = frame.get(5) & 0xFF;
        int channel = frame.getShort(6) & 0xFFFF;
        if (doff < MIN_DOFF || doff * 4 > size) {
            throw new AmqpDecodeException("a frame of " + size + " octets whose body would start at word " + doff);
        }
        ByteBuffer rest = frame.slice(doff * 4, size - doff * 4);
        if (type != Frame.TYPE_AMQP && type != Frame.TYPE_SASL) {
            throw new AmqpDecodeException("a frame of the unknown type " + type);
        }
        if (!rest.hasRemaining()) {
            if (type == Frame.TYPE_SASL) {
                throw new AmqpDecodeException("a SASL frame without a body");
            }
            return Frame.EMPTY;
        }
        AmqpDecoder decoder = new AmqpDecoder(rest, AmqpTypes.ALL);
        Object body = decoder.read();
        byte[] payload = new byte[rest.remaining() - decoder.position()];
        rest.get(decoder.position(), payload);
        if (type == Frame.TYPE_SASL) {
            if (!(body instanceof SaslBody sasl) || payload.length > 0) {
                throw new AmqpDecodeException("a SASL frame carries one SASL body, not " + describe(body));
            }
            // Octets 6 and 7 of a SASL frame are ignored (part 5, §5.3.2).
            return Frame.sasl(sasl);
        }
        if (!(body instanceof Performative performative)) {
            throw new AmqpDecodeException("an AMQP frame carries a performative, not " + describe(body));
        }
        return Frame.amqp(channel, performative, Binary.wrap(payload));
    }

    private static String describe(Object body) {
        return body instanceof DescribedType described ? described.descriptorName().toString()
                : body == null ? "null" : body.getClass().getSimpleName();
    }
}
