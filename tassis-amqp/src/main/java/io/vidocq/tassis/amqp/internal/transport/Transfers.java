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

import io.vidocq.tassis.amqp.internal.types.AmqpEncoder;
import io.vidocq.tassis.amqp.internal.types.Binary;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** A message as the transfer frames that carry it. */
public final class Transfers {

    private Transfers() {
    }

    /**
     * Returns the transfer frames of one message, none larger than the peer accepts (part 2,
     * §2.7.5): all but the last say that more follows.
     *
     * @param first the transfer of a message that fits in one frame, which is also what the
     *        last frame of a longer one carries
     * @param message the octets of the message, which are read and not kept
     * @param maxFrameSize the largest frame the peer accepts
     */
    public static List<byte[]> split(int channel, Transfer first, byte[] message, long maxFrameSize) {
        // Nearly every message fits in one frame: its transfer is encoded once, to know that.
        byte[] alone = AmqpEncoder.encode(first);
        if (8L + alone.length + message.length <= maxFrameSize) {
            // The array is the caller's, which only reads it: no copy before the one into the frame.
            return List.of(FrameCodec.encode(Frame.TYPE_AMQP, channel, alone, message, maxFrameSize));
        }
        List<byte[]> frames = new ArrayList<>();
        Transfer more = new Transfer(first.handle(), first.deliveryId(), first.deliveryTag(), first.messageFormat(),
                first.settled(), true, null, first.state(), null, null, null);
        // The frame header and the performative, the longer of the two it may take.
        int overhead = 8 + AmqpEncoder.encode(more).length;
        int room = (int) Math.min(Integer.MAX_VALUE, maxFrameSize) - overhead;
        for (int from = 0; from < message.length; from += room) {
            int to = Math.min(message.length, from + room);
            byte[] part = Arrays.copyOfRange(message, from, to);
            frames.add(FrameCodec.encode(Frame.amqp(channel, to < message.length ? more : first, Binary.wrap(part)),
                    maxFrameSize));
        }
        return frames;
    }
}
