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
package io.vidocq.tassis.bench;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.amqp.internal.jms.JmsMessageCodec;
import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.transport.Frame;
import io.vidocq.tassis.amqp.internal.transport.FrameCodec;
import io.vidocq.tassis.amqp.internal.transport.Transfer;
import io.vidocq.tassis.amqp.internal.transport.Transfers;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.core.internal.message.Messages;
import io.vidocq.tassis.core.internal.message.TassisBytesMessage;
import io.vidocq.tassis.core.internal.message.TassisMessage;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What the AMQP code allocates to carry a message (ROADMAP M12-5): a bytes message is turned
 * into the frames of a transfer, as the one who sends does, and those frames into a message
 * again, as the one who receives does. The bounds are loose on purpose: they tell a copy of
 * the body from a dozen, not one object from two.
 *
 * <p>Here and not in {@code tassis-amqp}: the count of what a thread allocated is the JDK's
 * ({@code jdk.management}), which a module of Tassis has no reason to read.
 */
class CodecAllocationTest {

    private static final long MAX_FRAME_SIZE = 16 * 1024 * 1024;
    private static final com.sun.management.ThreadMXBean THREADS =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    private static TassisBytesMessage message(int size) throws Exception {
        TassisBytesMessage message = new TassisBytesMessage();
        byte[] body = new byte[size];
        for (int i = 0; i < size; i++) {
            body[i] = (byte) i;
        }
        message.writeBytes(body);
        message.setJMSMessageID("ID:allocation-1");
        message.setStringProperty("region", "north");
        return message;
    }

    /** What the sender does of a message the application handed over, down to the frames. */
    private static List<byte[]> send(TassisMessage snapshot) throws Exception {
        byte[] octets = JmsMessageCodec.toAmqp(snapshot).encode();
        Transfer transfer = new Transfer(0L, 1L, Binary.of(1), 0L, null, null, null, null, null, null, null);
        return Transfers.split(0, transfer, octets, MAX_FRAME_SIZE);
    }

    /** What the receiver does of the one frame of a transfer, up to the message. */
    private static TassisMessage receive(byte[] frame) throws Exception {
        Frame read = FrameCodec.decode(ByteBuffer.wrap(frame), MAX_FRAME_SIZE);
        return JmsMessageCodec.toJms(AmqpMessage.decode(read.payload().array()));
    }

    private static long allocatedByARoundTrip(int size) throws Exception {
        TassisBytesMessage original = message(size);
        long least = Long.MAX_VALUE;
        // The first rounds load classes and compile; the least of many is what the code needs.
        for (int round = 0; round < 2_000; round++) {
            TassisMessage snapshot = Messages.snapshot(original);
            long before = THREADS.getCurrentThreadAllocatedBytes();
            TassisMessage received = receive(send(snapshot).get(0));
            least = Math.min(least, THREADS.getCurrentThreadAllocatedBytes() - before);
            if (round == 0) {
                byte[] body = new byte[size];
                ((TassisBytesMessage) received).readBytes(body);
                byte[] sent = new byte[size];
                original.reset();
                original.readBytes(sent);
                original = message(size);
                assertArrayEquals(sent, body);
            }
        }
        return least;
    }

    @Test
    void aLargeBodyIsCopiedAFewTimesOnItsWayNotDozens() throws Exception {
        int size = 100_000;

        long allocated = allocatedByARoundTrip(size);

        // Six copies of the body today, where there were twenty-one.
        assertTrue(allocated < 8L * size, allocated + " octets allocated to carry " + size);
    }

    // What a small message costs is its sections, its maps and its lists, not its body: some ten
    // kilobytes, which this keeps from growing back to the fourteen it was.
    @Test
    void aSmallMessageCostsNoMoreToCarryThanItDid() throws Exception {
        long allocated = allocatedByARoundTrip(100);

        assertTrue(allocated < 12_000, allocated + " octets allocated to carry 100");
    }
}
