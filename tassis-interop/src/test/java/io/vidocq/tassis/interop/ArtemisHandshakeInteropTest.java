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
package io.vidocq.tassis.interop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.amqp.internal.messaging.Accepted;
import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.messaging.AmqpValue;
import io.vidocq.tassis.amqp.internal.messaging.Properties;
import io.vidocq.tassis.amqp.internal.messaging.Source;
import io.vidocq.tassis.amqp.internal.messaging.Target;
import io.vidocq.tassis.amqp.internal.security.SaslInit;
import io.vidocq.tassis.amqp.internal.security.SaslMechanisms;
import io.vidocq.tassis.amqp.internal.security.SaslOutcome;
import io.vidocq.tassis.amqp.internal.transport.Attach;
import io.vidocq.tassis.amqp.internal.transport.Begin;
import io.vidocq.tassis.amqp.internal.transport.Close;
import io.vidocq.tassis.amqp.internal.transport.Disposition;
import io.vidocq.tassis.amqp.internal.transport.Flow;
import io.vidocq.tassis.amqp.internal.transport.Frame;
import io.vidocq.tassis.amqp.internal.transport.FrameCodec;
import io.vidocq.tassis.amqp.internal.transport.Open;
import io.vidocq.tassis.amqp.internal.transport.Performative;
import io.vidocq.tassis.amqp.internal.transport.ProtocolHeader;
import io.vidocq.tassis.amqp.internal.transport.Transfer;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The codec against a real broker: Apache ActiveMQ Artemis, embedded, listening for AMQP 1.0 on
 * a local port. Tassis has no connection, session or link logic yet, so the test drives the
 * protocol by hand, frame by frame; what it proves is that a real broker understands the
 * octets Tassis writes and that Tassis reads the broker's.
 */
class ArtemisHandshakeInteropTest {

    private static final int PORT = 25_672;
    private static final long MAX_FRAME_SIZE = 65_536;

    private static EmbeddedActiveMQ broker;

    @BeforeAll
    static void startBroker() throws Exception {
        ConfigurationImpl configuration = new ConfigurationImpl();
        configuration.setPersistenceEnabled(false);
        configuration.setSecurityEnabled(false);
        configuration.setJMXManagementEnabled(false);
        configuration.addAcceptorConfiguration("amqp", "tcp://127.0.0.1:" + PORT + "?protocols=AMQP");
        broker = new EmbeddedActiveMQ();
        broker.setConfiguration(configuration);
        broker.start();
    }

    @AfterAll
    static void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    /** A socket to the broker, and the frames that go through it. */
    private static final class Wire implements AutoCloseable {

        private final Socket socket = new Socket("127.0.0.1", PORT);
        private final InputStream in = socket.getInputStream();
        private final OutputStream out = socket.getOutputStream();

        Wire() throws IOException {
            socket.setSoTimeout(10_000);
        }

        ProtocolHeader exchange(ProtocolHeader header) throws IOException, AmqpDecodeException {
            out.write(header.octets());
            out.flush();
            return ProtocolHeader.parse(in.readNBytes(ProtocolHeader.LENGTH));
        }

        void send(Frame frame) throws IOException {
            out.write(FrameCodec.encode(frame, MAX_FRAME_SIZE));
            out.flush();
        }

        Frame receive() throws IOException, AmqpDecodeException {
            byte[] start = in.readNBytes(4);
            int size = FrameCodec.size(ByteBuffer.wrap(start), MAX_FRAME_SIZE);
            byte[] frame = new byte[size];
            System.arraycopy(start, 0, frame, 0, 4);
            assertEquals(size - 4, in.readNBytes(frame, 4, size - 4), "the whole frame arrives");
            return FrameCodec.decode(ByteBuffer.wrap(frame), MAX_FRAME_SIZE);
        }

        /** The next frame that is not a heartbeat, whose body must be of that type. */
        <T> T expect(Class<T> type) throws IOException, AmqpDecodeException {
            Frame frame = receive();
            while (frame.isEmpty()) {
                frame = receive();
            }
            return type.cast(assertInstanceOf(type, frame.body(), "from the broker"));
        }

        Frame expectFrame(Class<? extends Performative> type) throws IOException, AmqpDecodeException {
            Frame frame = receive();
            while (frame.isEmpty()) {
                frame = receive();
            }
            assertInstanceOf(type, frame.body(), "from the broker");
            return frame;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    /** Opens the security layer and the connection: what every exchange starts with. */
    private static Open connect(Wire wire) throws IOException, AmqpDecodeException {
        // part 5 §5.3: the SASL header, the mechanisms the broker offers, ours, the outcome
        assertEquals(ProtocolHeader.SASL, wire.exchange(ProtocolHeader.SASL));
        SaslMechanisms mechanisms = wire.expect(SaslMechanisms.class);
        assertTrue(mechanisms.saslServerMechanisms().contains(Symbol.of("ANONYMOUS")), mechanisms.toString());
        wire.send(Frame.sasl(new SaslInit(Symbol.of("ANONYMOUS"), null, null)));
        assertEquals(0, wire.expect(SaslOutcome.class).code());

        // part 2 §2.2 and §2.7.1: the AMQP header again, then open both ways
        assertEquals(ProtocolHeader.AMQP, wire.exchange(ProtocolHeader.AMQP));
        wire.send(Frame.amqp(0, new Open("tassis-interop", "localhost", MAX_FRAME_SIZE, 10, null, null, null, null,
                null, null)));
        return wire.expect(Open.class);
    }

    @Test
    void aConnectionIsOpenedAndClosed() throws Exception {
        try (Wire wire = new Wire()) {
            Open open = connect(wire);
            assertNotNull(open.containerId());

            wire.send(Frame.amqp(0, new Close(null)));
            assertNull(wire.expect(Close.class).error());
        }
    }

    @Test
    void aMessageIsSentToAQueueAndReceivedBack() throws Exception {
        try (Wire wire = new Wire()) {
            connect(wire);

            // §2.7.2: a session on channel 0
            wire.send(Frame.amqp(0, new Begin(null, 0L, 100L, 100L, null, null, null, null)));
            Begin begin = wire.expect(Begin.class);
            assertEquals(0, begin.remoteChannel());

            // §2.7.3: a sending link to the queue; the broker grants credit (§2.7.4)
            wire.send(Frame.amqp(0, new Attach("sender", 0L, false, null, null,
                    new Source(null, null, null, null, null, null, null, null, null, null, null),
                    new Target("interop.queue", null, null, null, null, null, List.of(Symbol.of("queue"))), null, null,
                    0L, null, null, null, null)));
            Attach attached = wire.expect(Attach.class);
            assertEquals("sender", attached.name());
            assertEquals(true, attached.role(), "the broker is the receiver of this link");
            Flow credit = wire.expect(Flow.class);
            assertTrue(credit.linkCredit() > 0, "the broker grants credit");

            // §2.7.5: the message is the payload of the transfer; the broker accepts it (§2.7.6)
            AmqpMessage message = new AmqpMessage(null, null, null,
                    new Properties("id-1", null, "interop.queue", null, null, null, null, null, null, null, null, null,
                            null),
                    null, List.of(new AmqpValue("hello from Tassis")), null);
            wire.send(Frame.amqp(0, new Transfer(0L, 0L, Binary.of(1), 0L, false, null, null, null, null, null, null),
                    new Binary(message.encode())));
            Disposition disposition = wire.expect(Disposition.class);
            assertEquals(new Accepted(), disposition.state());
            assertEquals(0L, disposition.first());

            // a receiving link on the same queue, with credit for one message
            wire.send(Frame.amqp(0, new Attach("receiver", 1L, true, null, null,
                    new Source("interop.queue", null, null, null, null, null, null, null, null, null,
                            List.of(Symbol.of("queue"))),
                    new Target(null, null, null, null, null, null, null), null, null, null, null, null, null, null)));
            assertEquals("receiver", wire.expect(Attach.class).name());
            wire.send(Frame.amqp(0, new Flow(1L, 100L, 1L, 100L, 1L, 0L, 1L, null, null, null, null)));

            List<byte[]> parts = new ArrayList<>();
            Frame transfer = wire.expectFrame(Transfer.class);
            parts.add(transfer.payload().bytes());
            while (Boolean.TRUE.equals(((Transfer) transfer.body()).more())) {
                transfer = wire.expectFrame(Transfer.class);
                parts.add(transfer.payload().bytes());
            }
            java.io.ByteArrayOutputStream whole = new java.io.ByteArrayOutputStream();
            for (byte[] part : parts) {
                whole.writeBytes(part);
            }
            AmqpMessage received = AmqpMessage.decode(whole.toByteArray());

            assertEquals(List.of(new AmqpValue("hello from Tassis")), received.body());
            assertEquals("id-1", received.properties().messageId());
            assertEquals("interop.queue", received.properties().to());

            wire.send(Frame.amqp(0, new Close(null)));
        }
    }
}
