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
package io.vidocq.tassis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.messaging.Source;
import io.vidocq.tassis.amqp.internal.messaging.Target;
import io.vidocq.tassis.amqp.internal.security.SaslInit;
import io.vidocq.tassis.amqp.internal.security.SaslMechanisms;
import io.vidocq.tassis.amqp.internal.security.SaslOutcome;
import io.vidocq.tassis.amqp.internal.transport.Attach;
import io.vidocq.tassis.amqp.internal.transport.Begin;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A client of the server, for the tests: a socket, and the frames that go through it, written
 * and read with the codec. It has no logic of its own: each test says what is sent.
 */
final class Wire implements AutoCloseable {

    static final long MAX_FRAME_SIZE = 65_536;

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    long maxFrameSize = MAX_FRAME_SIZE;

    Wire(int port) throws IOException {
        socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(5_000);
        in = socket.getInputStream();
        out = socket.getOutputStream();
    }

    void write(byte[] octets) throws IOException {
        out.write(octets);
        out.flush();
    }

    ProtocolHeader exchange(ProtocolHeader header) throws IOException, AmqpDecodeException {
        write(header.octets());
        return ProtocolHeader.parse(in.readNBytes(ProtocolHeader.LENGTH));
    }

    void send(Frame frame) throws IOException {
        write(FrameCodec.encode(frame, 0xFFFF_FFFFL));
    }

    void send(Performative performative) throws IOException {
        send(Frame.amqp(0, performative));
    }

    Frame receive() throws IOException, AmqpDecodeException {
        byte[] start = in.readNBytes(4);
        if (start.length < 4) {
            throw new java.io.EOFException("the server closed the connection");
        }
        int size = FrameCodec.size(ByteBuffer.wrap(start), maxFrameSize);
        byte[] frame = new byte[size];
        System.arraycopy(start, 0, frame, 0, 4);
        assertEquals(size - 4, in.readNBytes(frame, 4, size - 4), "the whole frame arrives");
        return FrameCodec.decode(ByteBuffer.wrap(frame), maxFrameSize);
    }

    /** The next frame that is not a heartbeat. */
    Frame next() throws IOException, AmqpDecodeException {
        Frame frame = receive();
        while (frame.isEmpty()) {
            frame = receive();
        }
        return frame;
    }

    /** The next frame that is not a heartbeat, whose body must be of that type. */
    <T> T expect(Class<T> type) throws IOException, AmqpDecodeException {
        return type.cast(assertInstanceOf(type, next().body(), "from the server"));
    }

    /** Whether the server sends nothing but heartbeats for a short while. */
    boolean silent() throws IOException, AmqpDecodeException {
        socket.setSoTimeout(300);
        try {
            next();
            return false;
        } catch (SocketTimeoutException nothing) {
            return true;
        } finally {
            socket.setSoTimeout(5_000);
        }
    }

    /** Whether the server has closed its end: nothing more to read. */
    boolean closedByServer() throws IOException {
        try {
            return in.read() < 0;
        } catch (java.net.SocketException reset) {
            return true;
        }
    }

    // ---- what most tests start with ------------------------------------------

    /** The security layer, without credentials; returns the mechanisms the server offered. */
    List<Symbol> saslAnonymous() throws IOException, AmqpDecodeException {
        assertEquals(ProtocolHeader.SASL, exchange(ProtocolHeader.SASL));
        List<Symbol> offered = expect(SaslMechanisms.class).saslServerMechanisms();
        send(Frame.sasl(new SaslInit(Symbol.of("ANONYMOUS"), null, null)));
        assertEquals(0, expect(SaslOutcome.class).code());
        return offered;
    }

    /** The security layer with a user name and a password; returns the code of the outcome. */
    int saslPlain(String user, String password) throws IOException, AmqpDecodeException {
        assertEquals(ProtocolHeader.SASL, exchange(ProtocolHeader.SASL));
        expect(SaslMechanisms.class);
        byte[] response = ("\u0000" + user + "\u0000" + password).getBytes(StandardCharsets.UTF_8);
        send(Frame.sasl(new SaslInit(Symbol.of("PLAIN"), new Binary(response), null)));
        return expect(SaslOutcome.class).code();
    }

    Open open(String containerId) throws IOException, AmqpDecodeException {
        return open(new Open(containerId, "localhost", maxFrameSize, 100, null, null, null, null, null, null));
    }

    Open open(Open open) throws IOException, AmqpDecodeException {
        assertEquals(ProtocolHeader.AMQP, exchange(ProtocolHeader.AMQP));
        send(open);
        return expect(Open.class);
    }

    /** Anonymous, open, and a session on channel 0. */
    Wire ready() throws IOException, AmqpDecodeException {
        saslAnonymous();
        open("test-client");
        send(new Begin(null, 0L, 1000L, 1000L, null, null, null, null));
        expect(Begin.class);
        return this;
    }

    /** Attaches a link the client sends by; returns the credit the server grants. */
    long attachSender(long handle, String queue) throws IOException, AmqpDecodeException {
        send(new Attach("sender-" + handle, handle, false, null, null,
                new Source(null, null, null, null, null, null, null, null, null, null, null),
                new Target(queue, null, null, null, null, null, null), null, null, 0L, null, null, null, null));
        expect(Attach.class);
        return expect(Flow.class).linkCredit();
    }

    /** Attaches a link the client receives by, without credit yet. */
    Attach attachReceiver(long handle, String queue) throws IOException, AmqpDecodeException {
        send(new Attach("receiver-" + handle, handle, true, null, null,
                new Source(queue, null, null, null, null, null, null, null, null, null, null),
                new Target(null, null, null, null, null, null, null), null, null, null, null, null, null, null));
        return expect(Attach.class);
    }

    void credit(long handle, long deliveryCount, long credit, Boolean drain) throws IOException {
        send(new Flow(0L, 1000L, 0L, 1000L, handle, deliveryCount, credit, null, drain, null, null));
    }

    void transfer(long handle, long deliveryId, AmqpMessage message) throws IOException {
        send(Frame.amqp(0, new Transfer(handle, deliveryId, Binary.of((int) deliveryId), 0L, false, null, null, null,
                null, null, null), new Binary(message.encode())));
    }

    /** What one delivery looks like once its frames are put back together. */
    record Received(Transfer transfer, AmqpMessage message, int frames) {
    }

    Received delivery() throws IOException, AmqpDecodeException {
        ByteArrayOutputStream octets = new ByteArrayOutputStream();
        Frame frame = next();
        Transfer first = (Transfer) assertInstanceOf(Transfer.class, frame.body(), "from the server");
        octets.writeBytes(frame.payload().bytes());
        int frames = 1;
        Transfer last = first;
        while (Boolean.TRUE.equals(last.more())) {
            frame = next();
            last = (Transfer) assertInstanceOf(Transfer.class, frame.body(), "from the server");
            octets.writeBytes(frame.payload().bytes());
            frames++;
        }
        return new Received(first, AmqpMessage.decode(octets.toByteArray()), frames);
    }

    /** Ends the connection the way a client that crashes does. */
    void vanish() throws IOException {
        socket.close();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
