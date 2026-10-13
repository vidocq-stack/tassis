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
package io.vidocq.tassis.server.internal;

import static io.vidocq.tassis.amqp.internal.jms.Conventions.*;

import io.vidocq.tassis.amqp.internal.jms.JmsMessageCodec;
import io.vidocq.tassis.amqp.internal.messaging.Accepted;
import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.messaging.AmqpValue;
import io.vidocq.tassis.amqp.internal.messaging.DeliveryState;
import io.vidocq.tassis.amqp.internal.messaging.Modified;
import io.vidocq.tassis.amqp.internal.messaging.Rejected;
import io.vidocq.tassis.amqp.internal.messaging.Released;
import io.vidocq.tassis.amqp.internal.messaging.Source;
import io.vidocq.tassis.amqp.internal.messaging.Target;
import io.vidocq.tassis.amqp.internal.security.SaslInit;
import io.vidocq.tassis.amqp.internal.security.SaslMechanisms;
import io.vidocq.tassis.amqp.internal.security.SaslOutcome;
import io.vidocq.tassis.amqp.internal.transactions.Coordinator;
import io.vidocq.tassis.amqp.internal.transactions.Declare;
import io.vidocq.tassis.amqp.internal.transactions.Declared;
import io.vidocq.tassis.amqp.internal.transactions.Discharge;
import io.vidocq.tassis.amqp.internal.transactions.TransactionalState;
import io.vidocq.tassis.amqp.internal.transport.AmqpError;
import io.vidocq.tassis.amqp.internal.transport.Attach;
import io.vidocq.tassis.amqp.internal.transport.Begin;
import io.vidocq.tassis.amqp.internal.transport.Close;
import io.vidocq.tassis.amqp.internal.transport.Detach;
import io.vidocq.tassis.amqp.internal.transport.Disposition;
import io.vidocq.tassis.amqp.internal.transport.End;
import io.vidocq.tassis.amqp.internal.transport.Flow;
import io.vidocq.tassis.amqp.internal.transport.Frame;
import io.vidocq.tassis.amqp.internal.transport.FrameCodec;
import io.vidocq.tassis.amqp.internal.transport.Open;
import io.vidocq.tassis.amqp.internal.transport.Performative;
import io.vidocq.tassis.amqp.internal.transport.ProtocolHeader;
import io.vidocq.tassis.amqp.internal.transport.Transfer;
import io.vidocq.tassis.amqp.internal.transport.Transfers;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.AmqpEncoder;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.Described;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.core.TassisQueue;
import io.vidocq.tassis.core.TassisTopic;
import io.vidocq.tassis.core.internal.broker.Broker;
import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import io.vidocq.tassis.core.internal.message.TassisMessage;
import io.vidocq.tassis.core.internal.selector.Selector;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Topic;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * One client of the network: the AMQP 1.0 connection it opened, and the
 * {@link BrokerConnection} that stands for it in the broker.
 *
 * <p>Three kinds of virtual thread serve it. The <em>reader</em> reads the frames and does what
 * each asks; it never writes to the socket, so it can never be held up by a client that does
 * not read. The <em>writer</em> writes the frames the others queue, in the order they were
 * queued, and a heartbeat when it has had nothing to write for a while. One <em>pump</em> per
 * link the client receives by takes messages from the broker ({@link OutgoingLink}).
 *
 * <p>One lock guards the sessions, the links and their credit. It is taken before the broker's
 * own lock, never after, and is never held while waiting for the broker or for the socket.
 */
public final class AmqpConnection {

    private static final System.Logger LOG = System.getLogger(AmqpConnection.class.getName());

    /** The codes of a sasl-outcome (part 5, §5.3.3.6): authenticated, or refused for its credentials. */
    private static final int SASL_OK = 0;
    private static final int SASL_AUTH = 1;

    /** Tells the writer it has written its last frame. */
    private static final byte[] NO_MORE = new byte[0];

    final ReentrantLock lock = new ReentrantLock();
    final Condition changed = lock.newCondition();

    private final Socket socket;
    // The TCP socket under an encrypted one, or the socket itself: what is closed to end the
    // connection at once. Closing a TLS socket first says goodbye, behind whoever is writing.
    private final Socket transport;
    private final Broker broker;
    private final Consumer<AmqpConnection> onEnd;
    private final LinkedBlockingQueue<byte[]> outbound = new LinkedBlockingQueue<>();
    private final Map<Integer, AmqpSession> sessions = new HashMap<>();
    // The transactions declared and not discharged yet, by their identifier. Guarded by the lock.
    private final Map<Binary, Transaction> transactions = new HashMap<>();
    private long nextTransactionId;

    // Set by the reader before any pump exists, then only read.
    private volatile BrokerConnection link;
    private volatile long peerMaxFrameSize = Limits.MAX_SASL_FRAME_SIZE;
    // How often the writer sends a frame when it has nothing to say, in milliseconds; 0 for never.
    private volatile int heartbeat;
    private volatile boolean ended;
    private volatile Thread writer;
    // Counted down once the connection has given back to the broker what it held.
    private final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
    // What waits to be written, in octets: the measure of a client that does not read.
    private final java.util.concurrent.atomic.AtomicLong pending = new java.util.concurrent.atomic.AtomicLong();

    /**
     * @param socket what frames are read from and written to
     * @param transport the TCP socket {@code socket} is layered on, or {@code socket} itself
     */
    public AmqpConnection(Socket socket, Socket transport, Broker broker, Consumer<AmqpConnection> onEnd) {
        this.socket = socket;
        this.transport = transport;
        this.broker = broker;
        this.onEnd = onEnd;
    }

    /** Starts serving the client. */
    public void start() {
        String peer = String.valueOf(socket.getRemoteSocketAddress());
        writer = Thread.ofVirtual().name("tassis-amqp-writer-" + peer).start(this::write);
        Thread.ofVirtual().name("tassis-amqp-reader-" + peer).start(this::read);
    }

    /**
     * Waits until the connection has ended and given back to the broker what its client held,
     * which follows {@link #abort()} shortly.
     */
    public void awaitEnd(long timeoutMillis) {
        try {
            done.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Ends the connection at once, without the exchange of close frames: the server is stopping. */
    public void abort() {
        try {
            // The TCP socket first: a writer the client holds up fails at once, where the close
            // of a TLS socket would wait behind it for its turn to write.
            transport.close();
            socket.close();
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "closing a socket", e);
        }
    }

    // ---- the writer ----------------------------------------------------------

    private void write() {
        try {
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            while (true) {
                int pace = heartbeat;
                byte[] octets = pace > 0 ? outbound.poll(pace, TimeUnit.MILLISECONDS) : outbound.take();
                if (octets == NO_MORE) {
                    out.flush();
                    return;
                }
                if (octets != null) {
                    pending.addAndGet(-octets.length);
                }
                // Nothing to say for a while: an empty frame, so as not to be taken for dead (§2.4.5).
                out.write(octets == null ? FrameCodec.encode(Frame.EMPTY, Limits.MAX_SASL_FRAME_SIZE) : octets);
                if (outbound.isEmpty()) {
                    out.flush();
                }
            }
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "a client stopped reading", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            hangUp();
            onEnd.accept(this);
        }
    }

    /**
     * Closes the socket once the writer has nothing more to write. A TLS socket says goodbye,
     * which nothing holds up since only the writer writes, and must not then wait long for the
     * client to answer: TLS 1.3 would, for as long as a read may take.
     */
    private void hangUp() {
        try {
            socket.setSoTimeout(Limits.GOODBYE_TIME_OUT);
            socket.close();
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "closing a socket", e);
        }
        abort();
    }

    private void queue(byte[] octets) {
        if (pending.addAndGet(octets.length) > Limits.MAX_PENDING_OCTETS) {
            // A client that asks and never reads the answers: not worth the memory. Closing the
            // socket ends the reader, which ends the connection.
            abort();
            return;
        }
        outbound.add(octets);
    }

    private void send(int channel, Performative performative) {
        queue(FrameCodec.encode(Frame.amqp(channel, performative), peerMaxFrameSize));
    }

    // ---- the reader ----------------------------------------------------------

    private void read() {
        AmqpError why = null;
        try {
            // Not closed here: closing it closes the socket, and the writer still has the last
            // frames to send. The writer closes the socket when it is done.
            InputStream in = socket.getInputStream();
            socket.setSoTimeout(Limits.HANDSHAKE_TIME_OUT);
            if (!handshake(in)) {
                return;
            }
            socket.setSoTimeout(2 * Limits.IDLE_TIME_OUT);
            while (true) {
                Frame frame = readFrame(in, Limits.MAX_FRAME_SIZE);
                if (frame.isEmpty()) {
                    continue;
                }
                if (frame.body() instanceof Close) {
                    break;
                }
                handle(frame);
            }
        } catch (AmqpFailure failure) {
            why = failure.error();
        } catch (AmqpDecodeException e) {
            why = new AmqpError(AmqpFailure.DECODE_ERROR, e.getMessage(), null);
        } catch (IOException e) {
            // The client went away, or said nothing for too long: there is nobody to tell.
            LOG.log(System.Logger.Level.DEBUG, "a connection ended without a close", e);
            ended = true;
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "a defect while serving an AMQP client", e);
            why = new AmqpError(AmqpFailure.INTERNAL_ERROR, "the server failed", null);
        } finally {
            end(why);
        }
    }

    /**
     * Ends the connection: gives back to the broker what the client held, tells the client why
     * if it is still there, and lets the writer finish.
     */
    private void end(AmqpError why) {
        try {
            List<OutgoingLink> pumps = new ArrayList<>();
            lock.lock();
            try {
                for (AmqpSession session : sessions.values()) {
                    pumps.addAll(session.outgoing.values());
                    session.unsettled.clear();
                }
                for (OutgoingLink pump : pumps) {
                    pump.closed = true;
                }
                sessions.clear();
                // What their transactions held goes back with everything else, below.
                transactions.clear();
                changed.signalAll();
            } finally {
                lock.unlock();
            }
            BrokerConnection opened = link;
            if (opened != null) {
                // Closes the consumers, which ends the pumps, and gives back every unsettled delivery.
                opened.close();
            }
            if (!ended && headerSent) {
                send(0, new Close(brief(why)));
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "a defect while ending a connection", e);
        } finally {
            ended = true;
            outbound.add(NO_MORE);
            // The writer sends what is left and closes the socket; a client that does not read
            // is not waited for beyond this.
            Thread writing = writer;
            try {
                if (writing != null && !writing.join(java.time.Duration.ofMillis(Limits.LAST_WORDS_TIME_OUT))) {
                    abort();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                abort();
            }
            done.countDown();
        }
    }

    /** An error whose description fits the smallest frame a peer accepts (§2.7.1). */
    private static AmqpError brief(AmqpError error) {
        if (error == null || error.description() == null || error.description().length() <= 200) {
            return error;
        }
        return new AmqpError(error.condition(), error.description().substring(0, 200), error.info());
    }

    private boolean headerSent;

    private Frame readFrame(InputStream in, long maxFrameSize) throws IOException, AmqpDecodeException {
        byte[] start = in.readNBytes(4);
        if (start.length < 4) {
            throw new EOFException("the client closed the connection");
        }
        int size = FrameCodec.size(ByteBuffer.wrap(start), maxFrameSize);
        byte[] frame = new byte[size];
        System.arraycopy(start, 0, frame, 0, 4);
        if (in.readNBytes(frame, 4, size - 4) < size - 4) {
            throw new EOFException("the client closed the connection in the middle of a frame");
        }
        return FrameCodec.decode(ByteBuffer.wrap(frame), maxFrameSize);
    }

    // ---- opening: protocol headers, SASL, open (part 2 §2.2, part 5 §5.3) ------

    /** Returns false if the connection ended before it was open, with nothing more to do. */
    private boolean handshake(InputStream in) throws IOException, AmqpDecodeException, AmqpFailure {
        ProtocolHeader header = readHeader(in);
        if (header.equals(ProtocolHeader.SASL)) {
            queue(ProtocolHeader.SASL.octets());
            if (!authenticate(in)) {
                ended = true;
                return false;
            }
            header = readHeader(in);
        } else if (header.equals(ProtocolHeader.AMQP)) {
            // No security layer: a client nobody vouches for, which only some brokers accept.
            try {
                link = broker.connect(null, null);
            } catch (JMSSecurityException refused) {
                // §2.2: answer with the header of what is required, then close.
                queue(ProtocolHeader.SASL.octets());
                ended = true;
                return false;
            }
        }
        if (!header.equals(ProtocolHeader.AMQP)) {
            // Another version, or a protocol the server does not speak: say which one it does.
            queue(ProtocolHeader.AMQP.octets());
            ended = true;
            return false;
        }
        queue(ProtocolHeader.AMQP.octets());
        headerSent = true;

        Frame first = readFrame(in, Limits.MAX_FRAME_SIZE);
        if (first.channel() != 0 || !(first.body() instanceof Open open)) {
            throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "a connection starts with open, on channel 0");
        }
        long peerMax = open.maxFrameSize() == null ? Limits.DEFAULT_MAX_FRAME_SIZE : open.maxFrameSize();
        if (peerMax < FrameCodec.MIN_MAX_FRAME_SIZE) {
            throw new AmqpFailure(AmqpFailure.INVALID_FIELD, "max-frame-size cannot be below 512");
        }
        boolean sole = open.desiredCapabilities() != null && open.desiredCapabilities().contains(SOLE_CONNECTION);
        Map<Symbol, Object> properties = null;
        AmqpFailure refused = null;
        if (sole) {
            try {
                link.claimClientId(open.containerId());
            } catch (InvalidClientIDException taken) {
                // What a JMS client reads as "this client identifier is in use".
                properties = new LinkedHashMap<>();
                properties.put(CONNECTION_ESTABLISHMENT_FAILED, true);
                refused = new AmqpFailure(AmqpFailure.INVALID_FIELD, taken.getMessage());
            } catch (JMSException e) {
                throw new AmqpFailure(AmqpFailure.INTERNAL_ERROR, e.getMessage());
            }
        }
        queue(FrameCodec.encode(Frame.amqp(0, new Open("tassis-" + broker.name(), null, Limits.MAX_FRAME_SIZE,
                Limits.CHANNEL_MAX, (long) Limits.IDLE_TIME_OUT, null, null,
                List.of(ANONYMOUS_RELAY, SOLE_CONNECTION, SHARED_SUBS, DELAYED_DELIVERY, FLOW_ECHO),
                null, properties)), FrameCodec.MIN_MAX_FRAME_SIZE));
        peerMaxFrameSize = Math.min(peerMax, Limits.MAX_FRAME_SIZE);
        if (refused != null) {
            Map<Symbol, Object> info = new LinkedHashMap<>();
            info.put(Symbol.of("invalid-field"), Symbol.of("container-id"));
            send(0, new Close(new AmqpError(AmqpFailure.INVALID_FIELD, refused.getMessage(), info)));
            ended = true;
            return false;
        }
        if (open.idleTimeOut() != null && open.idleTimeOut() > 0) {
            // Half of what the client allows, which is what it asks for (§2.4.5).
            heartbeat = (int) Math.max(1, Math.min(open.idleTimeOut() / 2, Integer.MAX_VALUE));
            // Wakes a writer that started waiting without a pace.
            queue(FrameCodec.encode(Frame.EMPTY, FrameCodec.MIN_MAX_FRAME_SIZE));
        }
        link.start();
        return true;
    }

    private static ProtocolHeader readHeader(InputStream in) throws IOException, AmqpDecodeException {
        byte[] octets = in.readNBytes(ProtocolHeader.LENGTH);
        if (octets.length < ProtocolHeader.LENGTH) {
            throw new EOFException("the client closed the connection before saying what it speaks");
        }
        return ProtocolHeader.parse(octets);
    }

    /** The SASL exchange (part 5, §5.3.2): returns whether the client is who the broker accepts. */
    private boolean authenticate(InputStream in) throws IOException, AmqpDecodeException, AmqpFailure {
        List<Symbol> mechanisms = new ArrayList<>(List.of(PLAIN));
        if (!broker.users().isSecured() || broker.users().isAnonymousAllowed()) {
            mechanisms.add(ANONYMOUS);
        }
        queue(FrameCodec.encode(Frame.sasl(new SaslMechanisms(mechanisms)), Limits.MAX_SASL_FRAME_SIZE));
        Frame frame = readFrame(in, Limits.MAX_SASL_FRAME_SIZE);
        if (!(frame.body() instanceof SaslInit init)) {
            throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "the security layer starts with sasl-init");
        }
        String user = null;
        String password = null;
        boolean understood = true;
        if (init.mechanism().equals(PLAIN)) {
            // RFC 4616: [authorization identity] NUL authentication identity NUL password
            String[] parts = init.initialResponse() == null ? new String[0]
                    : new String(init.initialResponse().bytes(), StandardCharsets.UTF_8).split("\u0000", -1);
            understood = parts.length == 3;
            if (understood) {
                user = parts[1];
                password = parts[2];
            }
        } else if (!init.mechanism().equals(ANONYMOUS)) {
            understood = false;
        }
        if (understood) {
            try {
                link = broker.connect(user, password);
            } catch (JMSSecurityException refused) {
                // The client is told it was refused, and not why.
                LOG.log(System.Logger.Level.DEBUG, "a client was refused", refused);
            }
        }
        boolean accepted = link != null;
        queue(FrameCodec.encode(Frame.sasl(new SaslOutcome(accepted ? SASL_OK : SASL_AUTH, null)),
                Limits.MAX_SASL_FRAME_SIZE));
        return accepted;
    }

    // ---- frames of an open connection ----------------------------------------

    private void handle(Frame frame) throws AmqpFailure {
        int channel = frame.channel();
        if (frame.type() != Frame.TYPE_AMQP) {
            throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "the security layer is over: no SASL frame is expected");
        }
        if (frame.body() instanceof Begin begin) {
            begin(channel, begin);
            return;
        }
        AmqpSession session;
        lock.lock();
        try {
            session = sessions.get(channel);
        } finally {
            lock.unlock();
        }
        if (session == null) {
            throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "no session was begun on channel " + channel);
        }
        switch ((Performative) frame.body()) {
            case Attach attach -> attach(session, attach);
            case Flow flow -> flow(session, flow);
            case Transfer transfer -> transfer(session, transfer, frame.payload());
            case Disposition disposition -> disposition(session, disposition);
            case Detach detach -> detach(session, detach);
            case End end -> end(session);
            case Open open -> throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "the connection is already open");
            case Begin begin -> throw new IllegalStateException();
            case Close close -> throw new IllegalStateException();
        }
    }

    // ---- sessions (§2.5) -------------------------------------------------------

    private void begin(int channel, Begin begin) throws AmqpFailure {
        if (channel > Limits.CHANNEL_MAX) {
            throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "channel " + channel + " is beyond the channel-max announced");
        }
        AmqpSession session = new AmqpSession(channel, begin.nextOutgoingId(), begin.incomingWindow());
        lock.lock();
        try {
            if (sessions.putIfAbsent(channel, session) != null) {
                throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "a session is already begun on channel " + channel);
            }
            // The server answers on the channel the client chose: one number for both directions.
            send(channel, new Begin(channel, session.nextOutgoingId, session.incomingWindow, Limits.INCOMING_WINDOW,
                    Limits.HANDLE_MAX, null, null, null));
        } finally {
            lock.unlock();
        }
    }

    private void end(AmqpSession session) {
        List<OutgoingLink> links;
        List<BrokerConnection.Delivery> unsettled = new ArrayList<>();
        lock.lock();
        try {
            sessions.remove(session.channel);
            links = new ArrayList<>(session.outgoing.values());
            for (OutgoingLink link : links) {
                link.closed = true;
            }
            for (AmqpSession.Sent sent : session.unsettled.values()) {
                unsettled.add(sent.delivery());
            }
            // A transaction whose coordinator link goes is rolled back (part 4, §4.2).
            for (IncomingLink incoming : session.incoming.values()) {
                unsettled.addAll(rolledBack(incoming));
            }
            session.unsettled.clear();
            session.outgoing.clear();
            session.incoming.clear();
            session.browsers.clear();
            session.lookups.clear();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        for (OutgoingLink link : links) {
            link.consumer.close();
        }
        // What the client had received and not settled is delivered again (§2.6.12).
        this.link.redeliver(unsettled);
        send(session.channel, new End(null));
    }

    /**
     * Forgets the transactions declared on a coordinator link and not discharged, and returns
     * what they had consumed, for the caller to give back. Under the lock.
     */
    private List<BrokerConnection.Delivery> rolledBack(IncomingLink coordinator) {
        List<BrokerConnection.Delivery> consumed = new ArrayList<>();
        transactions.values().removeIf(transaction -> {
            if (transaction.coordinator == coordinator) {
                consumed.addAll(transaction.consumed);
                return true;
            }
            return false;
        });
        return consumed;
    }

    /** A flow frame saying where the session stands, for the caller to add what it says of a link. */
    private Flow sessionFlow(AmqpSession session, Long handle, Long deliveryCount, Long linkCredit, Boolean drain) {
        return new Flow(session.nextIncomingId, session.incomingWindow, session.nextOutgoingId,
                Limits.INCOMING_WINDOW, handle, deliveryCount, linkCredit, handle == null ? null : 0L, drain, null,
                null);
    }

    // ---- links (§2.6) ----------------------------------------------------------

    private void attach(AmqpSession session, Attach attach) throws AmqpFailure {
        if (attach.handle() > Limits.HANDLE_MAX) {
            throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "handle " + attach.handle() + " is beyond the handle-max");
        }
        lock.lock();
        try {
            // A handle refused a moment ago may be used again without waiting for the detach to cross.
            session.refused.remove(attach.handle());
            if (session.incoming.containsKey(attach.handle()) || session.outgoing.containsKey(attach.handle())
                    || session.browsers.containsKey(attach.handle()) || session.lookups.containsKey(attach.handle())) {
                throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "handle " + attach.handle() + " is in use");
            }
        } finally {
            lock.unlock();
        }
        // role: true when the peer is the receiving end of the link
        if (attach.role()) {
            attachOutgoing(session, attach);
        } else {
            attachIncoming(session, attach);
        }
    }

    /** Refuses a link: it is attached with nothing at the server's end, then detached with the reason (§2.6.3). */
    private void refuse(AmqpSession session, Attach attach, Symbol condition, String reason) {
        lock.lock();
        try {
            session.refused.add(attach.handle());
        } finally {
            lock.unlock();
        }
        send(session.channel, new Attach(attach.name(), attach.handle(), !attach.role(), null, null, null, null, null,
                null, attach.role() ? 0L : null, null, null, null, null));
        send(session.channel, new Detach(attach.handle(), true, new AmqpError(condition, reason, null)));
    }

    private static boolean has(List<Symbol> capabilities, Symbol... wanted) {
        if (capabilities == null) {
            return false;
        }
        for (Symbol capability : wanted) {
            if (capabilities.contains(capability)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether an address is one the broker gives to a temporary destination. A client need not
     * say that what it names is temporary, and a deleted one must not come back as a queue of
     * that name: over the network, such a name is a temporary destination or nothing.
     */
    private static boolean temporaryName(String address) {
        return address.startsWith(TEMPORARY_QUEUE_NAME) || address.startsWith(TEMPORARY_TOPIC_NAME);
    }

    /** What an address names in the broker, as far as the server can tell. */
    private record Named(String address, boolean topic, boolean temporary) {
    }

    /**
     * Says what an address names: a client tells whether it means a queue or a topic by a
     * capability of the terminus, and a temporary destination is known by the broker.
     *
     * @return null if the address is that of a temporary destination and no such one exists
     */
    private Named named(String address, List<Symbol> capabilities) {
        boolean temporaryQueue = link.hasTemporaryQueue(address);
        boolean temporaryTopic = link.hasTemporaryTopic(address);
        if (!temporaryQueue && !temporaryTopic && (temporaryName(address)
                || has(capabilities, CAPABILITY_TEMPORARY_QUEUE, CAPABILITY_TEMPORARY_TOPIC))) {
            return null;
        }
        boolean topic = temporaryTopic || !temporaryQueue && has(capabilities, CAPABILITY_TOPIC);
        return new Named(address, topic, temporaryQueue || temporaryTopic);
    }

    /** The client sends by this link. */
    private void attachIncoming(AmqpSession session, Attach attach) {
        long initialDeliveryCount = attach.initialDeliveryCount() == null ? 0 : attach.initialDeliveryCount();
        IncomingLink incoming;
        Object answeredTarget;
        if (attach.target() instanceof Coordinator) {
            // Part 4, §4.2: the link a client declares and discharges its transactions on.
            incoming = new IncomingLink(attach.handle(), attach.name(), IncomingLink.Kind.COORDINATOR, null, false,
                    false, initialDeliveryCount);
            answeredTarget = new Coordinator(List.of(LOCAL_TRANSACTIONS));
        } else if (!(attach.target() instanceof Target target)) {
            refuse(session, attach, AmqpFailure.INVALID_FIELD, "a sending link says where its messages go");
            return;
        } else if (Boolean.TRUE.equals(target.dynamic())) {
            // A node created for the client: how a temporary destination is asked for (§3.5.4).
            boolean topic = has(target.capabilities(), CAPABILITY_TEMPORARY_TOPIC, CAPABILITY_TOPIC);
            String created;
            try {
                created = topic ? link.createTemporaryTopic() : link.createTemporaryQueue();
            } catch (JMSException e) {
                refuse(session, attach, AmqpFailure.INTERNAL_ERROR, e.getMessage());
                return;
            }
            incoming = new IncomingLink(attach.handle(), attach.name(), IncomingLink.Kind.TEMPORARY, created, topic,
                    true, initialDeliveryCount);
            answeredTarget = new Target(created, target.durable(), target.expiryPolicy(), target.timeout(), true,
                    target.dynamicNodeProperties(), target.capabilities());
        } else if (target.address() == null) {
            // No address: an anonymous relay, where each message says where it goes.
            incoming = new IncomingLink(attach.handle(), attach.name(), IncomingLink.Kind.DESTINATION, null, false,
                    false, initialDeliveryCount);
            answeredTarget = target;
        } else if (!(target.address() instanceof String address)) {
            refuse(session, attach, AmqpFailure.INVALID_FIELD, "the address of a destination is a string");
            return;
        } else {
            Named named = named(address, target.capabilities());
            if (named == null) {
                refuse(session, attach, AmqpFailure.NOT_FOUND, "temporary destination " + address + " has been deleted");
                return;
            }
            incoming = new IncomingLink(attach.handle(), attach.name(), IncomingLink.Kind.DESTINATION, address,
                    named.topic(), named.temporary(), initialDeliveryCount);
            answeredTarget = target;
        }
        incoming.credit = Limits.PRODUCER_CREDIT;
        lock.lock();
        try {
            session.incoming.put(incoming.handle, incoming);
            send(session.channel, new Attach(attach.name(), attach.handle(), true, attach.sndSettleMode(), null,
                    attach.source(), answeredTarget, null, null, null, null, null, null, null));
            send(session.channel, sessionFlow(session, incoming.handle, incoming.deliveryCount, incoming.credit, null));
        } finally {
            lock.unlock();
        }
    }

    /** The name of the subscription a link reads, which is the name of the link up to a separator. */
    private static String subscriptionName(String linkName) {
        int end = linkName.indexOf(SUBSCRIPTION_NAME_END);
        return end < 0 ? linkName : linkName.substring(0, end);
    }

    /** The client receives by this link. */
    private void attachOutgoing(AmqpSession session, Attach attach) throws AmqpFailure {
        if (attach.source() == null) {
            lookUp(session, attach);
            return;
        }
        if (!(attach.source() instanceof Source source)) {
            refuse(session, attach, AmqpFailure.INVALID_FIELD, "a receiving link says what it reads");
            return;
        }
        if (Boolean.TRUE.equals(source.dynamic())) {
            refuse(session, attach, AmqpFailure.NOT_IMPLEMENTED,
                    "a temporary destination is created by a sending link, not by a receiving one");
            return;
        }
        if (!(source.address() instanceof String address)) {
            refuse(session, attach, AmqpFailure.INVALID_FIELD, "a receiving link names what it reads");
            return;
        }
        Named named = named(address, source.capabilities());
        if (named == null) {
            refuse(session, attach, AmqpFailure.NOT_FOUND, "temporary destination " + address + " has been deleted");
            return;
        }
        Selector selector = null;
        boolean noLocal = false;
        if (source.filter() != null) {
            noLocal = source.filter().containsKey(FILTER_NO_LOCAL);
            Object filter = source.filter().get(FILTER_SELECTOR);
            Object text = filter instanceof Described described ? described.value() : filter;
            if (text instanceof String selectorText) {
                try {
                    selector = Selector.parse(selectorText);
                } catch (InvalidSelectorException e) {
                    refuse(session, attach, AmqpFailure.INVALID_FIELD, e.getMessage());
                    return;
                }
            }
        }
        if (!named.topic() && DISTRIBUTION_COPY.equals(source.distributionMode())) {
            browse(session, attach, source, address, selector);
            return;
        }

        boolean durable = named.topic()
                && (source.durable() != null && source.durable() > 0 || EXPIRY_NEVER.equals(source.expiryPolicy()));
        boolean shared = named.topic() && has(source.capabilities(), CAPABILITY_SHARED);
        String subscription = durable || shared ? subscriptionName(attach.name()) : null;
        // A client without a client identifier of its own says so: every connection is given
        // one here, its container-id, and its subscriptions must not be scoped by that.
        boolean global = has(source.capabilities(), CAPABILITY_GLOBAL);
        BrokerConnection.Consumer consumer;
        try {
            consumer = named.topic()
                    ? link.subscribe(new BrokerConnection.Subscribing(address, named.temporary(), subscription, durable,
                            shared, noLocal, selector, global))
                    : link.attach(address, named.temporary(), selector);
        } catch (InvalidDestinationException e) {
            refuse(session, attach, AmqpFailure.NOT_FOUND, e.getMessage());
            return;
        } catch (JMSException e) {
            // A subscription in use, or one of another kind under that name (§8.3).
            refuse(session, attach, AmqpFailure.RESOURCE_LOCKED, e.getMessage());
            return;
        }
        // snd-settle-mode 1: the client wants what it is sent to be settled already (§2.8.2)
        boolean presettled = Integer.valueOf(1).equals(attach.sndSettleMode());
        OutgoingLink pump = new OutgoingLink(this, session, attach.handle(), attach.name(), consumer, presettled,
                durable ? subscription : null, global);
        lock.lock();
        try {
            session.outgoing.put(pump.handle, pump);
            send(session.channel, new Attach(attach.name(), attach.handle(), false, attach.sndSettleMode(),
                    attach.rcvSettleMode(), source, attach.target(), null, null, 0L, null,
                    shared ? List.of(SHARED_SUBS) : null, null, null));
        } finally {
            lock.unlock();
        }
        Thread.ofVirtual().name("tassis-amqp-pump-" + attach.name()).start(pump);
    }

    /**
     * A receiving link attached with no source: the client looks for the durable subscription
     * the link is named after, to delete it by closing the link (Jakarta Messaging 3.1 §8.3.5).
     */
    private void lookUp(AmqpSession session, Attach attach) {
        String name = subscriptionName(attach.name());
        // Said by a capability, or by the name of the link as Qpid JMS writes it. Anything else
        // after the name of the subscription tells links apart, and says nothing of its scope.
        boolean global = has(attach.desiredCapabilities(), CAPABILITY_GLOBAL)
                || attach.name().startsWith(name + SUBSCRIPTION_NAME_END + GLOBAL_SUFFIX);
        String topic = link.durableSubscriptionTopic(name, global);
        if (topic == null) {
            refuse(session, attach, AmqpFailure.NOT_FOUND, "there is no durable subscription named " + name);
            return;
        }
        lock.lock();
        try {
            session.lookups.put(attach.handle(), new AmqpSession.LookedUp(name, global));
            send(session.channel, new Attach(attach.name(), attach.handle(), false, null, null,
                    new Source(topic, 2L, EXPIRY_NEVER, null, null, null, null, null, null, null,
                            List.of(CAPABILITY_TOPIC)),
                    attach.target(), null, null, 0L, null, null, null, null));
        } finally {
            lock.unlock();
        }
    }

    /** A queue browser: the messages of the queue as they are now, to be sent as credit comes. */
    private void browse(AmqpSession session, Attach attach, Source source, String queue, Selector selector) {
        BrowserLink browser;
        try {
            browser = new BrowserLink(attach.handle(), link.browse(queue, selector));
        } catch (JMSException e) {
            refuse(session, attach, AmqpFailure.INTERNAL_ERROR, e.getMessage());
            return;
        }
        lock.lock();
        try {
            session.browsers.put(browser.handle, browser);
            // Settled as they are sent: there is nothing to acknowledge in what is only looked at.
            send(session.channel, new Attach(attach.name(), attach.handle(), false, 1, attach.rcvSettleMode(), source,
                    attach.target(), null, null, 0L, null, null, null, null));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sends a browser what its credit and the window of the session allow, one frame at a time:
     * what the window has no room for waits in the link for the next flow. Under the lock.
     */
    private void feed(AmqpSession session, BrowserLink browser) {
        while (session.remoteIncomingWindow > 0) {
            byte[] frame = browser.frames.poll();
            if (frame != null) {
                session.nextOutgoingId++;
                session.remoteIncomingWindow--;
                queue(frame);
                continue;
            }
            if (browser.credit == 0 || browser.next >= browser.messages.size()) {
                break;
            }
            TassisMessage message = browser.messages.get(browser.next++);
            byte[] octets;
            try {
                octets = JmsMessageCodec.toAmqp(message).encode();
            } catch (JMSException | RuntimeException e) {
                LOG.log(System.Logger.Level.ERROR, "a message could not be written for a browser: it is skipped", e);
                continue;
            }
            // Given its identifier with room for its first frame: identifiers follow the wire.
            long id = session.nextDeliveryId++;
            browser.credit--;
            browser.deliveryCount++;
            Transfer first = new Transfer(browser.handle, id, new Binary(AmqpEncoder.encode(id)), 0L, true, null, null,
                    null, null, null, null);
            browser.frames.addAll(split(session.channel, first, octets));
        }
        if (browser.draining && browser.frames.isEmpty()
                && (browser.credit == 0 || browser.next >= browser.messages.size())) {
            // Nothing more to look at: how a client knows it has seen the whole queue (§2.6.7).
            browser.deliveryCount += browser.credit;
            browser.credit = 0;
            browser.draining = false;
            send(session.channel, sessionFlow(session, browser.handle, browser.deliveryCount, 0L, true));
        }
    }

    private void detach(AmqpSession session, Detach detach) throws AmqpFailure {
        boolean closed = Boolean.TRUE.equals(detach.closed());
        OutgoingLink pump;
        IncomingLink incoming;
        AmqpSession.LookedUp lookedUp;
        List<BrokerConnection.Delivery> unsettled = new ArrayList<>();
        lock.lock();
        try {
            pump = session.outgoing.remove(detach.handle());
            incoming = session.incoming.remove(detach.handle());
            lookedUp = session.lookups.remove(detach.handle());
            if (pump == null && incoming == null && lookedUp == null && session.browsers.remove(detach.handle()) == null) {
                if (session.refused.remove(detach.handle())) {
                    // The client's answer to the detach that refused its link (§2.6.5).
                    return;
                }
                throw new AmqpFailure(AmqpFailure.UNATTACHED_HANDLE, "no link is attached with handle " + detach.handle());
            }
            if (pump != null) {
                pump.closed = true;
                session.unsettled.values().removeIf(sent -> {
                    if (sent.link() == pump) {
                        unsettled.add(sent.delivery());
                        return true;
                    }
                    return false;
                });
                changed.signalAll();
            }
            if (incoming != null && incoming.kind == IncomingLink.Kind.COORDINATOR) {
                unsettled.addAll(rolledBack(incoming));
            }
        } finally {
            lock.unlock();
        }
        AmqpError error = null;
        try {
            if (pump != null) {
                pump.consumer.close();
                if (closed && pump.durableName != null) {
                    // Closed, not merely detached: the subscription goes with its link (§2.6.6).
                    link.unsubscribe(pump.durableName, pump.global);
                }
            }
            if (lookedUp != null && closed) {
                link.unsubscribe(lookedUp.name(), lookedUp.global());
            }
            if (incoming != null && incoming.kind == IncomingLink.Kind.TEMPORARY && closed) {
                if (incoming.topic) {
                    link.deleteTemporaryTopic(incoming.address);
                } else {
                    link.deleteTemporaryQueue(incoming.address);
                }
            }
        } catch (JMSException e) {
            // In use by a consumer: it stays, and the client is told why.
            error = new AmqpError(AmqpFailure.RESOURCE_LOCKED, e.getMessage(), null);
        }
        link.redeliver(unsettled);
        send(session.channel, new Detach(detach.handle(), true, error));
    }

    // ---- flow control (§2.6.7) ---------------------------------------------------

    private void flow(AmqpSession session, Flow flow) throws AmqpFailure {
        OutgoingLink pump = null;
        boolean withdrawn = false;
        lock.lock();
        try {
            // What the client still accepts, counted from the transfer the server sends next (§2.5.6).
            long clientNextIncomingId = flow.nextIncomingId() == null ? session.nextOutgoingId : flow.nextIncomingId();
            session.remoteIncomingWindow =
                    Math.max(0, clientNextIncomingId + flow.incomingWindow() - session.nextOutgoingId);
            if (flow.handle() != null) {
                pump = session.outgoing.get(flow.handle());
                IncomingLink incoming = session.incoming.get(flow.handle());
                BrowserLink browser = session.browsers.get(flow.handle());
                if (browser != null) {
                    long clientDeliveryCount =
                            flow.deliveryCount() == null ? browser.deliveryCount : flow.deliveryCount();
                    long credit = flow.linkCredit() == null ? 0 : flow.linkCredit();
                    browser.credit = Math.max(0, clientDeliveryCount + credit - browser.deliveryCount);
                    // Kept until it is answered: the queue may take several flows to be seen.
                    browser.draining = Boolean.TRUE.equals(flow.drain());
                    if (Boolean.TRUE.equals(flow.echo())) {
                        send(session.channel, sessionFlow(session, browser.handle, browser.deliveryCount,
                                browser.credit, null));
                    }
                } else if (pump == null && incoming == null && !session.lookups.containsKey(flow.handle())
                        && !session.refused.contains(flow.handle())) {
                    throw new AmqpFailure(AmqpFailure.UNATTACHED_HANDLE, "no link is attached with handle " + flow.handle());
                }
                if (pump != null) {
                    // The receiver's credit is counted from its own view of the delivery-count.
                    long clientDeliveryCount = flow.deliveryCount() == null ? pump.deliveryCount : flow.deliveryCount();
                    long credit = flow.linkCredit() == null ? 0 : flow.linkCredit();
                    pump.credit = Math.max(0, clientDeliveryCount + credit - pump.deliveryCount);
                    pump.draining = Boolean.TRUE.equals(flow.drain()) && pump.credit > 0;
                    withdrawn = pump.credit == 0 || pump.draining;
                    if (Boolean.TRUE.equals(flow.drain()) && pump.credit == 0 || Boolean.TRUE.equals(flow.echo())) {
                        send(session.channel, sessionFlow(session, pump.handle, pump.deliveryCount, pump.credit,
                                flow.drain()));
                    }
                } else if (incoming != null && Boolean.TRUE.equals(flow.echo())) {
                    send(session.channel, sessionFlow(session, incoming.handle, incoming.deliveryCount, incoming.credit,
                            null));
                }
            } else if (Boolean.TRUE.equals(flow.echo())) {
                send(session.channel, sessionFlow(session, null, null, null, null));
            }
            // Credit for one of them, or a window that opens for those that waited for room.
            for (BrowserLink waiting : session.browsers.values()) {
                feed(session, waiting);
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        if (pump != null && withdrawn) {
            // After the state has changed: a pump that waits for a message stops waiting and looks.
            pump.consumer.listen(false);
        }
    }

    /** Whether the pump of a link may wait for a message: called by the pump once it listens. */
    boolean mayDeliver(OutgoingLink pump) {
        lock.lock();
        try {
            return !pump.closed && pump.credit > 0 && !pump.draining;
        } finally {
            lock.unlock();
        }
    }

    /** A drain found no message: the credit is given up, and the client told (§2.6.7). */
    void creditGivenUp(OutgoingLink pump) {
        lock.lock();
        try {
            if (pump.closed || !pump.draining) {
                return;
            }
            pump.deliveryCount += pump.credit;
            pump.credit = 0;
            pump.draining = false;
            send(pump.session.channel, sessionFlow(pump.session, pump.handle, pump.deliveryCount, 0L, true));
        } finally {
            lock.unlock();
        }
    }

    // ---- to the client (§2.6.12) -------------------------------------------------

    /** Sends a message the pump took. If the client no longer has credit for it, it goes back unseen. */
    void deliver(OutgoingLink pump, BrokerConnection.Delivery delivery) {
        byte[] octets;
        try {
            octets = JmsMessageCodec.toAmqp(delivery.message()).encode();
        } catch (JMSException | RuntimeException e) {
            // Giving it back would only bring it here again.
            LOG.log(System.Logger.Level.ERROR, "a message could not be written for AMQP: it is dropped", e);
            link.settle(delivery);
            return;
        }
        AmqpSession session = pump.session;
        boolean sent = false;
        lock.lock();
        try {
            // Room for one frame at least, before the delivery is given its identifier: another
            // link of the session may send while this one waits, and identifiers follow the wire.
            while (!pump.closed && pump.credit > 0 && session.remoteIncomingWindow < 1) {
                changed.awaitUninterruptibly();
            }
            if (!pump.closed && pump.credit > 0) {
                long id = session.nextDeliveryId++;
                pump.credit--;
                pump.deliveryCount++;
                if (pump.credit == 0) {
                    pump.draining = false;
                }
                if (pump.presettled) {
                    // Before anything is sent: a message the client is about to have for good
                    // must not come back if the connection ends now.
                    link.settle(delivery);
                } else {
                    session.unsettled.put(id, new AmqpSession.Sent(pump, delivery));
                }
                Transfer first = new Transfer(pump.handle, id, new Binary(AmqpEncoder.encode(id)), 0L,
                        pump.presettled ? Boolean.TRUE : null, null, null, null, null, null, null);
                // Queued under the lock, each frame as the window has room for it: a message
                // longer than the window is not held back for a room that may never come.
                for (byte[] frame : split(session.channel, first, octets)) {
                    while (!pump.closed && session.remoteIncomingWindow < 1) {
                        changed.awaitUninterruptibly();
                    }
                    if (pump.closed) {
                        // The link is gone: the client drops what it had of this delivery (§2.6.14).
                        break;
                    }
                    session.nextOutgoingId++;
                    session.remoteIncomingWindow--;
                    queue(frame);
                }
                sent = true;
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "a message could not be sent to a client: it is dropped", e);
            session.unsettled.values().removeIf(entry -> entry.delivery() == delivery);
            link.settle(delivery);
            sent = true;
        } finally {
            lock.unlock();
        }
        if (!sent) {
            link.returnUndelivered(delivery);
        }
    }

    /** The transfer frames of one message, none larger than the client accepts (§2.7.5). */
    private List<byte[]> split(int channel, Transfer first, byte[] message) {
        return Transfers.split(channel, first, message, peerMaxFrameSize);
    }

    private void disposition(AmqpSession session, Disposition disposition) {
        if (!disposition.role()) {
            // From the sending end of a link: about messages the client sent, which the server
            // settles as it accepts them. Nothing is waiting for this.
            return;
        }
        long last = disposition.last() == null ? disposition.first() : disposition.last();
        Object state = disposition.state();
        lock.lock();
        try {
            // Only what is really unsettled in the range: the range itself may be any size.
            Map<Long, AmqpSession.Sent> named =
                    new LinkedHashMap<>(session.unsettled.subMap(disposition.first(), true, last, true));
            // Part 4, §4.4.2: an outcome that only holds if its transaction is committed.
            Transaction transaction =
                    state instanceof TransactionalState transactional ? transactions.get(transactional.txnId()) : null;
            if (state instanceof TransactionalState transactional) {
                // In a transaction that is over, or never was, an outcome holds for nothing:
                // the message is delivered again, as after a rollback.
                state = transaction == null ? null
                        : transactional.outcome() == null ? new Accepted() : transactional.outcome();
            }
            for (Map.Entry<Long, AmqpSession.Sent> entry : named.entrySet()) {
                BrokerConnection.Delivery delivery = entry.getValue().delivery();
                switch (state) {
                    // Not seen by the application: as if it had not been delivered.
                    case Released released -> link.returnUndelivered(delivery);
                    case Modified modified when Boolean.TRUE.equals(modified.undeliverableHere()) ->
                        // The client will never take it, and there is nowhere else to send it.
                        link.settle(delivery);
                    case Modified modified when Boolean.TRUE.equals(modified.deliveryFailed()) ->
                        link.redeliver(List.of(delivery));
                    case Modified modified -> link.returnUndelivered(delivery);
                    // Consumed when the transaction commits, delivered again if it does not.
                    case Accepted accepted when transaction != null -> transaction.consumed.add(delivery);
                    case Accepted accepted -> link.settle(delivery);
                    // Invalid for the client: delivering it again would change nothing.
                    case Rejected rejected -> link.settle(delivery);
                    // Settled without saying what became of it: nothing shows it was consumed.
                    case null, default -> link.redeliver(List.of(delivery));
                }
                session.unsettled.remove(entry.getKey());
            }
            if (!Boolean.TRUE.equals(disposition.settled()) && !named.isEmpty()
                    && disposition.state() instanceof DeliveryState answered) {
                // The client waits for the server to settle first (rcv-settle-mode second).
                send(session.channel, new Disposition(false, disposition.first(), last, true, answered, null));
            }
        } finally {
            lock.unlock();
        }
    }

    // ---- from the client (§2.6.12, §2.7.5) ----------------------------------------

    private void transfer(AmqpSession session, Transfer transfer, Binary payload) throws AmqpFailure {
        IncomingLink incoming;
        byte[] message = null;
        Long deliveryId;
        Binary transactionId;
        boolean settled;
        Flow moreCredit = null;
        lock.lock();
        try {
            incoming = session.incoming.get(transfer.handle());
            if (incoming == null && session.refused.contains(transfer.handle())) {
                // Sent before the client learnt that its link was refused: nowhere to go.
                return;
            }
            if (incoming == null) {
                throw new AmqpFailure(AmqpFailure.UNATTACHED_HANDLE,
                        "no sending link is attached with handle " + transfer.handle());
            }
            session.nextIncomingId++;
            session.incomingWindow--;
            boolean more = Boolean.TRUE.equals(transfer.more());
            // A message that came in one frame is the payload of that frame: nothing to put together.
            boolean whole = incoming.partial == null && !more;
            if (incoming.partial == null) {
                if (incoming.credit <= 0) {
                    throw new AmqpFailure(AmqpFailure.NOT_ALLOWED, "a transfer on a link that has no credit");
                }
                if (!whole) {
                    incoming.partial = new ByteArrayOutputStream(2 * payload.length());
                }
                incoming.partialDeliveryId = transfer.deliveryId();
                incoming.partialSettled = Boolean.TRUE.equals(transfer.settled());
                // Part 4, §4.4.1: a message that only arrives if its transaction is committed.
                incoming.partialTransaction =
                        transfer.state() instanceof TransactionalState transactional ? transactional.txnId() : null;
            }
            if ((whole ? 0 : incoming.partial.size()) + (long) payload.length() > Limits.MAX_MESSAGE_SIZE) {
                throw new AmqpFailure(AmqpFailure.MESSAGE_SIZE_EXCEEDED,
                        "a message larger than " + Limits.MAX_MESSAGE_SIZE + " octets");
            }
            if (!whole) {
                incoming.partial.writeBytes(payload.array());
            }
            if (more) {
                return;
            }
            deliveryId = incoming.partialDeliveryId;
            transactionId = incoming.partialTransaction;
            settled = incoming.partialSettled || Boolean.TRUE.equals(transfer.settled());
            if (!Boolean.TRUE.equals(transfer.aborted())) {
                message = whole ? payload.array() : incoming.partial.toByteArray();
            }
            incoming.partial = null;
            incoming.credit--;
            incoming.deliveryCount++;
            if (incoming.credit <= Limits.PRODUCER_CREDIT / 2 || session.incomingWindow <= Limits.INCOMING_WINDOW / 2) {
                incoming.credit = Limits.PRODUCER_CREDIT;
                session.incomingWindow = Limits.INCOMING_WINDOW;
                moreCredit = sessionFlow(session, incoming.handle, incoming.deliveryCount, incoming.credit, null);
            }
        } finally {
            lock.unlock();
        }
        if (message != null) {
            // Outside the lock: the broker is asked to store the message.
            DeliveryState outcome = switch (incoming.kind) {
                case COORDINATOR -> coordinate(incoming, message);
                case TEMPORARY -> rejected(AmqpFailure.NOT_ALLOWED,
                        "this link stands for a temporary destination: messages for it go by a link of their own");
                case DESTINATION -> store(incoming, message, transactionId);
            };
            if (!settled && deliveryId != null) {
                send(session.channel, new Disposition(true, deliveryId, deliveryId, true, outcome, null));
            }
        }
        if (moreCredit != null) {
            send(session.channel, moreCredit);
        }
    }

    /** Hands a message to the broker, or to its transaction, and says what became of it. */
    private DeliveryState store(IncomingLink incoming, byte[] octets, Binary transactionId) {
        try {
            TassisMessage message = JmsMessageCodec.toJms(AmqpMessage.decode(octets));
            String address = incoming.address;
            boolean topic = incoming.topic;
            boolean temporary = incoming.temporary;
            if (address == null) {
                // An anonymous relay: the message says where it goes.
                address = switch (message.getJMSDestination()) {
                    case TassisQueue queue -> queue.getQueueName();
                    case TassisTopic named -> named.getTopicName();
                    case null, default -> null;
                };
                if (address == null) {
                    return rejected(AmqpFailure.INVALID_FIELD,
                            "a message sent to no link in particular names its destination in 'to'");
                }
                // Decided by the name alone: the broker then refuses a temporary destination
                // that is gone, in the same request that would have stored the message.
                temporary = temporaryName(address);
                topic = temporary ? address.startsWith(TEMPORARY_TOPIC_NAME) : message.getJMSDestination() instanceof Topic;
            }
            message.setJMSDestination(topic ? new TassisTopic(address) : new TassisQueue(address));
            if (message.getJMSMessageID() == null) {
                message.setJMSMessageID(link.nextMessageId());
            }
            BrokerConnection.Outgoing outgoing = new BrokerConnection.Outgoing(address, topic, temporary, message);
            if (transactionId == null) {
                link.send(outgoing);
                return new Accepted();
            }
            lock.lock();
            try {
                Transaction transaction = transactions.get(transactionId);
                if (transaction == null) {
                    return rejected(AmqpFailure.UNKNOWN_TRANSACTION, "no such transaction: it was never declared, or is over");
                }
                if (transaction.octets + octets.length > Limits.MAX_TRANSACTION_OCTETS) {
                    return rejected(AmqpFailure.RESOURCE_LIMIT_EXCEEDED, "a transaction holds no more than "
                            + Limits.MAX_TRANSACTION_OCTETS + " octets of messages before it is discharged");
                }
                transaction.octets += octets.length;
                transaction.sends.add(outgoing);
            } finally {
                lock.unlock();
            }
            return new TransactionalState(transactionId, new Accepted());
        } catch (AmqpDecodeException e) {
            return rejected(AmqpFailure.DECODE_ERROR, e.getMessage());
        } catch (InvalidDestinationException e) {
            return rejected(AmqpFailure.NOT_FOUND, e.getMessage());
        } catch (JMSException e) {
            return rejected(AmqpFailure.INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * What a client sends on a coordinator link: declare, which begins a transaction, or
     * discharge, which ends one (part 4, §4.3).
     */
    private DeliveryState coordinate(IncomingLink coordinator, byte[] octets) {
        Object request;
        try {
            AmqpMessage message = AmqpMessage.decode(octets);
            request = message.body().size() == 1 && message.body().get(0) instanceof AmqpValue value ? value.value() : null;
        } catch (AmqpDecodeException e) {
            return rejected(AmqpFailure.DECODE_ERROR, e.getMessage());
        }
        if (request instanceof Declare) {
            lock.lock();
            try {
                if (transactions.size() >= Limits.MAX_TRANSACTIONS) {
                    return rejected(AmqpFailure.RESOURCE_LIMIT_EXCEEDED, "a connection holds no more than "
                            + Limits.MAX_TRANSACTIONS + " transactions that are not discharged");
                }
                Binary id = new Binary(AmqpEncoder.encode(nextTransactionId++));
                transactions.put(id, new Transaction(id, coordinator));
                return new Declared(id);
            } finally {
                lock.unlock();
            }
        }
        if (!(request instanceof Discharge discharge)) {
            return rejected(AmqpFailure.NOT_ALLOWED, "a coordinator is sent declare or discharge, and nothing else");
        }
        Transaction transaction;
        lock.lock();
        try {
            transaction = transactions.remove(discharge.txnId());
        } finally {
            lock.unlock();
        }
        if (transaction == null) {
            return rejected(AmqpFailure.UNKNOWN_TRANSACTION, "no such transaction: it was never declared, or is over");
        }
        if (Boolean.TRUE.equals(discharge.fail())) {
            // Rolled back: nothing was sent, and what was received is delivered again (§6.2.7).
            link.redeliver(transaction.consumed);
            return new Accepted();
        }
        try {
            // One request to the broker: the sends and the acknowledgements together, or neither.
            link.commit(transaction.sends, transaction.consumed);
            return new Accepted();
        } catch (JMSException e) {
            link.redeliver(transaction.consumed);
            // Not amqp:transaction:rollback: a client such as Qpid JMS takes a rejected commit
            // for a rollback whatever the condition, and that very condition for a doubt.
            return rejected(e instanceof InvalidDestinationException ? AmqpFailure.NOT_FOUND : AmqpFailure.INTERNAL_ERROR,
                    "the transaction was rolled back: " + e.getMessage());
        }
    }

    private static Rejected rejected(Symbol condition, String reason) {
        return new Rejected(new AmqpError(condition, reason, null));
    }
}
