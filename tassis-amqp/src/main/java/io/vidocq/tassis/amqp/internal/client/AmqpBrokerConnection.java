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
package io.vidocq.tassis.amqp.internal.client;

import static io.vidocq.tassis.amqp.internal.jms.Conventions.*;

import io.vidocq.tassis.amqp.internal.jms.JmsMessageCodec;
import io.vidocq.tassis.amqp.internal.messaging.Accepted;
import io.vidocq.tassis.amqp.internal.messaging.AmqpMessage;
import io.vidocq.tassis.amqp.internal.messaging.AmqpValue;
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
import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import io.vidocq.tassis.core.internal.message.TassisMessage;
import io.vidocq.tassis.core.internal.selector.Selector;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * A client's connection to a broker of the network, over AMQP 1.0: what {@code LocalConnection}
 * is for the broker of the JVM. It speaks to a server on the conventions of
 * {@link io.vidocq.tassis.amqp.internal.jms.Conventions}, which are those the Tassis server
 * reads.
 *
 * <p>Two virtual threads serve a connection. The <em>reader</em> reads the frames of the broker
 * and records what they say; it never writes to the socket. The <em>writer</em> writes the
 * frames the others queue, in the order they were queued, and a heartbeat when it has had
 * nothing to write for a while. The threads of the application wait for what they asked.
 *
 * <p>One lock guards everything, and one condition tells whoever waits that something has
 * changed. It is never held while waiting for the broker: every wait releases it. Frames are
 * queued under it, so that the order of the wire is the order of the decisions.
 *
 * <p>One AMQP session carries the whole connection. Messages are sent by one link without an
 * address (an anonymous relay), where each message says where it goes; each consumer has a link
 * of its own, granted one credit at a time: a message the application has not asked for stays
 * with the broker, where another consumer can take it.
 */
public final class AmqpBrokerConnection implements BrokerConnection {

    private static final System.Logger LOG = System.getLogger(AmqpBrokerConnection.class.getName());

    /** The largest frame the client accepts, announced in its open (part 2, §2.7.1). */
    private static final long MAX_FRAME_SIZE = 256 * 1024;
    /** What the client announces as its idle time-out: a broker silent for twice as long is given up (§2.4.5). */
    private static final int IDLE_TIME_OUT = 60_000;
    /** How long the client waits for the broker to accept a connection, then for each step of its opening. */
    private static final int CONNECT_TIME_OUT = 10_000;
    /** How long the closing of an encrypted socket waits for the broker to say goodbye too. */
    private static final int GOODBYE_TIME_OUT = 200;
    /** How long the client waits for the broker to answer its close before it closes the socket. */
    private static final long CLOSE_TIME_OUT = 5_000;
    /** The session window the client grants and assumes: how many transfer frames (§2.5.6). */
    private static final long WINDOW = Integer.MAX_VALUE;
    private static final long HANDLE_MAX = 65_535;
    /** How many messages a browser asks for at a time. */
    private static final long BROWSE_CREDIT = 200;
    private static final Symbol NOT_FOUND = Symbol.of("amqp:not-found");
    private static final Symbol INVALID_FIELD = Symbol.of("amqp:invalid-field");
    private static final Symbol UNAUTHORIZED_ACCESS = Symbol.of("amqp:unauthorized-access");

    private static final Source NO_SOURCE = new Source(null, null, null, null, null, null, null, null, null, null, null);
    private static final Target NO_TARGET = new Target(null, null, null, null, null, null, null);

    /** Tells the writer it has written its last frame. */
    private static final byte[] NO_MORE = new byte[0];

    /** One socket and what serves it: replaced when a client identifier is claimed. */
    private static final class Wire {
        final Socket socket;
        /** The TCP socket under an encrypted one, or the socket itself. */
        final Socket transport;
        final LinkedBlockingQueue<byte[]> outbound = new LinkedBlockingQueue<>();
        long peerMaxFrameSize;
        int heartbeat;
        /** Whether the broker answers a session flow that asks for an echo: see {@code sync}. */
        boolean echoes;
        Thread reader;
        Thread writer;
        // Guarded by the lock of the connection.
        boolean closing;
        boolean ended;

        Wire(Socket socket, Socket transport) {
            this.socket = socket;
            this.transport = transport;
        }

        /**
         * Ends the conversation at once. The TCP socket first: a writer the broker holds up
         * fails at once, where the close of a TLS socket would wait behind it to say goodbye.
         */
        void cut() {
            closeQuietly(transport);
            closeQuietly(socket);
        }
    }

    /** What the broker answered to an attach or to a detach. */
    private static final class Reply {
        Attach attach;
        AmqpError error;
        boolean refused;
        boolean done;
    }

    /** A message the client sent, until the broker says what became of it. */
    private static final class Sent {
        Object state;
        boolean done;
    }

    /** A link the client sends by: its credit is the broker's to grant (§2.6.7). */
    private static final class SenderLink {
        final long handle;
        long credit;
        long deliveryCount;
        /** Ended by the broker: nothing more can be sent by it. */
        boolean detached;

        SenderLink(long handle) {
            this.handle = handle;
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    private final String host;
    private final int port;
    private final boolean encrypted;
    // Whom to trust on an encrypted connection; null for what the JVM trusts by default.
    private final SSLContext tls;
    private final String user;
    private final String password;
    private final String messageIdPrefix = "ID:" + UUID.randomUUID() + "-";
    private final AtomicLong messageIds = new AtomicLong();

    // Everything below is guarded by the lock.
    private Wire wire;
    private String clientId;
    private volatile boolean started;
    private boolean closed;
    private JMSException lost;
    private java.util.function.Consumer<JMSException> lostListener;

    private long nextOutgoingId;
    private long nextIncomingId;
    private long remoteIncomingWindow;
    private long nextDeliveryId;
    private long nextHandle;
    private final TreeSet<Long> freeHandles = new TreeSet<>();
    private long names;
    // How many links this connection has attached to each shared subscription, by its name.
    private final Map<String, Integer> subscriptionLinks = new HashMap<>();
    private final Map<Long, Reply> attaching = new HashMap<>();
    private final Map<Long, Reply> detaching = new HashMap<>();
    private final Map<Long, SenderLink> senders = new HashMap<>();
    private final Map<Long, ReceiverLink> receivers = new LinkedHashMap<>();
    private final TreeMap<Long, Sent> sent = new TreeMap<>();
    // What the broker delivered and the client has neither settled nor given back.
    private final Map<Long, AmqpDelivery> unsettled = new HashMap<>();
    // The temporary destinations of this connection, each with the link that stands for it.
    private final Map<String, Long> temporaryQueues = new HashMap<>();
    private final Map<String, Long> temporaryTopics = new HashMap<>();
    private SenderLink relay;
    private SenderLink coordinator;
    // Whether a commit is attaching the coordinator link: the others wait for it.
    private boolean coordinatorAttaching;
    private long syncsAsked;
    private long syncsAnswered;

    /**
     * Connects and authenticates.
     *
     * @param clientId the client identifier of the connection factory, or {@code null}
     * @throws JMSSecurityException if the broker rejects the credentials
     * @throws InvalidClientIDException if another connection holds that client identifier
     */
    AmqpBrokerConnection(String host, int port, boolean encrypted, SSLContext tls, String clientId, String user,
            String password) throws JMSException {
        this.host = host;
        this.port = port;
        this.encrypted = encrypted;
        this.tls = tls;
        this.user = user;
        this.password = password;
        establish(clientId);
        this.clientId = clientId;
    }

    // ---- opening (part 2 §2.2 and §2.4, part 5 §5.3) --------------------------

    /**
     * Opens a socket and an AMQP connection on it, with one session and the link messages are
     * sent by. Called by the one thread that creates the connection or names it: nothing else
     * uses the connection yet.
     *
     * @param identifier the client identifier to hold, alone, or {@code null} for none
     */
    private void establish(String identifier) throws JMSException {
        Socket transport = new Socket();
        Socket socket = transport;
        Wire installed = null;
        boolean established = false;
        try {
            transport.connect(new InetSocketAddress(host, port), CONNECT_TIME_OUT);
            transport.setTcpNoDelay(true);
            transport.setSoTimeout(CONNECT_TIME_OUT);
            if (encrypted) {
                socket = secured(transport);
            }
            InputStream in = new BufferedInputStream(socket.getInputStream());
            OutputStream out = socket.getOutputStream();
            authenticate(in, out);

            // §2.4.1: what follows may be sent without waiting for the answers.
            ByteArrayOutputStream opening = new ByteArrayOutputStream();
            opening.writeBytes(ProtocolHeader.AMQP.octets());
            // A connection without a client identifier must not hold one: its container
            // identifier is its own, and it does not ask to be the only one that bears it.
            opening.writeBytes(FrameCodec.encode(Frame.amqp(0, new Open(
                    identifier != null ? identifier : "tassis-" + UUID.randomUUID(), host, MAX_FRAME_SIZE, 0,
                    (long) IDLE_TIME_OUT, null, null, null, identifier != null ? List.of(SOLE_CONNECTION) : null,
                    null)), FrameCodec.MIN_MAX_FRAME_SIZE));
            opening.writeBytes(FrameCodec.encode(
                    Frame.amqp(0, new Begin(null, 0L, WINDOW, WINDOW, HANDLE_MAX, null, null, null)),
                    FrameCodec.MIN_MAX_FRAME_SIZE));
            out.write(opening.toByteArray());
            out.flush();

            if (!readHeader(in).equals(ProtocolHeader.AMQP)) {
                throw new JMSException("the broker at " + host + ":" + port + " does not speak AMQP 1.0");
            }
            Open open = expect(in, Open.class);
            if (open.properties() != null && open.properties().containsKey(CONNECTION_ESTABLISHMENT_FAILED)) {
                // The open of a broker that is about to say why it refuses the connection.
                throw refusal(expect(in, Close.class).error());
            }
            Begin begin = expect(in, Begin.class);

            Wire opened = new Wire(socket, transport);
            long peerMax = open.maxFrameSize() == null ? MAX_FRAME_SIZE : open.maxFrameSize();
            opened.peerMaxFrameSize = Math.max(FrameCodec.MIN_MAX_FRAME_SIZE, Math.min(peerMax, Integer.MAX_VALUE));
            if (open.idleTimeOut() != null && open.idleTimeOut() > 0) {
                // Half of what the broker allows, which is what it asks for (§2.4.5).
                opened.heartbeat = (int) Math.max(1, Math.min(open.idleTimeOut() / 2, Integer.MAX_VALUE));
            }
            opened.echoes = open.offeredCapabilities() != null && open.offeredCapabilities().contains(FLOW_ECHO);
            if (open.offeredCapabilities() == null || !open.offeredCapabilities().contains(ANONYMOUS_RELAY)) {
                throw new JMSException("the broker at " + host + ":" + port
                        + " does not let a client send by a link without an address (ANONYMOUS-RELAY)");
            }
            transport.setSoTimeout(2 * IDLE_TIME_OUT);
            lock.lock();
            try {
                wire = opened;
                installed = opened;
                nextOutgoingId = 0;
                nextIncomingId = begin.nextOutgoingId() == null ? 0 : begin.nextOutgoingId();
                remoteIncomingWindow = begin.incomingWindow() == null ? WINDOW : begin.incomingWindow();
                nextDeliveryId = 0;
                nextHandle = 0;
                freeHandles.clear();
                attaching.clear();
                detaching.clear();
                senders.clear();
                receivers.clear();
                sent.clear();
                unsettled.clear();
                temporaryQueues.clear();
                temporaryTopics.clear();
                subscriptionLinks.clear();
                coordinator = null;
                coordinatorAttaching = false;
                String peer = host + ":" + port;
                opened.writer = Thread.ofVirtual().name("tassis-amqp-client-writer-" + peer)
                        .start(() -> write(opened, out));
                opened.reader = Thread.ofVirtual().name("tassis-amqp-client-reader-" + peer)
                        .start(() -> read(opened, in));
                relay = attachSender("tassis-relay", NO_TARGET);
            } finally {
                lock.unlock();
            }
            established = true;
        } catch (SSLException e) {
            if (aboutTheCertificate(e)) {
                // Not one the client trusts, or issued to another name than the one asked for.
                throw problem("the broker at " + host + ":" + port + " is not trusted: " + e.getMessage(), e);
            }
            throw problem("the broker at " + host + ":" + port
                    + " and the client could not make an encrypted connection (TLS): " + e.getMessage(), e);
        } catch (IOException e) {
            throw problem("the broker at " + host + ":" + port + " cannot be reached: " + e.getMessage(), e);
        } catch (AmqpDecodeException e) {
            throw problem("the broker at " + host + ":" + port + " does not speak AMQP 1.0: " + e.getMessage(), e);
        } finally {
            if (!established) {
                if (installed != null) {
                    // Not a connection that was lost: one that was never there.
                    lock.lock();
                    try {
                        installed.closing = true;
                    } finally {
                        lock.unlock();
                    }
                }
                closeQuietly(transport);
                closeQuietly(socket);
            }
        }
    }

    /** Whether a handshake failed because of the certificate the broker showed, rather than for want of TLS. */
    private static boolean aboutTheCertificate(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof java.security.cert.CertificateException) {
                return true;
            }
        }
        return false;
    }

    /**
     * The TLS handshake of {@code amqps}, from the first octet, on a socket that is connected:
     * the broker must show a certificate the client trusts and that was issued to the name the
     * client connects to (RFC 2818, as for HTTPS).
     *
     * @return the encrypted socket, layered on the one given, which stays the one to close in haste
     */
    private SSLSocket secured(Socket transport) throws IOException {
        SSLContext context;
        try {
            context = tls != null ? tls : SSLContext.getDefault();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new SSLException("TLS is not available", e);
        }
        // The name is that of the URL: it is what the certificate is checked against, and what
        // the handshake asks the server for, unless it is an address (RFC 6066, §3).
        SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket(transport, host, port, true);
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        socket.setSSLParameters(parameters);
        socket.startHandshake();
        return socket;
    }

    /** The security layer (part 5, §5.3.2): a user name and a password, or nobody in particular. */
    private void authenticate(InputStream in, OutputStream out) throws IOException, AmqpDecodeException, JMSException {
        out.write(ProtocolHeader.SASL.octets());
        out.flush();
        if (!readHeader(in).equals(ProtocolHeader.SASL)) {
            throw new JMSException("the broker at " + host + ":" + port + " does not speak AMQP 1.0 with SASL");
        }
        List<Symbol> offered = expect(in, SaslMechanisms.class).saslServerMechanisms();
        SaslInit init;
        if (user != null && offered.contains(PLAIN)) {
            // RFC 4616: [authorization identity] NUL authentication identity NUL password
            byte[] response = ("\u0000" + user + "\u0000" + (password == null ? "" : password))
                    .getBytes(StandardCharsets.UTF_8);
            init = new SaslInit(PLAIN, new Binary(response), host);
        } else if (user == null && offered.contains(ANONYMOUS)) {
            init = new SaslInit(ANONYMOUS, null, host);
        } else {
            throw new JMSSecurityException(user == null
                    ? "the broker accepts no connection without a user name and a password"
                    : "the broker offers no way to give it a user name and a password");
        }
        out.write(FrameCodec.encode(Frame.sasl(init), FrameCodec.MIN_MAX_FRAME_SIZE));
        out.flush();
        Integer code = expect(in, SaslOutcome.class).code();
        if (code == null || code != 0) {
            // The broker does not say which of the two is wrong, and neither does the client.
            throw new JMSSecurityException("the broker rejected the user name or the password");
        }
    }

    private static ProtocolHeader readHeader(InputStream in) throws IOException, AmqpDecodeException {
        byte[] octets = in.readNBytes(ProtocolHeader.LENGTH);
        if (octets.length < ProtocolHeader.LENGTH) {
            throw new EOFException("the broker closed the connection before saying what it speaks");
        }
        return ProtocolHeader.parse(octets);
    }

    private static Frame readFrame(InputStream in) throws IOException, AmqpDecodeException {
        byte[] start = in.readNBytes(4);
        if (start.length < 4) {
            throw new EOFException("the broker closed the connection");
        }
        int size = FrameCodec.size(ByteBuffer.wrap(start), MAX_FRAME_SIZE);
        byte[] frame = new byte[size];
        System.arraycopy(start, 0, frame, 0, 4);
        if (in.readNBytes(frame, 4, size - 4) < size - 4) {
            throw new EOFException("the broker closed the connection in the middle of a frame");
        }
        return FrameCodec.decode(ByteBuffer.wrap(frame), MAX_FRAME_SIZE);
    }

    /** The next frame that is not a heartbeat, which the opening of a connection expects to be of one type. */
    private static <T> T expect(InputStream in, Class<T> type) throws IOException, AmqpDecodeException, JMSException {
        Frame frame = readFrame(in);
        while (frame.isEmpty()) {
            frame = readFrame(in);
        }
        if (frame.body() instanceof Close close && type != Close.class) {
            throw refusal(close.error());
        }
        if (!type.isInstance(frame.body())) {
            throw new JMSException("the broker sent " + frame.body() + " where " + type.getSimpleName()
                    + " was expected");
        }
        return type.cast(frame.body());
    }

    /** Why a broker closed a connection it had not opened yet. */
    private static JMSException refusal(AmqpError error) {
        String reason = error == null || error.description() == null ? "the broker refused the connection"
                : error.description();
        if (error != null && UNAUTHORIZED_ACCESS.equals(error.condition())) {
            // A broker may let the security layer pass and refuse at the open: Artemis does, to
            // a client that gave no name.
            return new JMSSecurityException(reason);
        }
        // What a broker says of a container identifier that another connection holds (§6.1.2).
        return error != null && INVALID_FIELD.equals(error.condition()) ? new InvalidClientIDException(reason)
                : new JMSException(reason);
    }

    private static JMSException problem(String message, Exception cause) {
        JMSException problem = new JMSException(message);
        problem.setLinkedException(cause);
        problem.initCause(cause);
        return problem;
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "closing a socket", e);
        }
    }

    // ---- the writer and the reader --------------------------------------------

    private void write(Wire wire, OutputStream raw) {
        try {
            OutputStream out = new BufferedOutputStream(raw);
            while (true) {
                byte[] octets = wire.heartbeat > 0 ? wire.outbound.poll(wire.heartbeat, TimeUnit.MILLISECONDS)
                        : wire.outbound.take();
                if (octets == NO_MORE) {
                    out.flush();
                    return;
                }
                // Nothing to say for a while: an empty frame, so as not to be taken for dead (§2.4.5).
                out.write(octets == null ? FrameCodec.encode(Frame.EMPTY, FrameCodec.MIN_MAX_FRAME_SIZE) : octets);
                if (wire.outbound.isEmpty()) {
                    out.flush();
                }
            }
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "the broker stopped reading", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // Ends the reader, if it has not ended already: it is the reader that says so. A TLS
            // socket says goodbye, which nothing holds up since only this thread writes, and
            // must not then wait long for the broker to answer: TLS 1.3 would.
            try {
                wire.socket.setSoTimeout(GOODBYE_TIME_OUT);
            } catch (IOException e) {
                LOG.log(System.Logger.Level.DEBUG, "closing a socket", e);
            }
            closeQuietly(wire.socket);
            wire.cut();
        }
    }

    private void read(Wire wire, InputStream in) {
        String why = "the broker closed the connection";
        Exception cause = null;
        try {
            while (true) {
                Frame frame = readFrame(in);
                if (frame.isEmpty()) {
                    continue;
                }
                if (frame.body() instanceof Close close) {
                    if (close.error() != null) {
                        why = close.error().condition() + ": " + close.error().description();
                    }
                    answerClose(wire);
                    break;
                }
                handle(wire, frame);
            }
        } catch (IOException | AmqpDecodeException | RuntimeException e) {
            why = String.valueOf(e.getMessage());
            cause = e;
        } finally {
            ended(wire, why, cause);
        }
    }

    /** §2.4.3: a close of the broker's own is answered, before the socket is closed. */
    private void answerClose(Wire wire) {
        lock.lock();
        try {
            if (!wire.closing) {
                wire.outbound.add(FrameCodec.encode(Frame.amqp(0, new Close(null)), FrameCodec.MIN_MAX_FRAME_SIZE));
            }
        } finally {
            lock.unlock();
        }
    }

    /** The reader is done with a socket: whoever waits is told, and the application if it did not ask for it. */
    private void ended(Wire wire, String why, Exception cause) {
        JMSException problem = null;
        java.util.function.Consumer<JMSException> listener = null;
        lock.lock();
        try {
            wire.ended = true;
            if (wire == this.wire && !wire.closing && !closed) {
                problem = problem("the connection to the broker at " + host + ":" + port + " is lost: " + why, cause);
                lost = problem;
                listener = lostListener;
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        // The writer writes what is left, then closes the socket.
        wire.outbound.add(NO_MORE);
        if (problem != null && listener != null) {
            // On a thread of its own: the listener is the application's, and may take its time.
            JMSException told = problem;
            java.util.function.Consumer<JMSException> telling = listener;
            Thread.ofVirtual().name("tassis-amqp-client-lost").start(() -> telling.accept(told));
        }
    }

    /** Ends the conversation on the current socket in good order, and waits a while for the broker to agree. */
    private void disconnect() {
        Wire ending;
        lock.lock();
        try {
            ending = wire;
            if (ending == null) {
                return;
            }
            ending.closing = true;
            if (!ending.ended) {
                send(new Close(null));
                long remaining = TimeUnit.MILLISECONDS.toNanos(CLOSE_TIME_OUT);
                while (!ending.ended && remaining > 0) {
                    try {
                        remaining = changed.awaitNanos(remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        ending.outbound.add(NO_MORE);
        try {
            if (ending.writer != null && !ending.writer.join(java.time.Duration.ofMillis(CLOSE_TIME_OUT))) {
                ending.cut();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ending.cut();
    }

    private void queue(byte[] octets) {
        wire.outbound.add(octets);
    }

    private void send(Performative performative) {
        queue(FrameCodec.encode(Frame.amqp(0, performative), wire.peerMaxFrameSize));
    }

    // ---- what the broker says ---------------------------------------------------

    private void handle(Wire from, Frame frame) {
        lock.lock();
        try {
            if (from != wire || from.closing) {
                // What a connection that is being replaced or closed still says is of no use.
                return;
            }
            switch (frame.body()) {
                case Attach attach -> attached(attach);
                case Detach detach -> detached(detach);
                case Flow flow -> flow(flow);
                case Transfer transfer -> transfer(transfer, frame.payload());
                case Disposition disposition -> disposition(disposition);
                case End end -> throw new java.lang.IllegalStateException("the broker ended the session"
                        + (end.error() == null ? "" : ": " + end.error().description()));
                case null, default -> LOG.log(System.Logger.Level.DEBUG, "ignored: {0}", frame.body());
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void attached(Attach attach) {
        Reply reply = attaching.get(attach.handle());
        if (reply == null) {
            return;
        }
        reply.attach = attach;
        // §2.6.3: a link the broker refuses is attached with nothing at its end, then detached.
        // role: true when the broker is the receiving end of the link.
        reply.refused = Boolean.TRUE.equals(attach.role()) ? attach.target() == null : attach.source() == null;
        if (!reply.refused) {
            attaching.remove(attach.handle());
            reply.done = true;
        }
    }

    private void detached(Detach detach) {
        long handle = detach.handle();
        Reply refused = attaching.remove(handle);
        if (refused != null) {
            refused.error = detach.error();
            refused.refused = true;
            refused.done = true;
            // §2.6.5: the detach of the broker is answered.
            send(new Detach(handle, true, null));
            freeHandles.add(handle);
            return;
        }
        Reply asked = detaching.remove(handle);
        if (asked != null) {
            asked.error = detach.error();
            asked.done = true;
            freeHandles.add(handle);
            return;
        }
        // The broker ends a link of its own accord: its consumer has nothing more to wait for.
        ReceiverLink receiver = receivers.remove(handle);
        if (receiver != null) {
            receiver.detached = true;
            receiver.draining = false;
            receiver.credit = 0;
            receiver.unsettled = 0;
            unsettled.values().removeIf(delivery -> delivery.link == receiver);
        }
        SenderLink sender = senders.remove(handle);
        if (sender != null) {
            sender.detached = true;
        }
        send(new Detach(handle, true, null));
        freeHandles.add(handle);
    }

    private void flow(Flow flow) {
        // What the broker still accepts, counted from the transfer the client sends next (§2.5.6).
        long brokerNextIncomingId = flow.nextIncomingId() == null ? 0 : flow.nextIncomingId();
        long window = flow.incomingWindow() == null ? WINDOW : flow.incomingWindow();
        remoteIncomingWindow = Math.max(0, brokerNextIncomingId + window - nextOutgoingId);
        if (flow.handle() == null) {
            // Only ever the echo of a flow the client sent to know the broker has read so far.
            syncsAnswered++;
            return;
        }
        SenderLink sender = senders.get(flow.handle());
        if (sender != null) {
            long brokerDeliveryCount = flow.deliveryCount() == null ? sender.deliveryCount : flow.deliveryCount();
            long credit = flow.linkCredit() == null ? 0 : flow.linkCredit();
            sender.credit = Math.max(0, brokerDeliveryCount + credit - sender.deliveryCount);
            return;
        }
        ReceiverLink receiver = receivers.get(flow.handle());
        if (receiver != null && receiver.draining && Boolean.TRUE.equals(flow.drain())
                && flow.deliveryCount() != null && flow.deliveryCount() == receiver.deliveryCount + receiver.credit) {
            // §2.6.7: the broker had nothing more for the credit, and has given it up. An answer
            // that does not account for all the credit is that of an earlier drain: ignored.
            receiver.deliveryCount += receiver.credit;
            receiver.credit = 0;
            receiver.draining = false;
        }
    }

    private void transfer(Transfer transfer, Binary payload) {
        nextIncomingId++;
        ReceiverLink link = receivers.get(transfer.handle());
        if (link == null) {
            return;
        }
        if (link.partial == null) {
            link.partial = new ByteArrayOutputStream();
            link.partialId = transfer.deliveryId() == null ? -1 : transfer.deliveryId();
            link.partialSettled = Boolean.TRUE.equals(transfer.settled());
        }
        link.partial.writeBytes(payload.bytes());
        if (Boolean.TRUE.equals(transfer.more())) {
            return;
        }
        byte[] octets = link.partial.toByteArray();
        long id = link.partialId;
        boolean settled = link.partialSettled || Boolean.TRUE.equals(transfer.settled());
        link.partial = null;
        if (Boolean.TRUE.equals(transfer.aborted())) {
            return;
        }
        link.deliveryCount++;
        if (link.credit > 0) {
            link.credit--;
        }
        if (link.credit == 0) {
            // All the credit is used: there is nothing left for a drain to give up.
            link.draining = false;
        }
        try {
            TassisMessage message = JmsMessageCodec.toJms(AmqpMessage.decode(octets));
            int deliveryCount = message.propertyExists("JMSXDeliveryCount") ? message.getIntProperty("JMSXDeliveryCount") : 1;
            if (link.browsing) {
                message.markReceived();
            } else {
                message.markDelivered(deliveryCount);
            }
            AmqpDelivery delivery = new AmqpDelivery(link, id, message, deliveryCount);
            link.arrived.add(delivery);
            if (!settled) {
                unsettled.put(id, delivery);
                link.unsettled++;
            }
        } catch (AmqpDecodeException | JMSException e) {
            LOG.log(System.Logger.Level.ERROR, "a message of the broker could not be read: it is rejected", e);
            if (!settled) {
                send(new Disposition(true, id, id, true,
                        new Rejected(new AmqpError(Symbol.of("amqp:decode-error"), e.getMessage(), null)), null));
            }
        }
    }

    private void disposition(Disposition disposition) {
        if (!Boolean.TRUE.equals(disposition.role())) {
            // From the sending end: about messages the client received, which it settles itself.
            return;
        }
        long last = disposition.last() == null ? disposition.first() : disposition.last();
        for (Sent waiting : sent.subMap(disposition.first(), true, last, true).values()) {
            waiting.state = disposition.state();
            waiting.done = true;
        }
    }

    // ---- waiting for the broker --------------------------------------------------

    /** Fails if the connection can no longer be used. Under the lock. */
    private void checkUsable() throws JMSException {
        if (lost != null) {
            throw problem(lost.getMessage(), lost);
        }
        if (closed) {
            throw new IllegalStateException("the connection is closed");
        }
    }

    /** Waits until the broker has said what is waited for. Under the lock, which the wait releases. */
    private void await(BooleanSupplier said) throws JMSException {
        while (!said.getAsBoolean()) {
            checkUsable();
            if (wire.ended) {
                throw new IllegalStateException("the connection is closed");
            }
            changed.awaitUninterruptibly();
        }
    }

    /** Like {@link #await}, for what must not fail: returns false if the connection is no longer there. */
    private boolean awaitQuietly(BooleanSupplier said) {
        while (!said.getAsBoolean()) {
            if (lost != null || wire.ended || closed) {
                return false;
            }
            changed.awaitUninterruptibly();
        }
        return true;
    }

    /**
     * Returns once the broker has read every frame queued so far, and so has done what they
     * ask: it answers an echo in the order it reads (§2.5.6). Under the lock.
     *
     * <p>Only with a broker that says it answers ({@code Conventions.FLOW_ECHO}). With another,
     * this returns at once: what was asked is done a moment later, which only another
     * connection could notice.
     */
    private void sync() {
        if (!wire.echoes) {
            // A broker that does not say it answers: what it was told is on its way, in order,
            // and the client does not know when it is done.
            return;
        }
        long asked = ++syncsAsked;
        send(new Flow(nextIncomingId, WINDOW, nextOutgoingId, WINDOW, null, null, null, null, null, true, null));
        awaitQuietly(() -> syncsAnswered >= asked);
    }

    private long allocateHandle() throws JMSException {
        Long free = freeHandles.pollFirst();
        if (free != null) {
            return free;
        }
        if (nextHandle > HANDLE_MAX) {
            throw new JMSException("too many consumers, browsers and temporary destinations on one connection");
        }
        return nextHandle++;
    }

    /** Attaches a link and returns the attach of the broker. Under the lock. */
    private Attach attach(Attach attach) throws JMSException {
        checkUsable();
        Reply reply = new Reply();
        attaching.put(attach.handle(), reply);
        send(attach);
        try {
            await(() -> reply.done);
        } finally {
            attaching.remove(attach.handle(), reply);
        }
        if (reply.refused) {
            throw failure(reply.error);
        }
        return reply.attach;
    }

    /** Detaches a link and returns what the broker has against it, or {@code null}. Under the lock. */
    private AmqpError detach(long handle, boolean closing) {
        if (lost != null || wire.ended) {
            return null;
        }
        Reply reply = new Reply();
        detaching.put(handle, reply);
        send(new Detach(handle, closing, null));
        awaitQuietly(() -> reply.done);
        return reply.error;
    }

    private SenderLink attachSender(String name, Object target) throws JMSException {
        SenderLink link = new SenderLink(allocateHandle());
        senders.put(link.handle, link);
        try {
            attach(new Attach(name, link.handle, false, null, null, NO_SOURCE, target, null, null, 0L, null, null,
                    null, null));
        } catch (JMSException e) {
            senders.remove(link.handle, link);
            throw e;
        }
        return link;
    }

    /** What a broker has against a request, as the exception Jakarta Messaging has for it. */
    private static JMSException failure(AmqpError error) {
        String reason = error == null || error.description() == null ? "the broker refused, and gave no reason"
                : error.description();
        if (error != null && UNAUTHORIZED_ACCESS.equals(error.condition())) {
            return new JMSSecurityException(reason);
        }
        return error != null && NOT_FOUND.equals(error.condition()) ? new InvalidDestinationException(reason)
                : new JMSException(reason);
    }

    // ---- identity, start and stop ------------------------------------------------

    @Override
    public String nextMessageId() {
        return messageIdPrefix + messageIds.incrementAndGet();
    }

    /**
     * An AMQP connection says who it is when it opens, and a client identifier may be set after
     * the connection was created (§6.1.2): the connection opened without one is closed, and
     * another is opened in its place. Nothing has used it yet, which is what §6.1.2 requires.
     */
    @Override
    public void claimClientId(String identifier) throws JMSException {
        lock.lock();
        try {
            checkUsable();
            if (identifier.equals(clientId)) {
                // The one the connection was opened with, by the settings of its factory.
                return;
            }
            if (clientId != null) {
                throw new IllegalStateException("the client identifier is already set to " + clientId);
            }
        } finally {
            lock.unlock();
        }
        disconnect();
        try {
            establish(identifier);
        } catch (JMSException refused) {
            // The connection goes on without a client identifier, as it was.
            establish(null);
            throw refused;
        }
        lock.lock();
        try {
            clientId = identifier;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean start() {
        lock.lock();
        try {
            if (started || closed || lost != null) {
                return false;
            }
            started = true;
            changed.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The broker is asked for no more messages, and those it had already sent for the credit
     * of a consumer are given back: no consumer hands one out before the connection starts again.
     */
    @Override
    public void stop() {
        lock.lock();
        try {
            if (!started) {
                return;
            }
            started = false;
            quiet(link -> !link.browsing);
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    /**
     * Takes back the credit of some consumers, all at once, and gives back the messages that
     * came for it and that no application has seen. Under the lock.
     */
    private void quiet(Predicate<ReceiverLink> which) {
        List<ReceiverLink> links = new ArrayList<>();
        for (ReceiverLink link : receivers.values()) {
            if (which.test(link) && !link.detached) {
                links.add(link);
                link.drain();
            }
        }
        for (ReceiverLink link : links) {
            awaitQuietly(() -> !link.draining || link.detached);
        }
        boolean released = false;
        for (ReceiverLink link : links) {
            released |= link.release();
        }
        if (released) {
            sync();
        }
    }

    // ---- temporary destinations (§6.2.2) -------------------------------------------

    @Override
    public String createTemporaryQueue() throws JMSException {
        return createTemporary(false);
    }

    @Override
    public String createTemporaryTopic() throws JMSException {
        return createTemporary(true);
    }

    /** A sending link with a dynamic target: the broker creates the node, and deletes it with the link. */
    private String createTemporary(boolean topic) throws JMSException {
        lock.lock();
        try {
            checkUsable();
            long handle = allocateHandle();
            Attach answer = attach(new Attach("tassis-temporary-" + names++, handle, false, null, null, NO_SOURCE,
                    new Target(null, null, null, null, true, null,
                            List.of(topic ? CAPABILITY_TEMPORARY_TOPIC : CAPABILITY_TEMPORARY_QUEUE)),
                    null, null, 0L, null, null, null, null));
            if (!(answer.target() instanceof Target target) || !(target.address() instanceof String name)) {
                throw new JMSException("the broker created a temporary destination and did not say its name");
            }
            (topic ? temporaryTopics : temporaryQueues).put(name, handle);
            return name;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean hasTemporaryQueue(String queue) {
        lock.lock();
        try {
            return temporaryQueues.containsKey(queue);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean hasTemporaryTopic(String topic) {
        lock.lock();
        try {
            return temporaryTopics.containsKey(topic);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deleteTemporaryQueue(String queue) throws JMSException {
        deleteTemporary(queue, false);
    }

    @Override
    public void deleteTemporaryTopic(String topic) throws JMSException {
        deleteTemporary(topic, true);
    }

    private void deleteTemporary(String name, boolean topic) throws JMSException {
        lock.lock();
        try {
            checkUsable();
            Map<String, Long> temporaries = topic ? temporaryTopics : temporaryQueues;
            Long handle = temporaries.get(name);
            if (handle == null) {
                throw new InvalidDestinationException("temporary destination " + name + " has been deleted");
            }
            // Only this connection may consume from it (§6.2.2), so the client knows: asking the
            // broker would cost the link that stands for the destination.
            for (ReceiverLink link : receivers.values()) {
                if (name.equals(link.address) && !link.closed) {
                    throw new JMSException(topic ? "topic " + name + " still has subscribers"
                            : "queue " + name + " still has consumers");
                }
            }
            detachDeferred(link -> name.equals(link.address));
            temporaries.remove(name);
            AmqpError error = detach(handle, true);
            if (error != null) {
                throw failure(error);
            }
        } finally {
            lock.unlock();
        }
    }

    // ---- sending -------------------------------------------------------------------

    @Override
    public void send(Outgoing message) throws JMSException {
        byte[] octets = encode(message);
        lock.lock();
        try {
            check(transfer(relay, octets, null));
        } finally {
            lock.unlock();
        }
    }

    private static byte[] encode(Outgoing outgoing) throws JMSException {
        TassisMessage message = outgoing.message();
        if (message.getJMSDestination() == null) {
            message.setJMSDestination(outgoing.topic() ? new TassisTopic(outgoing.destination())
                    : new TassisQueue(outgoing.destination()));
        }
        return JmsMessageCodec.toAmqp(message).encode();
    }

    /**
     * Sends a message by a link and waits for the broker to say what became of it. Under the
     * lock.
     *
     * @param state the transaction the message is sent in, or {@code null}
     * @return the outcome the broker gave
     */
    private Object transfer(SenderLink link, byte[] octets, Object state) throws JMSException {
        await(() -> link.detached || link.credit > 0 && remoteIncomingWindow > 0);
        if (link.detached) {
            throw new JMSException("the broker ended the link messages are sent by");
        }
        long id = nextDeliveryId++;
        link.credit--;
        link.deliveryCount++;
        Transfer first = new Transfer(link.handle, id, new Binary(AmqpEncoder.encode(id)), 0L, false, null, null,
                state, null, null, null);
        Sent waiting = new Sent();
        sent.put(id, waiting);
        try {
            for (byte[] frame : Transfers.split(0, first, octets, wire.peerMaxFrameSize)) {
                nextOutgoingId++;
                remoteIncomingWindow--;
                queue(frame);
            }
            await(() -> waiting.done || link.detached);
        } finally {
            sent.remove(id);
        }
        if (!waiting.done) {
            throw new JMSException("the broker ended the link messages are sent by, and did not say what"
                    + " became of the message");
        }
        return waiting.state;
    }

    /** Fails unless the broker accepted what it was sent. */
    private static void check(Object state) throws JMSException {
        Object outcome = state instanceof TransactionalState transactional ? transactional.outcome() : state;
        switch (outcome) {
            case Accepted accepted -> {
            }
            case Rejected rejected -> throw failure(rejected.error());
            case null, default -> throw new JMSException("the broker did not accept the message: " + outcome);
        }
    }

    /**
     * Part 4 of AMQP: a transaction is declared, the messages are sent and the deliveries
     * accepted in it, and it is discharged. The broker does all of it at the discharge, or none.
     */
    @Override
    public void commit(Collection<Outgoing> sends, Collection<? extends Delivery> consumed) throws JMSException {
        if (sends.isEmpty()) {
            // Nothing that could fail: settling needs no transaction.
            lock.lock();
            try {
                checkUsable();
            } finally {
                lock.unlock();
            }
            settle(consumed);
            return;
        }
        List<byte[]> messages = new ArrayList<>();
        for (Outgoing outgoing : sends) {
            messages.add(encode(outgoing));
        }
        lock.lock();
        try {
            checkUsable();
            if (messages.size() == 1 && consumed.isEmpty()) {
                // One message is all or nothing by itself.
                check(transfer(relay, messages.get(0), null));
                return;
            }
            // Attaching waits for the broker, and so lets other commits in: only one of them attaches.
            await(() -> !coordinatorAttaching);
            if (coordinator == null) {
                coordinatorAttaching = true;
                try {
                    coordinator = attachSender("tassis-coordinator", new Coordinator(List.of(LOCAL_TRANSACTIONS)));
                } finally {
                    coordinatorAttaching = false;
                    changed.signalAll();
                }
            }
            Object declared = transfer(coordinator, AmqpMessage.ofBody(new AmqpValue(new Declare(null))).encode(), null);
            if (!(declared instanceof Declared(Binary transaction))) {
                check(declared);
                throw new JMSException("the broker did not declare a transaction: " + declared);
            }
            JMSException problem = null;
            try {
                for (byte[] message : messages) {
                    check(transfer(relay, message, new TransactionalState(transaction, null)));
                }
            } catch (JMSException e) {
                problem = e;
            }
            List<AmqpDelivery> accepted = new ArrayList<>();
            if (problem == null) {
                for (Delivery delivery : consumed) {
                    if (delivery instanceof AmqpDelivery ours && unsettled.get(ours.id) == ours) {
                        send(new Disposition(true, ours.id, ours.id, true,
                                new TransactionalState(transaction, new Accepted()), null));
                        accepted.add(ours);
                    }
                }
            }
            Object outcome = transfer(coordinator,
                    AmqpMessage.ofBody(new AmqpValue(new Discharge(transaction, problem != null))).encode(), null);
            // Committed or rolled back, the broker has taken them back: they are no longer the client's.
            for (AmqpDelivery delivery : accepted) {
                forget(delivery);
            }
            if (problem != null) {
                throw problem;
            }
            check(outcome);
        } finally {
            lock.unlock();
        }
    }

    // ---- consuming ------------------------------------------------------------------

    private static Map<Symbol, Object> filter(Selector selector, boolean noLocal) {
        if (selector == null && !noLocal) {
            return null;
        }
        Map<Symbol, Object> filter = new LinkedHashMap<>();
        if (selector != null) {
            filter.put(FILTER_SELECTOR, new Described(SELECTOR_DESCRIPTOR, selector.text()));
        }
        if (noLocal) {
            filter.put(FILTER_NO_LOCAL, new Described(NO_LOCAL_DESCRIPTOR, "NoLocalFilter{}"));
        }
        return filter;
    }

    private ReceiverLink attachReceiver(String name, Source source, String address, String subscription,
            boolean durable, boolean browsing) throws JMSException {
        ReceiverLink link = new ReceiverLink(allocateHandle(), address, subscription, durable, browsing);
        receivers.put(link.handle, link);
        try {
            Attach answer = attach(new Attach(name, link.handle, true, null, null, source, NO_TARGET, null, null, null,
                    null, null, null, null));
            link.deliveryCount = answer.initialDeliveryCount() == null ? 0 : answer.initialDeliveryCount();
            return link;
        } catch (JMSException e) {
            receivers.remove(link.handle, link);
            throw e;
        }
    }

    @Override
    public BrokerConnection.Consumer attach(String queue, boolean temporary, Selector selector) throws JMSException {
        lock.lock();
        try {
            checkUsable();
            return attachReceiver("tassis-consumer-" + names++,
                    new Source(queue, null, null, null, null, null, null, filter(selector, false), null, null,
                            List.of(temporary ? CAPABILITY_TEMPORARY_QUEUE : CAPABILITY_QUEUE)),
                    queue, null, false, false);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public BrokerConnection.Consumer subscribe(Subscribing request) throws JMSException {
        lock.lock();
        try {
            checkUsable();
            // A connection without a client identifier has no names of its own: those of its
            // subscriptions are the broker's, which a broker must be told (it sees a container
            // identifier on every connection).
            boolean global = request.global() || clientId == null;
            String name = request.name() == null ? "tassis-subscriber-" + names++
                    : subscriptionLinkName(request.name(), request.durable(), request.shared(), global);
            if (request.name() != null) {
                // A consumer the application closed may still hold the subscription: see ReceiverLink.close.
                detachDeferred(link -> request.name().equals(link.subscription));
            }
            List<Symbol> capabilities = new ArrayList<>();
            capabilities.add(request.temporary() ? CAPABILITY_TEMPORARY_TOPIC : CAPABILITY_TOPIC);
            if (request.shared()) {
                capabilities.add(CAPABILITY_SHARED);
            }
            if (global && request.name() != null) {
                capabilities.add(CAPABILITY_GLOBAL);
            }
            return attachReceiver(name,
                    new Source(request.topic(), request.durable() ? 2L : null, request.durable() ? EXPIRY_NEVER : null,
                            null, null, null, null, filter(request.selector(), request.noLocal()), null, null,
                            capabilities),
                    request.topic(), request.name(), request.durable(), false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The name of the link of a subscription, which is how a broker knows the subscription: its
     * name, then what tells apart the links one connection has to it, as Apache Qpid JMS writes
     * them and brokers read them. {@code sub}, {@code sub|2} for a durable one;
     * {@code sub|volatile1} for one that is not; {@code sub|global}, {@code sub|global2} and
     * {@code sub|global-volatile1} when the name is the broker's. Under the lock.
     */
    private String subscriptionLinkName(String subscription, boolean durable, boolean shared, boolean global) {
        if (!shared) {
            return global ? subscription + SUBSCRIPTION_NAME_END + GLOBAL_SUFFIX : subscription;
        }
        int link = subscriptionLinks.merge(subscription, 1, Integer::sum);
        if (!durable) {
            return subscription + SUBSCRIPTION_NAME_END + (global ? GLOBAL_SUFFIX + "-" : "") + "volatile" + link;
        }
        if (global) {
            return subscription + SUBSCRIPTION_NAME_END + GLOBAL_SUFFIX + (link > 1 ? String.valueOf(link) : "");
        }
        return link > 1 ? subscription + SUBSCRIPTION_NAME_END + link : subscription;
    }

    /**
     * Attaches the link by which a durable subscription is looked up: a receiving link that
     * names it and says nothing of what it reads. Under the lock.
     *
     * @throws InvalidDestinationException if there is no such subscription
     */
    private Attach lookUp(String name, boolean asked) throws JMSException {
        boolean global = asked || clientId == null;
        return attach(new Attach(global ? name + SUBSCRIPTION_NAME_END + GLOBAL_SUFFIX : name, allocateHandle(), true,
                null, null, null, NO_TARGET, null, null, null, null, null, global ? List.of(CAPABILITY_SHARED, CAPABILITY_GLOBAL) : List.of(CAPABILITY_SHARED),
                null));
    }

    @Override
    public String durableSubscriptionTopic(String name, boolean global) {
        lock.lock();
        try {
            checkUsable();
            Attach found = lookUp(name, global);
            detach(found.handle(), false);
            return found.source() instanceof Source source && source.address() instanceof String topic ? topic : null;
        } catch (JMSException none) {
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unsubscribe(String name, boolean global) throws JMSException {
        lock.lock();
        try {
            checkUsable();
            detachDeferred(link -> name.equals(link.subscription));
            Attach found = lookUp(name, global);
            // Closed, not merely detached: the subscription goes with the link.
            AmqpError error = detach(found.handle(), true);
            if (error != null) {
                throw failure(error);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<TassisMessage> browse(String queue, Selector selector) throws JMSException {
        lock.lock();
        try {
            checkUsable();
            ReceiverLink link;
            try {
                link = attachReceiver("tassis-browser-" + names++, new Source(queue, null, null, null, null, null,
                        DISTRIBUTION_COPY, filter(selector, false), null, null, null), queue, null, false, true);
            } catch (InvalidDestinationException gone) {
                return List.of();
            }
            List<TassisMessage> messages = new ArrayList<>();
            long got;
            do {
                // Credit and drain at once: the broker sends what it has, and says when that is all.
                link.credit = BROWSE_CREDIT;
                link.draining = true;
                link.flow(true);
                await(() -> !link.draining || link.detached);
                got = link.arrived.size();
                for (AmqpDelivery delivery : link.arrived) {
                    messages.add(delivery.message());
                }
                link.arrived.clear();
            } while (got == BROWSE_CREDIT);
            link.detachNow();
            return messages;
        } finally {
            lock.unlock();
        }
    }

    /** Detaches the links whose consumer is closed and that only waited for their deliveries to be settled. */
    private void detachDeferred(Predicate<ReceiverLink> which) {
        for (ReceiverLink link : new ArrayList<>(receivers.values())) {
            if (link.closed && !link.detached && which.test(link)) {
                link.detachNow();
            }
        }
    }

    // ---- settling ---------------------------------------------------------------------

    /**
     * A delivery is no longer the client's to settle. Under the lock.
     *
     * @return false if it was not the client's already
     */
    private boolean forget(AmqpDelivery delivery) {
        if (!unsettled.remove(delivery.id, delivery)) {
            return false;
        }
        ReceiverLink link = delivery.link;
        link.unsettled--;
        if (link.closed && !link.detached && link.unsettled == 0 && link.waiting == 0) {
            // The consumer was closed with deliveries its session still answered for: now it can go.
            link.detachNow();
        }
        return true;
    }

    /** Says what became of a delivery, if it is still the client's. Under the lock. */
    private boolean dispose(Delivery delivery, Object state) {
        if (!(delivery instanceof AmqpDelivery ours) || lost != null || wire.ended
                || unsettled.get(ours.id) != ours) {
            return false;
        }
        send(new Disposition(true, ours.id, ours.id, true, state, null));
        return forget(ours);
    }

    @Override
    public boolean settle(Delivery delivery) {
        lock.lock();
        try {
            return dispose(delivery, new Accepted());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void settle(Collection<? extends Delivery> deliveries) {
        lock.lock();
        try {
            for (Delivery delivery : deliveries) {
                dispose(delivery, new Accepted());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void redeliver(Collection<? extends Delivery> deliveries) {
        lock.lock();
        try {
            boolean any = false;
            for (Delivery delivery : deliveries) {
                // The application has seen it: its next delivery is a redelivery.
                any |= dispose(delivery, new Modified(true, null, null));
            }
            if (any) {
                // Back in the broker before this returns, as with the broker of the JVM.
                sync();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void returnUndelivered(Delivery delivery) {
        lock.lock();
        try {
            if (dispose(delivery, new Released())) {
                sync();
            }
        } finally {
            lock.unlock();
        }
    }

    // ---- the end ------------------------------------------------------------------------

    @Override
    public boolean isLost() {
        lock.lock();
        try {
            return lost != null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onLost(java.util.function.Consumer<JMSException> listener) {
        JMSException already;
        lock.lock();
        try {
            lostListener = listener;
            already = lost;
        } finally {
            lock.unlock();
        }
        if (already != null && listener != null) {
            // Lost before anybody asked to be told.
            Thread.ofVirtual().name("tassis-amqp-client-lost").start(() -> listener.accept(already));
        }
    }

    /** The broker takes back whatever the connection still held when it closes (§2.4.3). */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            started = false;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        disconnect();
    }

    // ---- a consumer -------------------------------------------------------------------

    /**
     * The link of one consumer. It has one credit at most, granted when the application asks
     * for a message: what the application has not asked for stays with the broker.
     *
     * <p>Guarded by the lock of the connection.
     */
    final class ReceiverLink implements BrokerConnection.Consumer {

        final long handle;
        /** The queue or the topic the link reads. */
        final String address;
        /** The name of the subscription the link reads, or {@code null}. */
        final String subscription;
        final boolean durable;
        final boolean browsing;

        long credit;
        long deliveryCount;
        /** Whether the broker was asked to use the credit now or give it up, and has not answered. */
        boolean draining;
        final Deque<AmqpDelivery> arrived = new ArrayDeque<>();
        ByteArrayOutputStream partial;
        long partialId;
        boolean partialSettled;
        /** The deliveries of this link the client has neither settled nor given back. */
        int unsettled;
        /** How many threads are in receive or take. */
        int waiting;
        boolean listening;
        long period;
        /** Closed by the application. */
        boolean closed;
        /** No longer attached: closed and done with, or ended by the broker. */
        boolean detached;
        /** Whether a close is still giving back what the link held. */
        boolean closing;

        ReceiverLink(long handle, String address, String subscription, boolean durable, boolean browsing) {
            this.handle = handle;
            this.address = address;
            this.subscription = subscription;
            this.durable = durable;
            this.browsing = browsing;
        }

        void flow(boolean drain) {
            send(new Flow(nextIncomingId, WINDOW, nextOutgoingId, WINDOW, handle, deliveryCount, credit, null,
                    drain ? Boolean.TRUE : null, null, null));
        }

        /** Asks the broker for one message, unless it was asked already. */
        private void grant() {
            if (credit == 0 && !draining) {
                credit = 1;
                flow(false);
            }
        }

        /** Asks the broker to use the credit now or give it up (§2.6.7); the answer ends {@code draining}. */
        void drain() {
            if (credit > 0 && !draining) {
                draining = true;
                flow(true);
            }
        }

        /**
         * Gives back the messages that came and that no application has seen, as if they had
         * not been delivered.
         *
         * @return whether there were any
         */
        boolean release() {
            boolean any = false;
            // A copy: giving back the last delivery of a closed link detaches it, which waits.
            List<AmqpDelivery> unseen = new ArrayList<>(arrived);
            arrived.clear();
            for (AmqpDelivery delivery : unseen) {
                any |= dispose(delivery, new Released());
            }
            return any;
        }

        /**
         * Waits for a message.
         *
         * @param deadline when to give up, as {@link System#nanoTime} counts, or {@code null} for never
         * @param awaited the listening period the message is for, or a negative number for a receive
         */
        private AmqpDelivery next(Long deadline, long awaited) throws JMSException {
            while (true) {
                if (closed || detached || AmqpBrokerConnection.this.closed) {
                    return null;
                }
                checkUsable();
                if (awaited >= 0 && (!listening || period != awaited)) {
                    // The listener this was for is gone: what came for it goes back.
                    drain();
                    awaitQuietly(() -> !draining || detached);
                    if (release()) {
                        sync();
                    }
                    return null;
                }
                if (started && !arrived.isEmpty()) {
                    return arrived.poll();
                }
                if (started) {
                    grant();
                }
                try {
                    if (deadline == null) {
                        changed.await();
                        continue;
                    }
                    long remaining = deadline - System.nanoTime();
                    if (remaining > 0) {
                        changed.awaitNanos(remaining);
                        continue;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new JMSException("interrupted while waiting for a message");
                }
                // Out of time. The broker is asked to send what it has now or to say it has
                // nothing: a message that was on its way is not left behind.
                drain();
                awaitQuietly(() -> !draining || detached);
                checkUsable();
                return started && !closed && !detached ? arrived.poll() : null;
            }
        }

        @Override
        public Delivery receive(long timeoutNanos, boolean settleOnDelivery) throws JMSException {
            Long deadline = timeoutNanos == FOREVER ? null : System.nanoTime() + Math.max(0, timeoutNanos);
            lock.lock();
            try {
                AmqpDelivery delivery;
                waiting++;
                try {
                    delivery = next(deadline, -1);
                } finally {
                    waiting--;
                    changed.signalAll();
                }
                if (delivery != null && settleOnDelivery) {
                    dispose(delivery, new Accepted());
                }
                return delivery;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Delivery take(long awaited) throws JMSException {
            lock.lock();
            try {
                waiting++;
                try {
                    return next(null, awaited);
                } finally {
                    waiting--;
                    changed.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public long listen(boolean nowListening) {
            lock.lock();
            try {
                if (nowListening) {
                    listening = true;
                } else if (listening) {
                    listening = false;
                    period++;
                    changed.signalAll();
                }
                return period;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Closes the consumer. Its link is detached at once unless the session still answers
         * for deliveries made by it: a broker gives back the unsettled deliveries of a link that
         * goes, and the session may yet acknowledge or commit them (§6.2.10). The link then
         * stays, without credit, until the last of them is settled or given back.
         */
        @Override
        public void close() {
            lock.lock();
            try {
                if (closed) {
                    // Closed by another thread, which may not be done: a close returns to a quiet consumer.
                    while (closing) {
                        changed.awaitUninterruptibly();
                    }
                    return;
                }
                closed = true;
                closing = true;
                changed.signalAll();
                try {
                    while (waiting > 0) {
                        changed.awaitUninterruptibly();
                    }
                    if (detached || lost != null || wire.ended) {
                        return;
                    }
                    drain();
                    awaitQuietly(() -> !draining || detached);
                    // Giving back the last delivery of a closed link detaches it.
                    if (release()) {
                        sync();
                    }
                    if (!detached && unsettled == 0) {
                        detachNow();
                    }
                } finally {
                    closing = false;
                    changed.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        /** Detaches the link, with whatever it still held: the broker gives that back. */
        void detachNow() {
            if (detached) {
                return;
            }
            detached = true;
            receivers.remove(handle, this);
            AmqpBrokerConnection.this.unsettled.values().removeIf(delivery -> delivery.link == this);
            unsettled = 0;
            // A durable subscription outlives its link: detached, not closed (§2.6.6).
            detach(handle, !durable);
            changed.signalAll();
        }
    }
}
