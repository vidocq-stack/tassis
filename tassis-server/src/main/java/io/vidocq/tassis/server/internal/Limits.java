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

/**
 * What the server announces, what it accepts, and the defaults AMQP 1.0 gives the fields a
 * peer leaves out: in one place, so that each is decided once.
 */
final class Limits {

    /** The largest frame the server accepts, announced in its open (part 2, §2.7.1). */
    static final long MAX_FRAME_SIZE = 256 * 1024;

    /** The largest frame of the security layer, which comes before any negotiation (§2.7.1). */
    static final long MAX_SASL_FRAME_SIZE = 512;

    /** What a peer accepts if its open does not say: any size a frame can have (§2.7.1). */
    static final long DEFAULT_MAX_FRAME_SIZE = 0xFFFF_FFFFL;

    /** The highest channel number the server accepts, hence the sessions of a connection. */
    static final int CHANNEL_MAX = 1023;

    /** The highest link handle the server accepts on a session. */
    static final long HANDLE_MAX = 65_535;

    /**
     * What the server announces as its idle time-out, in milliseconds: a peer should send a
     * frame at least that often. The connection is given up after twice as long (§2.4.5).
     */
    static final int IDLE_TIME_OUT = 60_000;

    /** How long a client has to authenticate and open the connection, in milliseconds. */
    static final int HANDSHAKE_TIME_OUT = 10_000;

    /** How long the server waits for a client to read the last frames it is sent, in milliseconds. */
    static final int LAST_WORDS_TIME_OUT = 5_000;

    /** How long the closing of an encrypted socket waits for the client to say goodbye too, in milliseconds. */
    static final int GOODBYE_TIME_OUT = 200;

    /** The session window the server grants: how many transfer frames it accepts (§2.5.6). */
    static final long INCOMING_WINDOW = Integer.MAX_VALUE;

    /** How many messages a producer may send before the server grants more credit (§2.6.7). */
    static final long PRODUCER_CREDIT = 100;

    /** The largest message the server puts back together from its transfer frames. */
    static final long MAX_MESSAGE_SIZE = 16L * 1024 * 1024;

    /** How many octets may wait to be written to one client before the server gives it up. */
    static final long MAX_PENDING_OCTETS = 64L * 1024 * 1024;

    /** How many transactions a connection may have declared and not discharged (part 4, §4.3). */
    static final int MAX_TRANSACTIONS = 1024;

    /** How many octets of messages a transaction may hold before it is discharged. */
    static final long MAX_TRANSACTION_OCTETS = 64L * 1024 * 1024;

    private Limits() {
    }
}
