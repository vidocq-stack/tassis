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

import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A session of a client (part 2, §2.5): its links, its transfer windows and the deliveries it
 * has sent that are not settled yet. All of it is guarded by the lock of its connection.
 */
final class AmqpSession {

    /** A delivery sent to the client and not settled: which link it went by, and what the broker calls it. */
    record Sent(OutgoingLink link, BrokerConnection.Delivery delivery) {
    }

    final int channel;
    final Map<Long, IncomingLink> incoming = new HashMap<>();
    final Map<Long, OutgoingLink> outgoing = new HashMap<>();
    // The handles of the links the server refused, until the client answers the detach: what it
    // sent for them meanwhile is not an error.
    final Set<Long> refused = new HashSet<>();
    // By delivery-id, in order: a disposition names a range of them.
    final TreeMap<Long, Sent> unsettled = new TreeMap<>();

    // The transfer-id of the next transfer frame the server sends, and how many the client still accepts.
    long nextOutgoingId;
    long remoteIncomingWindow;
    // The transfer-id of the next transfer frame expected from the client, and how many the server still accepts.
    long nextIncomingId;
    long incomingWindow = Limits.INCOMING_WINDOW;
    // The delivery-id of the next delivery the server sends on this session.
    long nextDeliveryId;

    AmqpSession(int channel, long clientNextOutgoingId, long clientIncomingWindow) {
        this.channel = channel;
        this.nextIncomingId = clientNextOutgoingId;
        this.remoteIncomingWindow = clientIncomingWindow;
    }
}
