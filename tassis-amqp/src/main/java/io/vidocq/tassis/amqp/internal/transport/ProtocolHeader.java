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

import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;

/**
 * The eight octets each peer sends first, on a new connection and again after each security
 * layer is in place: "AMQP", the protocol they announce, and its version (part 2, §2.2).
 *
 * @param protocolId 0 for AMQP itself, 2 for TLS (part 5, §5.2.1), 3 for SASL (§5.3.1)
 */
public record ProtocolHeader(int protocolId, int major, int minor, int revision) {

    public static final int LENGTH = 8;

    public static final ProtocolHeader AMQP = new ProtocolHeader(0, 1, 0, 0);
    public static final ProtocolHeader TLS = new ProtocolHeader(2, 1, 0, 0);
    public static final ProtocolHeader SASL = new ProtocolHeader(3, 1, 0, 0);

    public byte[] octets() {
        return new byte[] {'A', 'M', 'Q', 'P', (byte) protocolId, (byte) major, (byte) minor, (byte) revision};
    }

    /** Reads a protocol header: any version, any protocol id, for the caller to accept or not. */
    public static ProtocolHeader parse(byte[] octets) throws AmqpDecodeException {
        if (octets.length != LENGTH || octets[0] != 'A' || octets[1] != 'M' || octets[2] != 'Q' || octets[3] != 'P') {
            throw new AmqpDecodeException("not an AMQP protocol header");
        }
        return new ProtocolHeader(octets[4] & 0xFF, octets[5] & 0xFF, octets[6] & 0xFF, octets[7] & 0xFF);
    }
}
