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

import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import java.util.ArrayList;
import java.util.List;

/**
 * A local transaction a client declared (part 4, §4.3): what it has sent and what it has
 * accepted so far, which reach the broker together when it is discharged, or not at all.
 * Guarded by the lock of its connection.
 */
final class Transaction {

    final Binary id;
    /** The link it was declared on: it is rolled back if that link goes before it is discharged (§4.2). */
    final IncomingLink coordinator;
    final List<BrokerConnection.Outgoing> sends = new ArrayList<>();
    final List<BrokerConnection.Delivery> consumed = new ArrayList<>();
    /** The size of the sends as they came, which is bounded (see {@code Limits}). */
    long octets;

    Transaction(Binary id, IncomingLink coordinator) {
        this.id = id;
        this.coordinator = coordinator;
    }
}
