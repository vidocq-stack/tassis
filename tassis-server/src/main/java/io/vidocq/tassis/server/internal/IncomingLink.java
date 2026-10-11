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

import java.io.ByteArrayOutputStream;

/**
 * A link by which a client sends messages (the server is its receiving end): where they go,
 * the credit the client has left, and the message being put back together from its transfer
 * frames. Guarded by the lock of its connection.
 */
final class IncomingLink {

    final long handle;
    final String name;
    /** The queue the messages go to, or {@code null} if each message says it in its {@code to} field. */
    final String queue;

    long credit;
    long deliveryCount;

    // The delivery in progress, when its message came in several frames.
    ByteArrayOutputStream partial;
    Long partialDeliveryId;
    boolean partialSettled;

    IncomingLink(long handle, String name, String queue, long initialDeliveryCount) {
        this.handle = handle;
        this.name = name;
        this.queue = queue;
        this.deliveryCount = initialDeliveryCount;
    }
}
