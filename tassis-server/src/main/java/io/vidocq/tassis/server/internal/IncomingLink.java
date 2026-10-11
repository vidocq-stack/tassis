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

    /** What a link the client sends by is for. */
    enum Kind {
        /** Messages for a queue or a topic. */
        DESTINATION,
        /** The declare and discharge of transactions (part 4, §4.2). */
        COORDINATOR,
        /** Nothing is sent by it: it stands for a temporary destination, which lasts as long as it does. */
        TEMPORARY
    }

    final long handle;
    final String name;
    final Kind kind;
    /**
     * The queue or topic the messages go to, or {@code null} if each message says it in its
     * {@code to} field. For a {@link Kind#TEMPORARY} link, the temporary destination it stands for.
     */
    final String address;
    final boolean topic;
    final boolean temporary;

    long credit;
    long deliveryCount;

    // The delivery in progress, when its message came in several frames.
    ByteArrayOutputStream partial;
    Long partialDeliveryId;
    boolean partialSettled;

    // The transaction the delivery in progress is part of, if its first frame named one.
    io.vidocq.tassis.amqp.internal.types.Binary partialTransaction;

    IncomingLink(long handle, String name, Kind kind, String address, boolean topic, boolean temporary,
            long initialDeliveryCount) {
        this.handle = handle;
        this.name = name;
        this.kind = kind;
        this.address = address;
        this.topic = topic;
        this.temporary = temporary;
        this.deliveryCount = initialDeliveryCount;
    }
}
