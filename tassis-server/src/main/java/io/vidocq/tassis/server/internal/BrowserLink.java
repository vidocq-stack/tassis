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

import io.vidocq.tassis.core.internal.message.TassisMessage;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * A link by which a client looks at a queue without consuming it (the distribution mode
 * {@code copy}; Jakarta Messaging 3.1 §4.1.3): the messages the queue held when the link was
 * attached, sent as the client grants credit and settled as they are sent. Guarded by the lock
 * of its connection.
 */
final class BrowserLink {

    final long handle;
    final List<TassisMessage> messages;
    int next;
    long credit;
    long deliveryCount;
    /** The transfer frames of the message being sent that the window of the session had no room for. */
    final Deque<byte[]> frames = new ArrayDeque<>();
    /** Whether the client asked to be told when there is nothing more for its credit (§2.6.7). */
    boolean draining;

    BrowserLink(long handle, List<TassisMessage> messages) {
        this.handle = handle;
        this.messages = messages;
    }
}
