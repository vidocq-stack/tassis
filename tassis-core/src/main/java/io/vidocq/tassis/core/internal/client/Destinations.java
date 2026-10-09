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
package io.vidocq.tassis.core.internal.client;

import io.vidocq.tassis.core.TassisQueue;
import jakarta.jms.Destination;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.Queue;

/** Checks the destinations a client hands to the provider. */
final class Destinations {

    private Destinations() {
    }

    /**
     * Returns {@code destination} as a queue of this provider.
     *
     * @throws InvalidDestinationException if it is null, deleted, or not a Tassis queue
     */
    static Queue queue(Destination destination) throws InvalidDestinationException {
        return switch (destination) {
            case null -> throw new InvalidDestinationException("the destination is null");
            case TassisQueue queue -> queue;
            case TassisTemporaryQueue temporary -> {
                if (temporary.isDeleted()) {
                    throw new InvalidDestinationException("temporary queue " + temporary + " has been deleted");
                }
                yield temporary;
            }
            default -> throw new InvalidDestinationException(
                    destination.getClass().getName() + " is not a Tassis queue (topics: see ROADMAP M4)");
        };
    }

    /** Returns whether a queue {@link #queue} has accepted is a temporary one. */
    static boolean isTemporary(Queue queue) {
        return queue instanceof TassisTemporaryQueue;
    }

    /** Returns the broker-side name of a queue {@link #queue} has accepted. */
    static String name(Queue queue) {
        return queue instanceof TassisQueue named
                ? named.getQueueName()
                : ((TassisTemporaryQueue) queue).getQueueName();
    }
}
