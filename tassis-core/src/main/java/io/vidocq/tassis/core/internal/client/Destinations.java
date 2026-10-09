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
import io.vidocq.tassis.core.TassisTopic;
import jakarta.jms.Destination;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.Queue;
import jakarta.jms.Topic;

/** The one place that decides which {@code Destination} objects are ours, and what they name. */
final class Destinations {

    /**
     * A destination as the broker knows it.
     *
     * @param owner the connection a temporary destination belongs to, {@code null} otherwise
     */
    record Address(String name, boolean topic, TassisConnection owner) {

        boolean temporary() {
            return owner != null;
        }
    }

    private Destinations() {
    }

    /**
     * Returns what {@code destination} names in the broker.
     *
     * @throws InvalidDestinationException if it is null, deleted, or not a Tassis destination
     */
    static Address resolve(Destination destination) throws InvalidDestinationException {
        return switch (destination) {
            case null -> throw new InvalidDestinationException("the destination is null");
            case TassisQueue queue -> new Address(queue.getQueueName(), false, null);
            case TassisTopic topic -> new Address(topic.getTopicName(), true, null);
            case TassisTemporaryQueue temporary -> {
                if (temporary.isDeleted()) {
                    throw new InvalidDestinationException("temporary queue " + temporary + " has been deleted");
                }
                yield new Address(temporary.getQueueName(), false, temporary.owner());
            }
            case TassisTemporaryTopic temporary -> {
                if (temporary.isDeleted()) {
                    throw new InvalidDestinationException("temporary topic " + temporary + " has been deleted");
                }
                yield new Address(temporary.getTopicName(), true, temporary.owner());
            }
            default -> throw new InvalidDestinationException(
                    destination.getClass().getName() + " is not a Tassis destination");
        };
    }

    /** Like {@link #resolve}, for a method that only takes a queue. */
    static Address queue(Queue queue) throws InvalidDestinationException {
        Address address = resolve(queue);
        if (address.topic()) {
            throw new InvalidDestinationException(queue + " is a topic");
        }
        return address;
    }

    /** Like {@link #resolve}, for a method that only takes a topic. */
    static Address topic(Topic topic) throws InvalidDestinationException {
        Address address = resolve(topic);
        if (!address.topic()) {
            throw new InvalidDestinationException(topic + " is a queue");
        }
        return address;
    }
}
