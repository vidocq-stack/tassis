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
import jakarta.jms.JMSException;

/**
 * A link by which a client receives messages (the server is its sending end), and the thread
 * that feeds it: the pump, which takes a message from the broker whenever the client has
 * credit for one (part 2, §2.6.7).
 *
 * <p>The fields are guarded by the lock of the connection. The pump never waits for the broker
 * while holding it.
 */
final class OutgoingLink implements Runnable {

    private static final System.Logger LOG = System.getLogger(OutgoingLink.class.getName());

    final AmqpConnection connection;
    final AmqpSession session;
    final long handle;
    final String name;
    final BrokerConnection.Consumer consumer;
    /** Whether the client asked for deliveries it does not have to settle (snd-settle-mode settled). */
    final boolean presettled;
    /** The name of the durable subscription the link reads, which goes if the link is closed for good; else null. */
    final String durableName;
    /** Whether that name is one of the broker rather than of the client identifier. */
    final boolean global;

    long credit;
    long deliveryCount;
    /** The client asked for its credit to be used up now: by messages if there are any, else given up. */
    boolean draining;
    boolean closed;

    OutgoingLink(AmqpConnection connection, AmqpSession session, long handle, String name,
            BrokerConnection.Consumer consumer, boolean presettled, String durableName, boolean global) {
        this.connection = connection;
        this.session = session;
        this.handle = handle;
        this.name = name;
        this.consumer = consumer;
        this.presettled = presettled;
        this.durableName = durableName;
        this.global = global;
    }

    @Override
    public void run() {
        try {
            pump();
        } catch (JMSException | RuntimeException e) {
            // Only an interrupted wait, or a defect: either way this link delivers nothing more.
            LOG.log(System.Logger.Level.WARNING, "the link " + name + " stops delivering", e);
        }
    }

    private void pump() throws JMSException {
        while (true) {
            boolean drain;
            connection.lock.lock();
            try {
                while (!closed && credit <= 0) {
                    connection.changed.awaitUninterruptibly();
                }
                if (closed) {
                    return;
                }
                drain = draining;
            } finally {
                connection.lock.unlock();
            }

            BrokerConnection.Delivery delivery;
            if (drain) {
                // §2.6.7: what is there now, and nothing is waited for.
                delivery = consumer.receive(0, false);
                if (delivery == null) {
                    connection.creditGivenUp(this);
                    continue;
                }
            } else {
                long period = consumer.listen(true);
                // Looked at again once listening: whoever withdraws the credit ends the period
                // after changing the state, so one of the two is seen here or in take().
                if (!connection.mayDeliver(this)) {
                    continue;
                }
                delivery = consumer.take(period);
                if (delivery == null) {
                    // The credit was withdrawn, or the consumer closed: the loop finds out which.
                    continue;
                }
            }
            connection.deliver(this, delivery);
        }
    }
}
