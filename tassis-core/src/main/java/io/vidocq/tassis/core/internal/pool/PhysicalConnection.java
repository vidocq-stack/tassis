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
package io.vidocq.tassis.core.internal.pool;

import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import io.vidocq.tassis.core.internal.client.Domain;
import io.vidocq.tassis.core.internal.client.TassisConnection;
import io.vidocq.tassis.core.internal.client.TassisContext;
import jakarta.jms.Connection;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.QueueConnection;
import jakarta.jms.TopicConnection;

/**
 * A connection to a broker that outlives the connections the application sees: a pool lends it
 * to one of them at a time. Each is a new object, so nothing of the one before shows through.
 */
public final class PhysicalConnection {

    private final BrokerConnection link;
    private final int maxListenerDeliveries;

    PhysicalConnection(BrokerConnection link, int maxListenerDeliveries) {
        this.link = link;
        this.maxListenerDeliveries = maxListenerDeliveries;
    }

    private TassisConnection lend(Domain domain, Recycler recycler) throws JMSException {
        return new TassisConnection(link, domain, null, maxListenerDeliveries, recycler);
    }

    /** A connection for the application, which tells the recycler when it is closed. */
    public Connection connection(Recycler recycler) throws JMSException {
        return lend(Domain.UNIFIED, recycler);
    }

    public QueueConnection queueConnection(Recycler recycler) throws JMSException {
        return lend(Domain.QUEUE, recycler);
    }

    public TopicConnection topicConnection(Recycler recycler) throws JMSException {
        return lend(Domain.TOPIC, recycler);
    }

    /** A context on a connection of its own, which tells the recycler when the last context on it is closed. */
    public JMSContext context(int sessionMode, Recycler recycler) throws JMSException {
        return new TassisContext(lend(Domain.UNIFIED, recycler), sessionMode);
    }

    /**
     * Asks to be told when the connection to the broker is lost, at once if it is already. Whoever
     * asks last is the one told.
     */
    public void onLost(Runnable listener) {
        link.onLost(problem -> listener.run());
    }

    /** Whether the connection to the broker is known to be lost. */
    public boolean isLost() {
        return link.isLost();
    }

    /** Closes the connection to the broker. */
    public void close() {
        link.close();
    }
}
