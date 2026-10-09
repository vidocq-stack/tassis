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

import jakarta.jms.JMSException;
import jakarta.jms.TemporaryQueue;

/**
 * A queue that lives as long as the connection that created it, and that only
 * this connection may consume from (Jakarta Messaging 3.1 §6.2.2).
 */
final class TassisTemporaryQueue implements TemporaryQueue {

    private final TassisConnection owner;
    private final String name;

    TassisTemporaryQueue(TassisConnection owner, String name) {
        this.owner = owner;
        this.name = name;
    }

    TassisConnection owner() {
        return owner;
    }

    /** The broker is the one place that knows which temporary queues exist. */
    boolean isDeleted() {
        return !owner.broker().hasTemporaryQueue(name);
    }

    /** Deletes this queue because its connection is closing. */
    void dispose() {
        owner.broker().dropTemporaryQueue(name);
    }

    @Override
    public String getQueueName() {
        return name;
    }

    @Override
    public void delete() throws JMSException {
        owner.broker().deleteTemporaryQueue(name);
        owner.forget(this);
    }

    @Override
    public String toString() {
        return name;
    }
}
