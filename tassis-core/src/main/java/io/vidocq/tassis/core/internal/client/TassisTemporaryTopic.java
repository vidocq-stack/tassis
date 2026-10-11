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
import jakarta.jms.TemporaryTopic;

/**
 * A topic that lives as long as the connection that created it, and that only
 * this connection may subscribe to (Jakarta Messaging 3.1 §6.2.2).
 */
final class TassisTemporaryTopic implements TemporaryTopic {

    private final TassisConnection owner;
    private final String name;

    TassisTemporaryTopic(TassisConnection owner, String name) {
        this.owner = owner;
        this.name = name;
    }

    TassisConnection owner() {
        return owner;
    }

    /** The broker is the one place that knows which temporary topics exist. */
    boolean isDeleted() {
        return !owner.link().hasTemporaryTopic(name);
    }

    @Override
    public String getTopicName() {
        return name;
    }

    @Override
    public void delete() throws JMSException {
        owner.link().deleteTemporaryTopic(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
