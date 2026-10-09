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

import jakarta.jms.Connection;
import jakarta.jms.ConnectionConsumer;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.ServerSessionPool;
import jakarta.jms.Session;
import jakarta.jms.Topic;

/**
 * A client's active connection to the in-VM broker (Jakarta Messaging 3.1 §6.1).
 * M0 skeleton: only {@link #close()} is implemented; every other method throws
 * with a pointer to the ROADMAP milestone that implements it.
 */
public final class TassisConnection implements Connection {

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) {
        throw new UnsupportedOperationException("createSession(transacted, acknowledgeMode): see ROADMAP M2");
    }

    @Override
    public Session createSession(int sessionMode) {
        throw new UnsupportedOperationException("createSession(sessionMode): see ROADMAP M2");
    }

    @Override
    public Session createSession() {
        throw new UnsupportedOperationException("createSession(): see ROADMAP M2");
    }

    @Override
    public String getClientID() {
        throw new UnsupportedOperationException("getClientID(): see ROADMAP M4");
    }

    @Override
    public void setClientID(String clientID) {
        throw new UnsupportedOperationException("setClientID(clientID): see ROADMAP M4");
    }

    @Override
    public ConnectionMetaData getMetaData() {
        throw new UnsupportedOperationException("getMetaData(): see ROADMAP M2");
    }

    @Override
    public ExceptionListener getExceptionListener() {
        throw new UnsupportedOperationException("getExceptionListener(): see ROADMAP M6");
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        throw new UnsupportedOperationException("setExceptionListener(listener): see ROADMAP M6");
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException("start(): see ROADMAP M2");
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("stop(): see ROADMAP M2");
    }

    // Nothing is allocated yet, and closing a closed connection must not throw (§6.1.8).
    @Override
    public void close() {
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException("createConnectionConsumer: optional (§11), not on the ROADMAP");
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException("createSharedConnectionConsumer: optional (§11), not on the ROADMAP");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException("createDurableConnectionConsumer: optional (§11), not on the ROADMAP");
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) {
        throw new UnsupportedOperationException(
                "createSharedDurableConnectionConsumer: optional (§11), not on the ROADMAP");
    }
}
