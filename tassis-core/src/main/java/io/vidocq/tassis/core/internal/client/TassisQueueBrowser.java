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

import io.vidocq.tassis.core.internal.selector.Selector;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import java.util.Collections;
import java.util.Enumeration;

/** Looks at the messages of a queue without removing them (Jakarta Messaging 3.1 §4.1.3). */
final class TassisQueueBrowser implements QueueBrowser, SessionChild {

    private final TassisSession session;
    private final Queue queue;
    private final Selector selector;
    private volatile boolean closed;

    TassisQueueBrowser(TassisSession session, Queue queue, Selector selector) {
        this.session = session;
        this.queue = queue;
        this.selector = selector;
    }

    private void checkOpen() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("the browser is closed");
        }
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkOpen();
        return queue;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkOpen();
        return selector == null ? null : selector.text();
    }

    /** A snapshot of the queue at the time of the call. */
    @Override
    public Enumeration<?> getEnumeration() throws JMSException {
        checkOpen();
        return Collections.enumeration(session.broker().browse(Destinations.queue(queue).name(), selector));
    }

    @Override
    public void close() {
        closed = true;
        session.forget(this);
    }
}
