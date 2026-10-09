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

import io.vidocq.tassis.core.internal.broker.Broker;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Queue;
import jakarta.jms.QueueReceiver;
import java.util.concurrent.TimeUnit;

/** Receives the messages of a queue, synchronously (Jakarta Messaging 3.1 §8). */
final class TassisMessageConsumer implements QueueReceiver, SessionChild {

    private final TassisSession session;
    private final Queue queue;
    private final Broker.QueueConsumer attachment;
    private volatile boolean closed;

    TassisMessageConsumer(TassisSession session, Queue queue) throws JMSException {
        this.session = session;
        this.queue = queue;
        this.attachment = session.broker().attach(Destinations.name(queue), Destinations.isTemporary(queue),
                session.connection()::isStarted);
    }

    private void checkOpen() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("the consumer is closed");
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
        return null;
    }

    @Override
    public Message receive() throws JMSException {
        return receive(0);
    }

    /** A timeout of zero never expires (§8.5). */
    @Override
    public Message receive(long timeout) throws JMSException {
        checkOpen();
        return session.delivered(session.broker().receive(attachment,
                timeout == 0 ? Broker.FOREVER : TimeUnit.MILLISECONDS.toNanos(timeout)));
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        checkOpen();
        return session.delivered(session.broker().receive(attachment, 0));
    }

    @Override
    public void close() {
        closed = true;
        session.broker().detach(attachment);
        session.forget(this);
    }

    // ---- asynchronous delivery: ROADMAP M6 -----------------------------

    @Override
    public MessageListener getMessageListener() {
        throw new UnsupportedOperationException("message listeners: see ROADMAP M6");
    }

    @Override
    public void setMessageListener(MessageListener listener) {
        throw new UnsupportedOperationException("message listeners: see ROADMAP M6");
    }
}
