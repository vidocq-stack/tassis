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

import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import io.vidocq.tassis.core.internal.message.TassisMessage;
import io.vidocq.tassis.core.internal.selector.Selector;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageListener;
import jakarta.jms.Queue;
import jakarta.jms.QueueReceiver;
import jakarta.jms.StreamMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;
import java.util.concurrent.TimeUnit;

/**
 * Receives the messages of a queue or of a topic subscription (Jakarta Messaging
 * 3.1 §8): synchronously through {@code receive}, or, once it has a listener,
 * asynchronously from a dispatcher thread of its own.
 */
final class TassisMessageConsumer implements QueueReceiver, TopicSubscriber, SessionChild {

    private final TassisSession session;
    private final Destination destination;
    private final boolean noLocal;
    private final Selector selector;
    private final BrokerConnection.Consumer attachment;
    private volatile boolean closed;
    // The broker is told whether there is one (attachment.listen): the dispatcher's wait for a
    // message ends when there is none any more.
    private volatile MessageListener listener;
    // The virtual thread that takes messages for the listener; null when there is no listener.
    private volatile Thread dispatcher;

    TassisMessageConsumer(TassisSession session, Destination destination, boolean noLocal, Selector selector,
            BrokerConnection.Consumer attachment) {
        this.session = session;
        this.destination = destination;
        this.noLocal = noLocal;
        this.selector = selector;
        this.attachment = attachment;
    }

    private void checkOpen() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("the consumer is closed");
        }
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkOpen();
        return destination instanceof Queue queue ? queue : null;
    }

    @Override
    public Topic getTopic() throws JMSException {
        checkOpen();
        return destination instanceof Topic topic ? topic : null;
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        checkOpen();
        return noLocal;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkOpen();
        return selector == null ? null : selector.text();
    }

    // ---- synchronous delivery (§8.5) -------------------------------------

    @Override
    public Message receive() throws JMSException {
        return receive(0);
    }

    /** A timeout of zero never expires (§8.5). */
    @Override
    public Message receive(long timeout) throws JMSException {
        checkSynchronous();
        return take(timeout == 0 ? BrokerConnection.FOREVER : TimeUnit.MILLISECONDS.toNanos(timeout));
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        checkSynchronous();
        return take(0);
    }

    /**
     * An automatically acknowledged session keeps nothing of a message it receives: the broker
     * settles the delivery as it makes it. In the other modes the session answers for it.
     */
    private Message take(long timeoutNanos) throws JMSException {
        return session.delivered(attachment.receive(timeoutNanos, session.automaticallyAcknowledged()));
    }

    /**
     * Receives the next message and returns its body as a {@code c}
     * ({@code JMSConsumer.receiveBody}), or {@code null} if no message comes in time.
     *
     * @throws MessageFormatException if the message has no body of that type. An automatically
     *         acknowledged session then behaves as if the call had not been made: the message
     *         stays where it was. In the other modes the message counts as delivered.
     */
    <T> T receiveBody(Class<T> c, long timeoutNanos) throws JMSException {
        checkSynchronous();
        // Not settled on delivery, whatever the mode: the message may still have to be given back.
        BrokerConnection.Delivery delivery = attachment.receive(timeoutNanos, false);
        if (delivery == null) {
            return null;
        }
        TassisMessage message = delivery.message();
        T body;
        try {
            if (message.getClass() == TassisMessage.class || message instanceof StreamMessage) {
                throw new MessageFormatException("this kind of message has no body to receive");
            }
            body = message.getBody(c);
            if (body == null) {
                throw new MessageFormatException("the message has no body");
            }
        } catch (JMSException | RuntimeException | Error failure) {
            // Whatever went wrong, the delivery must end up somewhere.
            if (session.automaticallyAcknowledged()) {
                session.link().returnUndelivered(delivery);
            } else {
                session.delivered(delivery);
            }
            throw failure;
        }
        if (session.automaticallyAcknowledged()) {
            // Null if the connection was closed meanwhile: the message went back to its queue.
            return session.link().settle(delivery) ? body : null;
        }
        return session.delivered(delivery) == null ? null : body;
    }

    private void checkSynchronous() throws IllegalStateException {
        checkOpen();
        if (listener != null) {
            throw new IllegalStateException("this consumer has a message listener: it cannot also receive synchronously");
        }
    }

    // ---- asynchronous delivery (§8.7) ------------------------------------

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkOpen();
        return listener;
    }

    @Override
    public void setMessageListener(MessageListener newListener) throws JMSException {
        checkOpen();
        listener = newListener;
        long period = attachment.listen(newListener != null);
        if (newListener == null) {
            awaitDispatcher();
        } else if (dispatcher == null && !closed) {
            // Assigned before it runs: the dispatcher stops as soon as it is no longer the one in charge.
            Thread thread = Thread.ofVirtual().name("tassis-listener-" + destination).unstarted(() -> dispatch(period));
            dispatcher = thread;
            thread.start();
        }
    }

    /**
     * The listener the calling dispatcher should call for the message it holds, or {@code null}
     * if it should not: the consumer is closed, has no listener, or has another dispatcher by now.
     */
    MessageListener listenerToCall() {
        return closed || dispatcher != Thread.currentThread() ? null : listener;
    }

    /** The dispatcher: takes messages one at a time for as long as the consumer has a listener. */
    private void dispatch(long period) {
        Thread me = Thread.currentThread();
        try {
            while (listenerToCall() != null) {
                BrokerConnection.Delivery delivery = attachment.take(period);
                if (delivery == null) {
                    // The listener this thread served is gone, or the consumer is closed, whoever
                    // closed it: either way there is nothing more to wait for.
                    break;
                }
                session.dispatch(this, delivery);
            }
        } catch (JMSException interrupted) {
            // Only an interrupt ends a wait this way; nothing was taken from the broker.
        } finally {
            // Whatever ended it, a later setMessageListener must be able to start another one.
            if (dispatcher == me) {
                dispatcher = null;
            }
        }
    }

    /**
     * Returns once the dispatcher has stopped, and so once a listener call in progress has
     * returned (§8.8). A listener of this session that ends the dispatching is not made to wait:
     * it holds the session's turn, so no other listener of the session can be running.
     */
    private void awaitDispatcher() {
        Thread running = dispatcher;
        if (running == null) {
            return;
        }
        session.resumeListeners();
        if (!session.inOwnListener()) {
            boolean interrupted = false;
            while (true) {
                try {
                    running.join();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (dispatcher == running) {
            dispatcher = null;
        }
    }

    @Override
    public void close() {
        shut();
    }

    @Override
    public void shut() {
        closed = true;
        attachment.close();
        awaitDispatcher();
        session.forget(this);
    }
}
