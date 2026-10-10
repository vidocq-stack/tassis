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
package io.vidocq.tassis.cdi.internal;

import jakarta.jms.BytesMessage;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.io.Serializable;

/**
 * What an application bean holds when it injects a {@code JMSContext}: a container-managed
 * context (Jakarta Messaging 3.1 §12.4.2). It owns nothing: each call goes to the context that
 * the request in progress has for this injection point's annotations (§12.4.4), which the
 * container creates on first use and closes with the request.
 *
 * <p>The methods that would change the state of that shared context are refused (§12.4.5).
 */
final class InjectedJMSContext implements JMSContext {

    private final ContextKey key;
    private final RequestContexts contexts;

    InjectedJMSContext(ContextKey key, RequestContexts contexts) {
        this.key = key;
        this.contexts = contexts;
    }

    /** The context of the request in progress; fails if there is no request. */
    private JMSContext context() {
        return contexts.get(key);
    }

    private static IllegalStateRuntimeException refused(String method) {
        return new IllegalStateRuntimeException(method + " is not permitted on an injected JMSContext: "
                + "the container manages it, and other beans share it");
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return context().createContext(sessionMode);
    }

    @Override
    public JMSProducer createProducer() {
        return context().createProducer();
    }

    @Override
    public String getClientID() {
        return context().getClientID();
    }

    @Override
    public ConnectionMetaData getMetaData() {
        return context().getMetaData();
    }

    @Override
    public ExceptionListener getExceptionListener() {
        return context().getExceptionListener();
    }

    @Override
    public boolean getAutoStart() {
        return context().getAutoStart();
    }

    @Override
    public BytesMessage createBytesMessage() {
        return context().createBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        return context().createMapMessage();
    }

    @Override
    public Message createMessage() {
        return context().createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return context().createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        return context().createObjectMessage(object);
    }

    @Override
    public StreamMessage createStreamMessage() {
        return context().createStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() {
        return context().createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) {
        return context().createTextMessage(text);
    }

    @Override
    public boolean getTransacted() {
        return context().getTransacted();
    }

    @Override
    public int getSessionMode() {
        return context().getSessionMode();
    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        return context().createConsumer(destination);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        return context().createConsumer(destination, messageSelector);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        return context().createConsumer(destination, messageSelector, noLocal);
    }

    @Override
    public Queue createQueue(String queueName) {
        return context().createQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) {
        return context().createTopic(topicName);
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return context().createDurableConsumer(topic, name);
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        return context().createDurableConsumer(topic, name, messageSelector, noLocal);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return context().createSharedDurableConsumer(topic, name);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        return context().createSharedDurableConsumer(topic, name, messageSelector);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return context().createSharedConsumer(topic, sharedSubscriptionName);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        return context().createSharedConsumer(topic, sharedSubscriptionName, messageSelector);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return context().createBrowser(queue);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        return context().createBrowser(queue, messageSelector);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        return context().createTemporaryQueue();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        return context().createTemporaryTopic();
    }

    @Override
    public void unsubscribe(String name) {
        context().unsubscribe(name);
    }

    // ---- not permitted on an injected context (§12.4.5) ----------------------

    @Override
    public void setClientID(String clientID) {
        throw refused("setClientID");
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        throw refused("setExceptionListener");
    }

    @Override
    public void start() {
        throw refused("start");
    }

    @Override
    public void stop() {
        throw refused("stop");
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        throw refused("setAutoStart");
    }

    @Override
    public void close() {
        throw refused("close");
    }

    @Override
    public void commit() {
        throw refused("commit");
    }

    @Override
    public void rollback() {
        throw refused("rollback");
    }

    @Override
    public void recover() {
        throw refused("recover");
    }

    @Override
    public void acknowledge() {
        throw refused("acknowledge");
    }
}
