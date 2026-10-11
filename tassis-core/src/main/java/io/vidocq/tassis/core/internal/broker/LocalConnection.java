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
package io.vidocq.tassis.core.internal.broker;

import io.vidocq.tassis.core.internal.message.TassisMessage;
import io.vidocq.tassis.core.internal.selector.Selector;
import jakarta.jms.JMSException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A connection to the broker of this JVM: what the {@link Broker} keeps for one client, and the
 * client's way in. The state is the broker's, guarded by the broker lock; every method goes to
 * the {@code Broker}, where the lock is taken.
 */
final class LocalConnection implements BrokerConnection {

    final Broker broker;
    // Written under the broker lock; volatile for isStarted(), which a session asks at each message.
    volatile boolean started;
    boolean closed;
    String clientId;
    final Set<Broker.Consumer> consumers = new LinkedHashSet<>();
    // The deliveries the client has neither settled nor given back: the broker's to give back
    // when the connection closes.
    final Set<Broker.Delivery> unsettled = new LinkedHashSet<>();
    final Set<String> temporaryQueues = new HashSet<>();
    final Set<String> temporaryTopics = new HashSet<>();

    LocalConnection(Broker broker) {
        this.broker = broker;
    }

    /**
     * Who this connection is as a publisher or a subscriber, for {@code noLocal}: its client
     * identifier, so that it stays the same across reconnections, or else itself.
     */
    Object identity() {
        return clientId != null ? clientId : this;
    }

    @Override
    public String nextMessageId() {
        return broker.nextMessageId();
    }

    @Override
    public void claimClientId(String id) throws JMSException {
        broker.claimClientId(this, id);
    }

    @Override
    public boolean start() {
        return broker.start(this);
    }

    @Override
    public void stop() {
        broker.stop(this);
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public String createTemporaryQueue() throws JMSException {
        return broker.createTemporary(this, false);
    }

    @Override
    public String createTemporaryTopic() throws JMSException {
        return broker.createTemporary(this, true);
    }

    @Override
    public boolean hasTemporaryQueue(String queue) {
        return broker.hasTemporaryQueue(queue);
    }

    @Override
    public boolean hasTemporaryTopic(String topic) {
        return broker.hasTemporaryTopic(topic);
    }

    @Override
    public void deleteTemporaryQueue(String queue) throws JMSException {
        broker.deleteTemporaryQueue(queue);
    }

    @Override
    public void deleteTemporaryTopic(String topic) throws JMSException {
        broker.deleteTemporaryTopic(topic);
    }

    @Override
    public void send(Outgoing message) throws JMSException {
        broker.commit(this, List.of(message), List.of());
    }

    @Override
    public void commit(Collection<Outgoing> sends, Collection<? extends Delivery> consumed) throws JMSException {
        broker.commit(this, sends, consumed);
    }

    @Override
    public Consumer attach(String queue, boolean temporary, Selector selector) throws JMSException {
        return broker.attach(this, queue, temporary, selector);
    }

    @Override
    public Consumer subscribe(Subscribing request) throws JMSException {
        return broker.subscribe(this, request);
    }

    @Override
    public String durableSubscriptionTopic(String name, boolean global) {
        return broker.durableSubscriptionTopic(this, name, global);
    }

    @Override
    public void unsubscribe(String name, boolean global) throws JMSException {
        broker.unsubscribe(this, name, global);
    }

    @Override
    public boolean settle(Delivery delivery) {
        return broker.settle(this, delivery);
    }

    @Override
    public void settle(Collection<? extends Delivery> deliveries) {
        broker.settle(this, deliveries);
    }

    @Override
    public void redeliver(Collection<? extends Delivery> deliveries) {
        broker.redeliver(this, deliveries);
    }

    @Override
    public void returnUndelivered(Delivery delivery) {
        broker.returnUndelivered(this, delivery);
    }

    @Override
    public List<TassisMessage> browse(String queue, Selector selector) {
        return broker.browse(queue, selector);
    }

    @Override
    public void close() {
        broker.close(this);
    }
}
