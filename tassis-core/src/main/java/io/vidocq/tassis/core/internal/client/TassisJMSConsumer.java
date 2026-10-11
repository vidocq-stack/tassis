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
import jakarta.jms.JMSConsumer;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import java.util.concurrent.TimeUnit;

/** The consumer of the simplified API (Jakarta Messaging 3.1 §2.8): a classic consumer, unchecked. */
final class TassisJMSConsumer implements JMSConsumer {

    private final TassisMessageConsumer consumer;

    TassisJMSConsumer(TassisMessageConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public String getMessageSelector() {
        return Unchecked.get(consumer::getMessageSelector);
    }

    @Override
    public MessageListener getMessageListener() {
        return Unchecked.get(consumer::getMessageListener);
    }

    @Override
    public void setMessageListener(MessageListener listener) {
        Unchecked.run(() -> consumer.setMessageListener(listener));
    }

    @Override
    public Message receive() {
        return Unchecked.get(consumer::receive);
    }

    @Override
    public Message receive(long timeout) {
        return Unchecked.get(() -> consumer.receive(timeout));
    }

    @Override
    public Message receiveNoWait() {
        return Unchecked.get(consumer::receiveNoWait);
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
        return Unchecked.get(() -> consumer.receiveBody(c, BrokerConnection.FOREVER));
    }

    /** A timeout of zero never expires, as for {@code receive}. */
    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        return Unchecked.get(() -> consumer.receiveBody(c,
                timeout == 0 ? BrokerConnection.FOREVER : TimeUnit.MILLISECONDS.toNanos(timeout)));
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        return Unchecked.get(() -> consumer.receiveBody(c, 0));
    }
}
