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
package io.vidocq.tassis.amqp.internal.client;

import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import io.vidocq.tassis.core.internal.message.TassisMessage;

/** A message the broker sent on a link, known to both by the delivery-id of its transfer. */
final class AmqpDelivery implements BrokerConnection.Delivery {

    final AmqpBrokerConnection.ReceiverLink link;
    final long id;
    private final TassisMessage message;
    private final int deliveryCount;

    AmqpDelivery(AmqpBrokerConnection.ReceiverLink link, long id, TassisMessage message, int deliveryCount) {
        this.link = link;
        this.id = id;
        this.message = message;
        this.deliveryCount = deliveryCount;
    }

    @Override
    public int deliveryCount() {
        return deliveryCount;
    }

    @Override
    public TassisMessage message() {
        return message;
    }
}
