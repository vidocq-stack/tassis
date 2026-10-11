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
import io.vidocq.tassis.core.internal.broker.Transport;
import io.vidocq.tassis.core.internal.settings.Settings;
import jakarta.jms.JMSException;

/** The transport {@code amqp} of a connection URL: a broker opened to the network by a server. */
public final class AmqpTransport implements Transport {

    @Override
    public String name() {
        return "amqp";
    }

    @Override
    public BrokerConnection connect(Settings settings, String user, String password) throws JMSException {
        return new AmqpBrokerConnection(settings.serverHost(), settings.serverPort(), settings.clientId(), user,
                password);
    }
}
