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
/**
 * AMQP 1.0 (OASIS Standard, 2012) for Tassis: the codec, how a Jakarta Messaging message
 * travels as an AMQP one, and the transport by which a Tassis client reaches a broker of the
 * network. Nothing is exported to applications; the server reads the codec.
 */
module io.vidocq.tassis.amqp {
    requires io.vidocq.tassis.core;

    exports io.vidocq.tassis.amqp.internal.jms to io.vidocq.tassis.server;
    exports io.vidocq.tassis.amqp.internal.messaging to io.vidocq.tassis.server;
    exports io.vidocq.tassis.amqp.internal.security to io.vidocq.tassis.server;
    exports io.vidocq.tassis.amqp.internal.transactions to io.vidocq.tassis.server;
    exports io.vidocq.tassis.amqp.internal.transport to io.vidocq.tassis.server;
    exports io.vidocq.tassis.amqp.internal.types to io.vidocq.tassis.server;

    // How a connection factory of tassis-core reaches a broker of the network: tassis:amqp://host
    provides io.vidocq.tassis.core.internal.broker.Transport
            with io.vidocq.tassis.amqp.internal.client.AmqpTransport;
}
