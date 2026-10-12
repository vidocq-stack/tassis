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
 * Jakarta Messaging 3.1 provider.
 */
module io.vidocq.tassis.core {
    requires transitive io.vidocq.tassis.api;

    exports io.vidocq.tassis.core;

    // The network transport reaches the broker and the message model, and nobody else does.
    exports io.vidocq.tassis.core.internal.broker to io.vidocq.tassis.amqp, io.vidocq.tassis.server;
    exports io.vidocq.tassis.core.internal.message to io.vidocq.tassis.amqp, io.vidocq.tassis.server;
    exports io.vidocq.tassis.core.internal.selector to io.vidocq.tassis.amqp, io.vidocq.tassis.server;
    exports io.vidocq.tassis.core.internal.settings to io.vidocq.tassis.amqp;

    // A pool lends connections it opens itself, and reaches no other internal package.
    exports io.vidocq.tassis.core.internal.pool to io.vidocq.tassis.pool;

    // A broker of the network is reached by a transport another Tassis module provides.
    uses io.vidocq.tassis.core.internal.broker.Transport;
}
