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
 * https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12 *
 * SPDX-License-Identifier: EPL-2.0 OR EUPL-1.2 OR GPL-2.0-or-later
 */
package io.vidocq.tassis.core.internal.broker;

import io.vidocq.tassis.core.internal.settings.Settings;
import jakarta.jms.JMSException;

/**
 * A way to reach a broker that is not in this JVM. {@code tassis-core} knows none: a transport
 * is a service another Tassis module provides, found when a connection factory names it.
 */
public interface Transport {

    /** The name of the transport in a connection URL: {@code amqp} in {@code tassis:amqp://host}. */
    String name();

    /**
     * Connects to the broker the settings name, as this user.
     *
     * @param user the user name, or {@code null} for none
     * @throws jakarta.jms.JMSSecurityException if the broker rejects the credentials (§6.1.1)
     * @throws JMSException if the broker cannot be reached
     */
    BrokerConnection connect(Settings settings, String user, String password) throws JMSException;
}
