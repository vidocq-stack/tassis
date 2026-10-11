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
import java.util.ServiceLoader;

/** Where a connection factory finds the broker its settings name. */
public final class Transports {

    private Transports() {
    }

    /**
     * Connects to the broker of the settings: the one of this JVM, or one of the network
     * through the transport that serves it.
     *
     * @throws JMSException if no module on the module path provides that transport, or the
     *         broker rejects the credentials or cannot be reached
     */
    public static BrokerConnection connect(Settings settings, String user, String password) throws JMSException {
        String wanted = settings.serverTransport();
        if (wanted == null) {
            return Broker.named(settings.broker()).connect(user, password);
        }
        for (Transport transport : ServiceLoader.load(Transport.class)) {
            if (transport.name().equals(wanted)) {
                return transport.connect(settings, user, password);
            }
        }
        // A module that only provides a service is not resolved because somebody uses that service:
        // on the module path the application has to name it.
        throw new JMSException("no transport for " + settings.server() + ": the artifact tassis-" + wanted
                + " is missing, or its module io.vidocq.tassis." + wanted
                + " is not resolved (requires it, or --add-modules)");
    }
}
