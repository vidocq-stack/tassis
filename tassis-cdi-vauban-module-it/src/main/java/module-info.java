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
 * An application, as a Java module, that injects {@code JMSContext}. Nothing is opened to the
 * container: the application bean is wired by the provider Vauban's annotation processor
 * generates, declared below.
 */
module io.vidocq.tassis.cdi.moduleit {
    requires io.vidocq.tassis.cdi.vauban;
    requires io.vidocq.vauban.core;
    // For the test on a broker of the network; an application that only connects would require
    // io.vidocq.tassis.amqp instead.
    requires io.vidocq.tassis.server;

    requires jakarta.cdi;
    requires jakarta.inject;

    exports io.vidocq.tassis.cdi.moduleit;

    provides io.vidocq.vauban.api.VaubanComponentProvider
            with io.vidocq.tassis.cdi.moduleit._VaubanComponents;
}
