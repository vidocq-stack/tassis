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

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.jms.JMSContext;

/**
 * Produces what {@code @Inject JMSContext} receives (Jakarta Messaging 3.1 §12.4.3).
 *
 * <p>{@code @JMSConnectionFactory}, {@code @JMSPasswordCredential} and {@code @JMSSessionMode}
 * are not CDI qualifiers, so one producer serves every injection point and reads them there.
 */
@Dependent
public class JmsContextProducer {

    /**
     * @param contexts the contexts of the request in progress, through their client proxy: what
     *        is produced here therefore follows the request of each later call
     */
    @Produces
    @Dependent
    public JMSContext produce(InjectionPoint injectionPoint, RequestContexts contexts) {
        return new InjectedJMSContext(ContextKey.of(injectionPoint.getAnnotated()), contexts);
    }
}
