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

import jakarta.enterprise.inject.spi.Annotated;
import jakarta.jms.JMSConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSPasswordCredential;
import jakarta.jms.JMSSessionMode;

/**
 * What the annotations of an injection point ask for (Jakarta Messaging 3.1 §12.4.3). Within a
 * scope, injection points with equal keys get the same {@code JMSContext} (§12.4.5).
 *
 * @param factory the name given by {@code @JMSConnectionFactory}, or {@code null} for the default
 *        connection factory
 * @param userName the user name given by {@code @JMSPasswordCredential}, or {@code null}
 * @param password the password given by {@code @JMSPasswordCredential}, or {@code null}
 * @param sessionMode the mode given by {@code @JMSSessionMode}, {@code AUTO_ACKNOWLEDGE} without it
 */
record ContextKey(String factory, String userName, String password, int sessionMode) {

    static ContextKey of(Annotated injectionPoint) {
        JMSConnectionFactory factory = injectionPoint.getAnnotation(JMSConnectionFactory.class);
        JMSPasswordCredential credential = injectionPoint.getAnnotation(JMSPasswordCredential.class);
        JMSSessionMode mode = injectionPoint.getAnnotation(JMSSessionMode.class);
        return new ContextKey(
                factory == null ? null : factory.value(),
                credential == null ? null : credential.userName(),
                credential == null ? null : credential.password(),
                mode == null ? JMSContext.AUTO_ACKNOWLEDGE : mode.value());
    }
}
