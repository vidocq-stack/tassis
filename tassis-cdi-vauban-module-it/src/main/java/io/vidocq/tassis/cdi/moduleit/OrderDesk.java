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
package io.vidocq.tassis.cdi.moduleit;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.jms.JMSConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSSessionMode;
import jakarta.jms.Queue;

/** An application bean: takes orders on one broker and keeps an audit trail on another. */
@Dependent
public class OrderDesk {

    @Inject
    JMSContext orders;

    @Inject
    @JMSConnectionFactory("audit")
    @JMSSessionMode(JMSContext.CLIENT_ACKNOWLEDGE)
    JMSContext audit;

    public void place(String order) {
        orders.createProducer().send(orders.createQueue("orders"), order);
        audit.createProducer().send(audit.createQueue("trail"), "placed " + order);
    }

    public String nextOrder() {
        Queue queue = orders.createQueue("orders");
        return orders.createConsumer(queue).receiveBody(String.class, 5_000);
    }

    public int auditSessionMode() {
        return audit.getSessionMode();
    }
}
