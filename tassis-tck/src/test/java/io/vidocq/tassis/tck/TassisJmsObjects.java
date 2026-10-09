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
package io.vidocq.tassis.tck;

import com.sun.ts.lib.porting.TSJMSObjectsInterface;
import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.core.TassisQueue;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnectionFactory;

/**
 * The TCK's porting class: hands the test suite the administered objects it asks
 * for by name. Selected by the system property {@code porting.ts.jmsObjects.class.1}.
 * Every factory connects to the default in-VM broker, and a queue is whatever name
 * the TCK asks for, since Tassis creates queues on first use.
 */
public final class TassisJmsObjects implements TSJMSObjectsInterface {

    @Override
    public Queue getQueue(String name) {
        return new TassisQueue(name);
    }

    @Override
    public Topic getTopic(String name) {
        throw new UnsupportedOperationException("topics: see ROADMAP M4");
    }

    @Override
    public TopicConnectionFactory getTopicConnectionFactory(String name) {
        throw new UnsupportedOperationException("topics: see ROADMAP M4");
    }

    @Override
    public QueueConnectionFactory getQueueConnectionFactory(String name) {
        return new TassisConnectionFactory();
    }

    @Override
    public ConnectionFactory getConnectionFactory(String name) {
        return new TassisConnectionFactory();
    }
}
