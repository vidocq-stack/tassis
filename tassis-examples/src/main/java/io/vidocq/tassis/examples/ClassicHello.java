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
package io.vidocq.tassis.examples;

import io.vidocq.tassis.core.TassisConnectionFactory;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

/** Sends a message to a queue and receives it, with the classic API. */
public final class ClassicHello {

    private ClassicHello() {
    }

    public static void main(String[] args) throws JMSException {
        System.out.println(sendAndReceive(new TassisConnectionFactory()));
    }

    static String sendAndReceive(ConnectionFactory factory) throws JMSException {
        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession();
            Queue queue = session.createQueue("orders");
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);
            // A connection is created stopped: its consumers receive nothing until it is started.
            connection.start();

            producer.send(session.createTextMessage("hello"));

            TextMessage received = (TextMessage) consumer.receive(5_000);
            return received.getText();
        }
    }
}
