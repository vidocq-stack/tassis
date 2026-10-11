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
package io.vidocq.tassis.interop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vidocq.tassis.core.TassisBroker;
import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.server.TassisServer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityRuntimeException;
import java.util.UUID;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Apache Qpid JMS over TLS ({@code amqps://}) against a {@code TassisServer} that encrypts
 * (ROADMAP M11.3d-2): TLS from the first octet, on a port of its own, is what third-party
 * clients and brokers of AMQP 1.0 do.
 */
class QpidJmsOverTlsTest {

    private static final long SOON = 5_000;
    private static final TestCertificates LOCALHOST = TestCertificates.issuedTo("localhost");

    private String brokerName;
    private TassisServer server;

    @BeforeEach
    void startServer() {
        brokerName = "qpid-tls-" + UUID.randomUUID();
        server = TassisServer.forBroker(brokerName).port(0).tls(LOCALHOST.server()).start();
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    private JmsConnectionFactory qpid() {
        JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:" + server.port());
        factory.setSslContext(LOCALHOST.client());
        return factory;
    }

    @Test
    void aThirdPartyClientExchangesMessagesOverTls() {
        try (JMSContext context = qpid().createContext()) {
            context.createProducer().send(context.createQueue("orders"), "encrypted");
            assertEquals("encrypted",
                    context.createConsumer(context.createQueue("orders")).receiveBody(String.class, SOON));
        }
    }

    @Test
    void aThirdPartyClientAndATassisClientShareAnEncryptedBroker() {
        TassisConnectionFactory tassis = TassisConnectionFactory.fromUrl("tassis:amqps://localhost:" + server.port())
                .withSslContext(LOCALHOST.client());
        try (JMSContext theirs = qpid().createContext(); JMSContext ours = tassis.createContext()) {
            theirs.createProducer().send(theirs.createQueue("to-tassis"), "from Qpid JMS");
            assertEquals("from Qpid JMS",
                    ours.createConsumer(ours.createQueue("to-tassis")).receiveBody(String.class, SOON));

            ours.createProducer().send(ours.createQueue("to-qpid"), "from Tassis");
            assertEquals("from Tassis",
                    theirs.createConsumer(theirs.createQueue("to-qpid")).receiveBody(String.class, SOON));
        }
    }

    @Test
    void aPasswordIsCheckedOverTlsAsItIsInClear() {
        TassisBroker.named(brokerName).addUser("alice", "secret");

        assertThrows(JMSSecurityRuntimeException.class, () -> qpid().createContext("alice", "wrong"));
        try (JMSContext context = qpid().createContext("alice", "secret")) {
            context.createProducer().send(context.createQueue("orders"), "let in");
        }
    }

    @Test
    void aThirdPartyClientThatDoesNotTrustTheCertificateIsNotServed() {
        // the trust store of the JVM, which knows nothing of this certificate
        JmsConnectionFactory distrustful = new JmsConnectionFactory("amqps://localhost:" + server.port());

        assertThrows(JMSRuntimeException.class, distrustful::createContext);
    }
}
