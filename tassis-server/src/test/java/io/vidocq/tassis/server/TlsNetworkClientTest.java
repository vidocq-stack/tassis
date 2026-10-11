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
package io.vidocq.tassis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.TassisConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import java.io.InputStream;
import java.net.Socket;
import java.time.Duration;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.Test;

/**
 * The Tassis client through an encrypted connection ({@code tassis:amqps://}; ROADMAP M11.3d-2):
 * everything {@link NetworkClientTest} checks in clear, and what TLS adds, which is that a
 * client only talks to a broker whose certificate it trusts and that bears the name it asked for
 * (RFC 2818).
 */
class TlsNetworkClientTest extends NetworkClientTest {

    private static final TestCertificates LOCALHOST = TestCertificates.issuedTo("localhost");
    private static final TestCertificates ELSEWHERE = TestCertificates.issuedTo("elsewhere.example");

    @Override
    TassisServer.Builder configured(TassisServer.Builder server) {
        return server.tls(LOCALHOST.server());
    }

    @Override
    TassisConnectionFactory factoryFor(int port) {
        return TassisConnectionFactory.fromUrl("tassis:amqps://localhost:" + port).withSslContext(LOCALHOST.client());
    }

    @Test
    void whatTravelsIsEncrypted() throws Exception {
        try (SSLSocket socket = (SSLSocket) LOCALHOST.client().getSocketFactory().createSocket("localhost",
                server.port())) {
            socket.startHandshake();
            assertTrue(socket.getSession().getProtocol().startsWith("TLSv1."), socket.getSession().getProtocol());
        }
    }

    @Test
    void aBrokerWhoseCertificateIsNotTrustedIsRefused() {
        // the JVM's own trust store knows nothing of a certificate made a moment ago
        TassisConnectionFactory distrustful = TassisConnectionFactory.fromUrl("tassis:amqps://localhost:" + server.port());

        JMSException refused = assertThrows(JMSException.class, distrustful::createConnection);

        assertTrue(refused.getMessage().contains("not trusted"), refused.getMessage());
    }

    @Test
    void aBrokerWhoseCertificateBearsAnotherNameIsRefused() {
        try (TassisServer other = TassisServer.forBroker(server.brokerName()).port(0).tls(ELSEWHERE.server()).start()) {
            // trusted, and issued to elsewhere.example: not to the localhost the client asks for
            TassisConnectionFactory misled = TassisConnectionFactory.fromUrl("tassis:amqps://localhost:" + other.port())
                    .withSslContext(ELSEWHERE.client());

            JMSException refused = assertThrows(JMSException.class, misled::createConnection);

            assertTrue(refused.getMessage().contains("not trusted"), refused.getMessage());
        }
    }

    @Test
    void aClientThatSpeaksInClearToAnEncryptedPortIsTurnedAway() {
        TassisConnectionFactory inClear = TassisConnectionFactory.fromUrl("tassis:amqp://localhost:" + server.port());

        assertTimeoutPreemptively(Duration.ofSeconds(20),
                () -> assertThrows(JMSException.class, inClear::createConnection));

        // and the server serves the next one
        try (JMSContext context = network.createContext()) {
            context.createProducer().send(context.createQueue("orders"), "still there");
            assertEquals("still there", context.createConsumer(context.createQueue("orders")).receiveBody(String.class, 5_000));
        }
    }

    @Test
    void aClientThatWantsEncryptionRefusesAPortInClear() {
        try (TassisServer inClear = TassisServer.forBroker(server.brokerName()).port(0).start()) {
            TassisConnectionFactory encrypting = TassisConnectionFactory.fromUrl("tassis:amqps://localhost:" + inClear.port())
                    .withSslContext(LOCALHOST.client());

            JMSException refused = assertTimeoutPreemptively(Duration.ofSeconds(20),
                    () -> assertThrows(JMSException.class, encrypting::createConnection));

            assertTrue(refused.getMessage().contains("TLS"), refused.getMessage());
        }
    }

    @Test
    void aClientThatConnectsAndSaysNothingDoesNotHoldTheServerUp() throws Exception {
        // the TLS handshake is the business of the thread that serves the connection
        try (Socket silent = new Socket("localhost", server.port()); JMSContext context = network.createContext()) {
            context.createProducer().send(context.createQueue("orders"), "served meanwhile");
            assertEquals("served meanwhile",
                    context.createConsumer(context.createQueue("orders")).receiveBody(String.class, 5_000));
            InputStream in = silent.getInputStream();
            silent.setSoTimeout(15_000);
            // given up after the time a client has to open a connection: an alert at most, then the end
            assertTrue(in.readAllBytes().length < 64);
        }
    }

    @Test
    void aClientThatStopsReadingDoesNotKeepTheServerFromClosing() throws Exception {
        try (JMSContext sender = inVmContext()) {
            for (int i = 0; i < 40; i++) {
                sender.createProducer().send(sender.createQueue("flood"), new byte[1024 * 1024]);
            }
        }
        SSLSocket socket = (SSLSocket) LOCALHOST.client().getSocketFactory().createSocket("localhost", server.port());
        try (Wire wire = new Wire(socket).ready()) {
            // asks for everything, and reads none of it: the server ends up waiting to write
            wire.attachReceiver(0, "flood");
            wire.credit(0, 0, 100, null);
            Thread.sleep(1_000);

            // closing an encrypted socket says goodbye first, which must not wait for that write
            assertTimeoutPreemptively(Duration.ofSeconds(20), server::close);
        }
    }
}
