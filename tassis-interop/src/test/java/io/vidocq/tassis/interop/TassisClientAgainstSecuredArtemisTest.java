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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.TassisConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Queue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The Tassis client against a broker that is not Tassis, as one meets it outside a test: it
 * asks who connects and it encrypts (ROADMAP M11.3e). Apache ActiveMQ Artemis, embedded, with
 * one user and an acceptor over TLS.
 */
@Timeout(40)
class TassisClientAgainstSecuredArtemisTest {

    private static final int PORT = 25_674;
    private static final long SOON = 5_000;

    private static EmbeddedActiveMQ broker;
    private static Path keyStore;
    private static TestCertificates certificate;

    @BeforeAll
    static void startBroker() throws Exception {
        keyStore = Files.createTempFile("tassis-artemis", ".p12");
        certificate = TestCertificates.issuedTo("localhost", keyStore);
        ConfigurationImpl configuration = new ConfigurationImpl();
        configuration.setPersistenceEnabled(false);
        configuration.setSecurityEnabled(true);
        configuration.setJMXManagementEnabled(false);
        configuration.addAcceptorConfiguration("amqps", "tcp://127.0.0.1:" + PORT + "?protocols=AMQP;sslEnabled=true"
                + ";keyStorePath=" + keyStore + ";keyStoreType=PKCS12;keyStorePassword=" + TestCertificates.STORE_PASSWORD);
        broker = new EmbeddedActiveMQ();
        broker.setConfiguration(configuration);
        // One user, who may do anything: what is tested is that the broker knows who connects.
        broker.setSecurityManager(new ActiveMQSecurityManager() {
            @Override
            public boolean validateUser(String user, String password) {
                return "alice".equals(user) && "secret".equals(password);
            }

            @Override
            public boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType) {
                return validateUser(user, password);
            }
        });
        broker.start();
    }

    @AfterAll
    static void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
        Files.deleteIfExists(keyStore);
    }

    private static TassisConnectionFactory tassis() {
        return TassisConnectionFactory.fromUrl("tassis:amqps://localhost:" + PORT).withSslContext(certificate.client());
    }

    @Test
    void aDeclaredUserExchangesMessagesOverTls() {
        try (JMSContext context = tassis().createContext("alice", "secret")) {
            Queue orders = context.createQueue("orders-" + UUID.randomUUID());
            context.createProducer().send(orders, "encrypted");
            assertEquals("encrypted", context.createConsumer(orders).receiveBody(String.class, SOON));
        }
    }

    @Test
    void aWrongPasswordIsRefused() {
        // §6.1.1
        assertThrows(JMSSecurityException.class, () -> tassis().createConnection("alice", "wrong"));
    }

    @Test
    void aClientThatSaysNothingOfItselfIsRefused() {
        assertThrows(JMSSecurityException.class, () -> tassis().createConnection());
    }

    @Test
    void aCertificateTheClientDoesNotTrustIsRefused() {
        TassisConnectionFactory distrustful = TassisConnectionFactory.fromUrl("tassis:amqps://localhost:" + PORT);

        JMSException refused = assertThrows(JMSException.class, () -> distrustful.createConnection("alice", "secret"));

        assertTrue(refused.getMessage().contains("not trusted"), refused.getMessage());
    }
}
