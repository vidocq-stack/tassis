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
package io.vidocq.tassis.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Authentication of a client when it creates a connection (Jakarta Messaging 3.1
 * §6.1.1): the user registry of a broker, and the {@code JMSSecurityException}
 * that reports rejected credentials (§10.3).
 */
class AuthenticationTest {

    private static final AtomicInteger BROKERS = new AtomicInteger();

    private String brokerName;
    private TassisBroker broker;
    private TassisConnectionFactory factory;

    @BeforeEach
    void aBrokerOfItsOwn() {
        brokerName = "authentication-" + BROKERS.incrementAndGet();
        broker = TassisBroker.named(brokerName);
        factory = new TassisConnectionFactory(brokerName);
    }

    @Test
    void aBrokerWithoutUsersAcceptsAnyone() throws JMSException {
        assertFalse(broker.isSecured());

        try (Connection anonymous = factory.createConnection();
                Connection anyone = factory.createConnection("anyone", "anything")) {
            anonymous.start();
            anyone.start();
        }
    }

    @Test
    void aDeclaredUserConnectsWithItsPassword() throws JMSException {
        broker.addUser("alice", "secret");

        assertTrue(broker.isSecured());
        assertEquals(Set.of("alice"), broker.users());
        try (Connection connection = factory.createConnection("alice", "secret")) {
            connection.start();
        }
    }

    @Test
    void aWrongPasswordIsRejected() {
        broker.addUser("alice", "secret");

        assertThrows(JMSSecurityException.class, () -> factory.createConnection("alice", "wrong"));
        assertThrows(JMSSecurityException.class, () -> factory.createConnection("alice", null));
        assertThrows(JMSSecurityException.class, () -> factory.createConnection("alice", ""));
    }

    @Test
    void anUnknownUserIsRejected() {
        broker.addUser("alice", "secret");

        assertThrows(JMSSecurityException.class, () -> factory.createConnection("invalid", "invalid"));
    }

    @Test
    void theRejectionDoesNotSayWhichOfTheTwoWasWrong() {
        broker.addUser("alice", "secret");

        JMSSecurityException wrongPassword =
                assertThrows(JMSSecurityException.class, () -> factory.createConnection("alice", "wrong"));
        JMSSecurityException unknownUser =
                assertThrows(JMSSecurityException.class, () -> factory.createConnection("bob", "wrong"));

        assertEquals(wrongPassword.getMessage().replace("alice", "bob"), unknownUser.getMessage());
    }

    @Test
    void everyKindOfConnectionIsAuthenticated() throws JMSException {
        broker.addUser("alice", "secret");

        assertThrows(JMSSecurityException.class, () -> factory.createQueueConnection("alice", "wrong"));
        assertThrows(JMSSecurityException.class, () -> factory.createTopicConnection("alice", "wrong"));
        factory.createQueueConnection("alice", "secret").close();
        factory.createTopicConnection("alice", "secret").close();
    }

    @Test
    void aSecuredBrokerRejectsAConnectionWithoutCredentials() {
        broker.addUser("alice", "secret");

        assertThrows(JMSSecurityException.class, () -> factory.createConnection());
        assertThrows(JMSSecurityException.class, () -> factory.createQueueConnection());
        assertThrows(JMSSecurityException.class, () -> factory.createTopicConnection());
        assertThrows(JMSSecurityException.class, () -> factory.createConnection(null, null));
    }

    @Test
    void anonymousConnectionsCanBeAllowedOnASecuredBroker() throws JMSException {
        broker.addUser("alice", "secret").allowAnonymous(true);

        assertTrue(broker.isAnonymousAllowed());
        factory.createConnection().close();
        assertThrows(JMSSecurityException.class, () -> factory.createConnection("invalid", "invalid"),
                "allowing anonymous connections does not make wrong credentials right");
    }

    @Test
    void theSimplifiedApiReportsARejectionUnchecked() {
        broker.addUser("alice", "secret");

        assertThrows(JMSSecurityRuntimeException.class, () -> factory.createContext("invalid", "invalid"));
        assertThrows(JMSSecurityRuntimeException.class,
                () -> factory.createContext("invalid", "invalid", JMSContext.CLIENT_ACKNOWLEDGE));
        assertThrows(JMSSecurityRuntimeException.class, () -> factory.createContext());
        assertThrows(JMSSecurityRuntimeException.class, () -> factory.createContext(JMSContext.SESSION_TRANSACTED));
        assertDoesNotThrow(() -> factory.createContext("alice", "secret").close());
        assertDoesNotThrow(() -> factory.createContext("alice", "secret", JMSContext.SESSION_TRANSACTED).close());
    }

    @Test
    void addingAUserAgainChangesItsPassword() throws JMSException {
        broker.addUser("alice", "secret").addUser("alice", "changed");

        assertEquals(Set.of("alice"), broker.users());
        assertThrows(JMSSecurityException.class, () -> factory.createConnection("alice", "secret"));
        factory.createConnection("alice", "changed").close();
    }

    @Test
    void aRemovedUserNoLongerConnectsAndTheBrokerStaysSecured() {
        broker.addUser("alice", "secret");

        assertTrue(broker.removeUser("alice"));
        assertFalse(broker.removeUser("alice"));

        assertEquals(Set.of(), broker.users());
        assertTrue(broker.isSecured(), "removing the last user must not open the broker to anyone");
        assertThrows(JMSSecurityException.class, () -> factory.createConnection("alice", "secret"));
        assertThrows(JMSSecurityException.class, () -> factory.createConnection());
    }

    @Test
    void aConnectionAlreadyOpenOutlivesItsUser() throws JMSException {
        broker.addUser("alice", "secret");

        try (Connection connection = factory.createConnection("alice", "secret")) {
            broker.removeUser("alice");

            assertDoesNotThrow(() -> connection.createSession().close());
        }
    }

    @Test
    void aRejectedConnectionDoesNotTakeItsClientIdentifier() throws JMSException {
        broker.addUser("alice", "secret");
        TassisConnectionFactory identified = factory.withClientID("desk");

        assertThrows(JMSSecurityException.class, () -> identified.createConnection("alice", "wrong"));

        try (Connection connection = identified.createConnection("alice", "secret")) {
            assertEquals("desk", connection.getClientID());
        }
    }

    @Test
    void usersBelongToOneBroker() throws JMSException {
        broker.addUser("alice", "secret");
        String other = brokerName + "-other";

        assertFalse(TassisBroker.named(other).isSecured());
        new TassisConnectionFactory(other).createConnection("invalid", "invalid").close();
        assertEquals(Set.of("alice"), TassisBroker.named(brokerName).users(),
                "two handles of the same name administer the same broker");
    }

    @Test
    void aUserNeedsANameAndAPassword() {
        assertThrows(NullPointerException.class, () -> broker.addUser(null, "secret"));
        assertThrows(NullPointerException.class, () -> broker.addUser("alice", null));
        assertThrows(IllegalArgumentException.class, () -> broker.addUser("", "secret"));
        assertThrows(IllegalArgumentException.class, () -> broker.addUser("alice", ""));
        assertFalse(broker.isSecured(), "a refused declaration secures nothing");
    }
}
