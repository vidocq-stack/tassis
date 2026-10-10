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
package io.vidocq.tassis.core.internal.broker;

import jakarta.jms.JMSSecurityException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The user registry of a broker, and the check made when a client creates a
 * connection (Jakarta Messaging 3.1 §6.1.1).
 *
 * <p>A broker is open until a first user is declared: it then accepts any
 * credentials, and none. From the first declaration on it is secured, and stays
 * so even if every user is removed.
 *
 * <p>A password is kept as a salted digest: enough not to leave it readable in
 * the memory of the JVM, and nothing more, since nothing is ever written out.
 */
public final class Users {

    private static final SecureRandom RANDOM = new SecureRandom();

    /** What an unknown user's password is compared with, so that the answer takes as long. */
    private static final Credential NOBODY = Credential.of("");

    private final String broker;
    private final ConcurrentHashMap<String, Credential> byName = new ConcurrentHashMap<>();
    private volatile boolean secured;
    private volatile boolean anonymousAllowed;

    private record Credential(byte[] salt, byte[] digest) {

        static Credential of(String password) {
            byte[] salt = new byte[16];
            RANDOM.nextBytes(salt);
            return new Credential(salt, digest(salt, password));
        }

        boolean matches(String password) {
            return MessageDigest.isEqual(digest, digest(salt, password));
        }

        private static byte[] digest(byte[] salt, String password) {
            try {
                MessageDigest sha = MessageDigest.getInstance("SHA-256");
                sha.update(salt);
                return sha.digest(password.getBytes(StandardCharsets.UTF_8));
            } catch (NoSuchAlgorithmException e) {
                // Every Java platform is required to provide SHA-256.
                throw new IllegalStateException(e);
            }
        }
    }

    Users(String broker) {
        this.broker = broker;
    }

    /** Declares a user, or changes the password of one already declared. */
    public void add(String name, String password) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(password, "password");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("a user name must not be empty");
        }
        if (password.isEmpty()) {
            throw new IllegalArgumentException("the password of " + name + " must not be empty");
        }
        byName.put(name, Credential.of(password));
        secured = true;
    }

    /** Removes a user; the connections it has open stay open. */
    public boolean remove(String name) {
        return byName.remove(Objects.requireNonNull(name, "name")) != null;
    }

    public Set<String> names() {
        return Set.copyOf(byName.keySet());
    }

    public boolean isSecured() {
        return secured;
    }

    public boolean isAnonymousAllowed() {
        return anonymousAllowed;
    }

    public void allowAnonymous(boolean allowed) {
        anonymousAllowed = allowed;
    }

    /**
     * Checks the credentials a connection is created with.
     *
     * @param name the user name, or {@code null} for a connection created without credentials
     * @throws JMSSecurityException if the broker is secured and rejects them (§10.3)
     */
    public void check(String name, String password) throws JMSSecurityException {
        if (!secured) {
            return;
        }
        if (name == null) {
            if (!anonymousAllowed) {
                throw new JMSSecurityException(
                        "the broker " + broker + " accepts no connection without a user name and a password");
            }
            return;
        }
        Credential declared = byName.get(name);
        // An unknown user is compared too, and answered in the same words as a wrong password.
        boolean matches = (declared == null ? NOBODY : declared).matches(password == null ? "" : password);
        if (declared == null || !matches) {
            throw new JMSSecurityException(
                    "the broker " + broker + " rejects the user name or the password given for " + name);
        }
    }
}
