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

import io.vidocq.tassis.core.internal.broker.Broker;
import io.vidocq.tassis.core.internal.broker.Users;
import java.util.Objects;
import java.util.Set;

/**
 * The administration of a broker: today, its user registry (Jakarta Messaging 3.1
 * §6.1.1 leaves authentication, and the way users are declared, to the provider).
 *
 * <p>A broker is identified by its name within the JVM, as for a
 * {@link TassisConnectionFactory}: {@code TassisBroker.named("orders")} administers
 * the broker that {@code new TassisConnectionFactory("orders")} connects to, and
 * creates it if nothing has used it yet.
 *
 * <p>A broker is <em>open</em> until a first user is declared: it accepts any user
 * name and password, and connections created without any. Declaring a user
 * <em>secures</em> it, for good: a connection then needs the name and the password
 * of a declared user, or is created without credentials on a broker that
 * {@linkplain #allowAnonymous(boolean) allows anonymous connections}. A rejected
 * connection fails with {@code JMSSecurityException}, or with
 * {@code JMSSecurityRuntimeException} in the simplified API.
 *
 * <pre>{@code
 * TassisBroker.named("orders").addUser("billing", password);
 * Connection connection = new TassisConnectionFactory("orders").createConnection("billing", password);
 * }</pre>
 *
 * <p>Users are held in memory, like everything else in a broker: they are to be
 * declared again each time the application starts. Authentication says who may
 * connect; it gives no user different rights from another's.
 */
public final class TassisBroker {

    private final String name;
    private final Users users;

    private TassisBroker(String name, Users users) {
        this.name = name;
        this.users = users;
    }

    /** Returns the administration of the broker of that name in this JVM. */
    public static TassisBroker named(String name) {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("a broker name must not be empty");
        }
        return new TassisBroker(name, Broker.named(name).users());
    }

    /** The name of the broker. */
    public String name() {
        return name;
    }

    /**
     * Declares a user, which secures the broker, or changes the password of a user already
     * declared.
     *
     * @return this, to chain declarations
     * @throws IllegalArgumentException if the name or the password is empty
     */
    public TassisBroker addUser(String userName, String password) {
        users.add(userName, password);
        return this;
    }

    /**
     * Removes a user. Its open connections stay open, and the broker stays secured even without
     * any user left.
     *
     * @return whether such a user was declared
     */
    public boolean removeUser(String userName) {
        return users.remove(userName);
    }

    /** The names of the declared users. */
    public Set<String> users() {
        return users.names();
    }

    /** Whether a user has ever been declared, so that credentials are checked. */
    public boolean isSecured() {
        return users.isSecured();
    }

    /**
     * Says whether a secured broker accepts the connections created without a user name and a
     * password. It does not by default. An open broker accepts them whatever is said here.
     *
     * @return this, to chain declarations
     */
    public TassisBroker allowAnonymous(boolean allowed) {
        users.allowAnonymous(allowed);
        return this;
    }

    /** Whether a secured broker accepts the connections created without credentials. */
    public boolean isAnonymousAllowed() {
        return users.isAnonymousAllowed();
    }

    @Override
    public String toString() {
        return "TassisBroker[" + name + "]";
    }
}
