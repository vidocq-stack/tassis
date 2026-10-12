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
package io.vidocq.tassis.core.internal.pool;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.core.internal.broker.Transports;
import io.vidocq.tassis.core.internal.client.TassisContext;
import io.vidocq.tassis.core.internal.client.Unchecked;
import io.vidocq.tassis.core.internal.settings.Settings;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import javax.net.ssl.SSLContext;

/**
 * What a connection factory connects with, for a pool: the pool opens the connections to the
 * broker itself, to keep them. This is how the module of the pool reaches what a
 * {@link TassisConnectionFactory} keeps private, without a public method for it.
 */
public final class ConnectionSource {

    // Set by TassisConnectionFactory when it is loaded: nobody can ask before having a factory.
    private static volatile Function<TassisConnectionFactory, SSLContext> tlsOf;

    private final Settings settings;
    private final SSLContext tls;

    private ConnectionSource(Settings settings, SSLContext tls) {
        this.settings = settings;
        this.tls = tls;
    }

    /**
     * For {@link TassisConnectionFactory} alone, once: how to read the one thing its URL does not
     * say, the context of its encrypted connections.
     *
     * @throws IllegalStateException if it is said already
     */
    public static synchronized void register(Function<TassisConnectionFactory, SSLContext> reader) {
        if (tlsOf != null) {
            throw new IllegalStateException("already registered");
        }
        tlsOf = Objects.requireNonNull(reader, "reader");
    }

    /** What that factory connects with: the settings of its URL, and its TLS context. */
    public static ConnectionSource of(TassisConnectionFactory factory) {
        Objects.requireNonNull(factory, "factory");
        return new ConnectionSource(Settings.fromUrl(factory.toUrl()), tlsOf.apply(factory));
    }

    /** Whether the factory gives its connections a client identifier, so can only have one open. */
    public boolean assignsClientId() {
        return settings.clientId() != null;
    }

    /** Whether the broker is one of the network, where a connection is a socket. */
    public boolean isRemote() {
        return settings.server() != null;
    }

    /** Whether the settings let whoever builds the factories itself pool this one. */
    public boolean poolEnabled() {
        return settings.pool().enabled();
    }

    /** The settings of a pool, as the factory carries them: what a pool starts from. */
    public int poolMaxSize() {
        return settings.pool().maxSize();
    }

    public Duration poolAcquireTimeout() {
        return settings.pool().acquireTimeout();
    }

    /** Zero for never. */
    public Duration poolIdleTimeout() {
        return settings.pool().idleTimeout();
    }

    /** Zero for never. */
    public Duration poolMaxLifetime() {
        return settings.pool().maxLifetime();
    }

    /** Zero for never. */
    public Duration poolLeakThreshold() {
        return settings.pool().leakThreshold();
    }

    /**
     * Opens a connection to the broker.
     *
     * @throws JMSException if the broker rejects the credentials or cannot be reached
     */
    public PhysicalConnection open(String userName, String password) throws JMSException {
        return new PhysicalConnection(Transports.connect(settings, tls, userName, password),
                settings.maxListenerDeliveries());
    }

    /** Checks a session mode before a connection is borrowed for it. */
    public static void checkSessionMode(int sessionMode) {
        TassisContext.checkSessionMode(sessionMode);
    }

    /** The unchecked counterpart of a checked exception, as the simplified API throws it. */
    public static JMSRuntimeException unchecked(JMSException e) {
        return Unchecked.of(e);
    }

    /** The connection URL of the factory. */
    @Override
    public String toString() {
        return settings.toUrl();
    }
}
