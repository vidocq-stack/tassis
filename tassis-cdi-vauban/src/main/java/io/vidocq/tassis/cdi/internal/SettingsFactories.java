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
package io.vidocq.tassis.cdi.internal;

import io.vidocq.tassis.core.TassisConnectionFactory;
import io.vidocq.tassis.pool.TassisConnectionPool;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.ConnectionFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The connection factories the settings describe, for the injection points whose factory the
 * application does not supply as a bean. This is the one place where Tassis reads the system
 * properties by itself: with injection the application does not create the factory, so it
 * cannot hand its settings over.
 *
 * <p>A factory is built the first time it is asked for and kept as long as the application: one
 * that reaches a broker of the network is a {@link TassisConnectionPool}, so that the contexts
 * of the requests, which come and go, share connections that stay. The pools are closed with
 * the application.
 */
@ApplicationScoped
public class SettingsFactories {

    /** Guards the two collections. Held while a factory is built, which connects to nothing. */
    private final ReentrantLock lock = new ReentrantLock();

    /** By name; the default factory under the empty name, which no factory can have. */
    private final Map<String, ConnectionFactory> factories = new HashMap<>();

    private final List<TassisConnectionPool> pools = new ArrayList<>();

    private boolean closed;

    /** The factory of that name, or the default one for {@code null}. */
    ConnectionFactory named(String name) {
        lock.lock();
        try {
            if (closed) {
                // A request that outlives the application must not open a pool nobody would close.
                throw new IllegalStateException("the application is ending: no connection factory any more");
            }
            return factories.computeIfAbsent(name == null ? "" : name, key -> build(name));
        } finally {
            lock.unlock();
        }
    }

    private ConnectionFactory build(String name) {
        TassisConnectionFactory factory = name == null
                ? TassisConnectionFactory.from(System.getProperties())
                : TassisConnectionFactory.named(name, System.getProperties());
        if (!TassisConnectionPool.suits(factory)) {
            return factory;
        }
        TassisConnectionPool pool = TassisConnectionPool.over(factory).build();
        pools.add(pool);
        return pool;
    }

    /** Closes the pools: their idle connections now, the others when their requests end. */
    @PreDestroy
    void close() {
        List<TassisConnectionPool> toClose;
        lock.lock();
        try {
            closed = true;
            toClose = new ArrayList<>(pools);
            pools.clear();
            factories.clear();
        } finally {
            lock.unlock();
        }
        toClose.forEach(TassisConnectionPool::close);
    }
}
