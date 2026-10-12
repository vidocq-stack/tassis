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

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The container-managed {@code JMSContext} objects of one request (Jakarta Messaging 3.1
 * §12.4.4): each is created the first time it is used in the request, shared by the injection
 * points that ask for the same thing, and closed when the request ends.
 *
 * <p>A request may run on several threads at once, its structured subtasks for instance, which
 * inherit its scope: the contexts are guarded by a lock. That makes the bookkeeping safe, not
 * the contexts themselves, which remain objects for one thread at a time (§2.14).
 */
@RequestScoped
public class RequestContexts {

    @Inject
    Instance<ConnectionFactory> factories;

    @Inject
    SettingsFactories settings;

    private static final System.Logger LOG = System.getLogger(RequestContexts.class.getName());

    /** Guards {@link #contexts}. Held while a context is created, so that only one is per key. */
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<ContextKey, JMSContext> contexts = new LinkedHashMap<>();

    /** Returns the context of this request for that key, creating it on first use. */
    JMSContext get(ContextKey key) {
        lock.lock();
        try {
            JMSContext context = contexts.get(key);
            if (context == null) {
                context = create(key);
                contexts.put(key, context);
            }
            return context;
        } finally {
            lock.unlock();
        }
    }

    private JMSContext create(ContextKey key) {
        ConnectionFactory factory = factory(key.factory());
        return key.userName() == null
                ? factory.createContext(key.sessionMode())
                : factory.createContext(key.userName(), key.password(), key.sessionMode());
    }

    /**
     * The connection factory of that name, or the default one for {@code null}: a bean the
     * application supplies, and otherwise the factory the settings describe
     * ({@link SettingsFactories}).
     *
     * <p>A named factory is the {@code ConnectionFactory} bean with {@code @Named(name)}. The
     * default factory is the {@code ConnectionFactory} bean without {@code @Named}: a bean that
     * only has {@code @Named} also has {@code @Default} for CDI, but it was given a name to be
     * asked for by that name.
     */
    ConnectionFactory factory(String name) {
        List<Instance.Handle<ConnectionFactory>> supplied = new ArrayList<>();
        for (Instance.Handle<ConnectionFactory> handle : factories.select(Any.Literal.INSTANCE).handles()) {
            String beanName = null;
            for (Annotation qualifier : handle.getBean().getQualifiers()) {
                if (qualifier instanceof Named named) {
                    beanName = named.value();
                }
            }
            if (Objects.equals(beanName, name)) {
                supplied.add(handle);
            }
        }
        if (supplied.size() > 1) {
            throw new JMSRuntimeException("several ConnectionFactory beans match "
                    + (name == null ? "the default connection factory" : "@JMSConnectionFactory(\"" + name + "\")"));
        }
        if (supplied.size() == 1) {
            return supplied.get(0).get();
        }
        return settings.named(name);
    }

    /**
     * Closes the contexts of the request. A context that fails to close is reported in the log
     * and does not keep the others open; nothing is thrown, as a container ignores what a
     * {@code @PreDestroy} method throws.
     */
    @PreDestroy
    void close() {
        List<JMSContext> toClose;
        lock.lock();
        try {
            toClose = new ArrayList<>(contexts.values());
            contexts.clear();
        } finally {
            lock.unlock();
        }
        for (JMSContext context : toClose) {
            try {
                context.close();
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "an injected JMSContext failed to close", e);
            }
        }
    }
}
