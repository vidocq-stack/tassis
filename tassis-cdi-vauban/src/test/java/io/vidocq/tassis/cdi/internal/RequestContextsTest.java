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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.vidocq.tassis.core.TassisConnectionFactory;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * The contexts of one request (Jakarta Messaging 3.1 §12.4.4), without a container: what must
 * hold when a request runs on several threads, as its structured subtasks do, and when a
 * context fails to close.
 */
class RequestContextsTest {

    private static final ContextKey DEFAULT = new ContextKey(null, null, null, JMSContext.AUTO_ACKNOWLEDGE);
    private static final ContextKey AUDIT = new ContextKey("audit", null, null, JMSContext.AUTO_ACKNOWLEDGE);

    /** A connection factory whose contexts come from a supplier. Connections are not used here. */
    private record Contexts(Supplier<JMSContext> supplier) implements ConnectionFactory {

        @Override
        public JMSContext createContext() {
            return supplier.get();
        }

        @Override
        public JMSContext createContext(int sessionMode) {
            return supplier.get();
        }

        @Override
        public JMSContext createContext(String userName, String password) {
            return supplier.get();
        }

        @Override
        public JMSContext createContext(String userName, String password, int sessionMode) {
            return supplier.get();
        }

        @Override
        public Connection createConnection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection createConnection(String userName, String password) {
            throw new UnsupportedOperationException();
        }
    }

    private static RequestContexts requestWith(ConnectionFactory factory) {
        return new RequestContexts() {
            @Override
            ConnectionFactory factory(String name) {
                return factory;
            }
        };
    }

    /** A context that records its close, and fails it when asked to. */
    private static JMSContext context(String name, List<String> closed, RuntimeException closeFailure) {
        return (JMSContext) Proxy.newProxyInstance(RequestContextsTest.class.getClassLoader(),
                new Class<?>[] {JMSContext.class}, (proxy, method, arguments) -> {
                    if (!method.getName().equals("close")) {
                        throw new UnsupportedOperationException(method.getName());
                    }
                    closed.add(name);
                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                    return null;
                });
    }

    @Test
    void threadsOfOneRequestGetOneContextForOneKey() throws Exception {
        TassisConnectionFactory real = new TassisConnectionFactory("request-" + UUID.randomUUID());
        AtomicInteger created = new AtomicInteger();
        RequestContexts request = requestWith(new Contexts(() -> {
            created.incrementAndGet();
            pause(50); // long enough for every thread to arrive while the first one creates
            return real.createContext();
        }));
        int threads = 16;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        Set<JMSContext> seen = ConcurrentHashMap.newKeySet();
        List<Thread> started = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            started.add(Thread.ofVirtual().start(() -> {
                ready.countDown();
                await(go);
                seen.add(request.get(DEFAULT));
            }));
        }
        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        for (Thread thread : started) {
            thread.join(5_000);
        }
        try {
            assertEquals(1, created.get(), "one context is created, so none is left unclosed");
            assertEquals(1, seen.size());
        } finally {
            seen.forEach(JMSContext::close);
        }
    }

    @Test
    void contextThatFailsToCloseDoesNotKeepTheOthersOpen() {
        List<String> closed = new ArrayList<>();
        List<JMSContext> toCreate = new ArrayList<>(List.of(
                context("first", closed, new IllegalStateException("not a JMSRuntimeException")),
                context("second", closed, null)));
        RequestContexts request = requestWith(new Contexts(() -> toCreate.remove(0)));
        JMSContext first = request.get(DEFAULT);
        request.get(AUDIT);
        assertSame(first, request.get(DEFAULT));

        request.close(); // what the container calls when the request ends: it must not throw

        assertEquals(List.of("first", "second"), closed);
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
