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
package io.vidocq.tassis.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.TassisBroker;
import io.vidocq.tassis.core.TassisConnectionFactory;
import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.ResourceAllocationRuntimeException;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The pool over the broker of the JVM: what is lent, what a close gives back, and what a
 * borrower finds. Jakarta Messaging does not define pooling; the sections cited are those a
 * borrowed connection must still honour.
 */
class TassisConnectionPoolTest {

    private static final long SOON = 2_000;
    private static final long NOTHING = 200;

    private final String broker = "pool-" + UUID.randomUUID();
    private final TassisConnectionFactory factory = new TassisConnectionFactory(broker);
    private final List<TassisConnectionPool> pools = new ArrayList<>();

    private TassisConnectionPool pool(int maxSize) {
        TassisConnectionPool pool =
                TassisConnectionPool.over(factory).maxSize(maxSize).acquireTimeout(Duration.ofMillis(300)).build();
        pools.add(pool);
        return pool;
    }

    @AfterEach
    void closePools() {
        pools.forEach(TassisConnectionPool::close);
    }

    private void send(String queue, String... texts) {
        try (JMSContext context = factory.createContext()) {
            for (String text : texts) {
                context.createProducer().send(context.createQueue(queue), text);
            }
        }
    }

    private String receive(String queue) {
        try (JMSContext context = factory.createContext()) {
            return context.createConsumer(context.createQueue(queue)).receiveBody(String.class, NOTHING);
        }
    }

    // ---- lending -----------------------------------------------------------

    @Test
    void aPoolOpensNothingUntilAConnectionIsAskedFor() {
        TassisConnectionPool pool = pool(4);

        assertEquals(0, pool.size());
        assertEquals(0, pool.idle());
    }

    @Test
    void aClosedConnectionGoesBackToThePoolAndIsLentAgain() throws JMSException {
        TassisConnectionPool pool = pool(4);

        for (int i = 0; i < 5; i++) {
            try (Connection connection = pool.createConnection()) {
                assertEquals(1, pool.size());
                assertEquals(0, pool.idle());
                Session session = connection.createSession();
                session.createProducer(session.createQueue("orders")).send(session.createTextMessage("n" + i));
            }
            assertEquals(1, pool.size(), "the connection is kept");
            assertEquals(1, pool.idle());
        }
        assertEquals("n0", receive("orders"));
    }

    @Test
    void connectionsBorrowedTogetherAreDistinct() throws JMSException {
        TassisConnectionPool pool = pool(4);

        try (Connection first = pool.createConnection(); Connection second = pool.createConnection()) {
            assertEquals(2, pool.size());
            // §6.1.2: one client identifier, one connection. Two that take one each are two.
            first.setClientID("one");
            second.setClientID("two");
        }
    }

    @Test
    void aPoolIsAFactoryOfTheLegacyDomainsToo() throws JMSException {
        TassisConnectionPool pool = pool(1);

        try (QueueConnection queues = pool.createQueueConnection()) {
            assertNotNull(queues.createQueueSession(false, Session.AUTO_ACKNOWLEDGE));
        }
        try (TopicConnection topics = pool.createTopicConnection()) {
            assertNotNull(topics.createTopicSession(false, Session.AUTO_ACKNOWLEDGE));
        }
        assertEquals(1, pool.size(), "the domain belongs to what is lent, not to what is kept");
    }

    // ---- what a borrower finds ---------------------------------------------

    @Test
    void aBorrowedConnectionIsStoppedWhateverTheOneBeforeDid() throws JMSException {
        TassisConnectionPool pool = pool(1);
        send("orders", "waiting");
        try (Connection before = pool.createConnection()) {
            before.start();
        }

        try (Connection connection = pool.createConnection()) {
            MessageConsumer consumer = connection.createSession().createConsumer(queue(connection, "orders"));
            // §6.1.4: a connection is created stopped, and delivers nothing until started
            assertNull(consumer.receive(NOTHING));
            connection.start();
            assertNotNull(consumer.receive(SOON));
        }
    }

    private static Queue queue(Connection connection, String name) throws JMSException {
        return connection.createSession().createQueue(name);
    }

    @Test
    void whatTheOneBeforeDidNotAcknowledgeIsDeliveredAgain() throws JMSException {
        TassisConnectionPool pool = pool(1);
        send("orders", "unacknowledged");
        try (Connection before = pool.createConnection()) {
            before.start();
            Session session = before.createSession(Session.CLIENT_ACKNOWLEDGE);
            assertNotNull(session.createConsumer(session.createQueue("orders")).receive(SOON));
            // §6.1.8: closing a connection does not acknowledge
        }

        try (Connection connection = pool.createConnection()) {
            connection.start();
            Session session = connection.createSession();
            Message again = session.createConsumer(session.createQueue("orders")).receive(SOON);
            assertEquals("unacknowledged", ((TextMessage) again).getText());
            assertTrue(again.getJMSRedelivered());
        }
    }

    @Test
    void theTransactionOfTheOneBeforeIsRolledBack() throws JMSException {
        TassisConnectionPool pool = pool(1);
        send("orders", "consumed in a transaction");
        try (Connection before = pool.createConnection()) {
            before.start();
            Session session = before.createSession(Session.SESSION_TRANSACTED);
            session.createProducer(session.createQueue("invoices")).send(session.createTextMessage("never sent"));
            assertNotNull(session.createConsumer(session.createQueue("orders")).receive(SOON));
            // §6.1.8: closing a connection rolls back the transactions in progress
        }

        assertNull(receive("invoices"));
        assertEquals("consumed in a transaction", receive("orders"));
    }

    @Test
    void theConsumersOfTheOneBeforeAreGone() throws JMSException {
        TassisConnectionPool pool = pool(1);
        try (Connection before = pool.createConnection()) {
            before.start();
            Session session = before.createSession();
            session.createConsumer(session.createQueue("orders")).setMessageListener(message -> { });
            Topic topic = session.createTopic("news");
            session.createConsumer(topic);
        }
        send("orders", "for whoever asks");

        // nothing is left to take it behind the back of the pool
        assertEquals("for whoever asks", receive("orders"));
    }

    @Test
    void theTemporaryDestinationsOfTheOneBeforeAreDeleted() throws JMSException {
        TassisConnectionPool pool = pool(1);
        TemporaryQueue left;
        try (Connection before = pool.createConnection()) {
            Session session = before.createSession();
            left = session.createTemporaryQueue();
            session.createTemporaryTopic();
            session.createTemporaryQueue().delete();
        }

        // §6.2.2: a temporary destination lasts as long as its connection, which the application closed
        try (Connection other = factory.createConnection()) {
            Session session = other.createSession();
            assertThrows(JMSException.class, () -> session.createProducer(left).send(session.createTextMessage("lost")));
        }
        try (Connection connection = pool.createConnection()) {
            Session session = connection.createSession();
            assertThrows(JMSException.class, () -> session.createConsumer(left));
        }
    }

    @Test
    void theExceptionListenerOfTheOneBeforeIsGone() throws JMSException {
        TassisConnectionPool pool = pool(1);
        try (Connection before = pool.createConnection()) {
            before.setExceptionListener(problem -> { });
        }

        try (Connection connection = pool.createConnection()) {
            assertNull(connection.getExceptionListener());
        }
    }

    @Test
    void aConnectionGivenBackIsClosedForWhoeverKeptIt() throws JMSException {
        TassisConnectionPool pool = pool(1);
        Connection kept = pool.createConnection();
        Session session = kept.createSession();
        kept.close();

        try (Connection connection = pool.createConnection()) {
            connection.start();
            // §6.1.8: a closed connection is of no use, and here it must not act on its successor
            assertThrows(IllegalStateException.class, kept::stop);
            assertThrows(IllegalStateException.class, kept::createSession);
            assertThrows(IllegalStateException.class, () -> session.createQueue("orders"));
            kept.close();
            kept.close();

            send("orders", "still delivered");
            Session mine = connection.createSession();
            assertNotNull(mine.createConsumer(mine.createQueue("orders")).receive(SOON),
                    "closing the old connection again has not stopped the new one");
            assertEquals(1, pool.size());
            assertEquals(0, pool.idle(), "nor given it back");
        }
    }

    // ---- client identifiers ------------------------------------------------

    @Test
    void aConnectionGivenAClientIdentifierIsClosedForGood() throws JMSException {
        TassisConnectionPool pool = pool(2);

        try (Connection connection = pool.createConnection()) {
            connection.setClientID("billing");
            assertEquals(1, pool.size(), "it counts while it is open");
            // §6.1.2: the identifier is in use
            try (Connection rival = factory.createConnection()) {
                assertThrows(InvalidClientIDException.class, () -> rival.setClientID("billing"));
            }
        }

        assertEquals(0, pool.size(), "it is not kept");
        try (Connection next = pool.createConnection()) {
            assertNull(next.getClientID());
            next.setClientID("billing");
        }
    }

    @Test
    void aConnectionRefusedAClientIdentifierIsNotKeptEither() throws JMSException {
        TassisConnectionPool pool = pool(2);

        try (Connection holder = factory.createConnection(); Connection connection = pool.createConnection()) {
            holder.setClientID("billing");
            // §6.1.2: in use. What the refusal left of the connection to the broker is not known.
            assertThrows(InvalidClientIDException.class, () -> connection.setClientID("billing"));
        }

        assertEquals(0, pool.size());
    }

    @Test
    void aFactoryThatAssignsAClientIdentifierIsRefused() {
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> TassisConnectionPool.over(factory.withClientID("billing")));

        assertTrue(refused.getMessage().contains("client identifier"), refused.getMessage());
    }

    // ---- size --------------------------------------------------------------

    @Test
    void aBorrowerOfAnExhaustedPoolWaitsThenFails() throws JMSException {
        TassisConnectionPool pool = pool(2);

        try (Connection first = pool.createConnection(); Connection second = pool.createConnection()) {
            long start = System.nanoTime();
            ResourceAllocationException exhausted =
                    assertThrows(ResourceAllocationException.class, pool::createConnection);

            assertTrue(System.nanoTime() - start >= Duration.ofMillis(300).toNanos(), "it waited");
            assertTrue(exhausted.getMessage().contains("2"), exhausted.getMessage());
            assertThrows(ResourceAllocationRuntimeException.class, pool::createContext);
            assertEquals(2, pool.size());
        }
    }

    @Test
    void aBorrowerThatWaitsGetsTheConnectionGivenBack() throws Exception {
        TassisConnectionPool pool =
                TassisConnectionPool.over(factory).maxSize(1).acquireTimeout(Duration.ofSeconds(10)).build();
        pools.add(pool);
        Connection held = pool.createConnection();

        try (ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Connection> waiting = threads.submit(() -> pool.createConnection());
            assertThrows(java.util.concurrent.TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));

            held.close();

            waiting.get(5, TimeUnit.SECONDS).close();
        }
        assertEquals(1, pool.size());
    }

    @Test
    void manyBorrowersNeverGetMoreConnectionsThanThePoolAllows() throws Exception {
        int maxSize = 3;
        TassisConnectionPool pool =
                TassisConnectionPool.over(factory).maxSize(maxSize).acquireTimeout(Duration.ofSeconds(30)).build();
        pools.add(pool);
        AtomicInteger together = new AtomicInteger();
        AtomicInteger most = new AtomicInteger();
        int borrowers = 200;
        CountDownLatch done = new CountDownLatch(borrowers);
        List<Throwable> failures = java.util.Collections.synchronizedList(new ArrayList<>());

        try (ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < borrowers; i++) {
                String text = "m" + i;
                threads.submit(() -> {
                    try (JMSContext context = pool.createContext()) {
                        most.accumulateAndGet(together.incrementAndGet(), Math::max);
                        context.createProducer().send(context.createQueue("orders"), text);
                        together.decrementAndGet();
                    } catch (Throwable e) {
                        failures.add(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS));
        }

        assertEquals(List.of(), failures);
        assertTrue(most.get() <= maxSize, "at most " + maxSize + " at a time, not " + most.get());
        assertTrue(pool.size() <= maxSize);
        assertEquals(pool.size(), pool.idle(), "all are back");
        try (JMSContext context = factory.createContext()) {
            int received = 0;
            while (context.createConsumer(context.createQueue("orders")).receiveBody(String.class, NOTHING) != null) {
                received++;
            }
            assertEquals(borrowers, received);
        }
    }

    @Test
    void aSizeOrAWaitThatMakesNoSenseIsRefused() {
        TassisConnectionPool.Builder builder = TassisConnectionPool.over(factory);

        assertThrows(IllegalArgumentException.class, () -> builder.maxSize(0));
        assertThrows(IllegalArgumentException.class, () -> builder.acquireTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> builder.acquireTimeout(Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class, () -> builder.acquireTimeout(null));
    }

    // ---- credentials -------------------------------------------------------

    @Test
    void aBorrowerOnlyGetsAConnectionOpenedWithItsCredentials() throws JMSException {
        TassisBroker.named(broker).addUser("alice", "a-secret").addUser("bob", "b-secret");
        TassisConnectionPool pool = pool(4);

        pool.createConnection("alice", "a-secret").close();
        assertEquals(1, pool.idle());

        // §6.1.1: bob is authenticated as bob, not handed what alice opened
        pool.createConnection("bob", "b-secret").close();
        assertEquals(2, pool.size());
        assertThrows(JMSSecurityException.class, () -> pool.createConnection("alice", "wrong"));
        assertThrows(JMSSecurityRuntimeException.class, () -> pool.createContext("bob", "wrong"));
        assertEquals(2, pool.size(), "a refused connection takes no place");

        pool.createConnection("alice", "a-secret").close();
        assertEquals(2, pool.size(), "alice got hers back");
    }

    @Test
    void aFullPoolMakesRoomByClosingAConnectionNobodyUses() throws JMSException {
        TassisBroker.named(broker).addUser("alice", "a-secret").addUser("bob", "b-secret");
        TassisConnectionPool pool = pool(1);
        pool.createConnection("alice", "a-secret").close();

        try (Connection bobs = pool.createConnection("bob", "b-secret")) {
            assertEquals(1, pool.size());
            assertEquals(0, pool.idle());
        }
    }

    // ---- the simplified API ------------------------------------------------

    @Test
    void aContextIsServedFromThePool() {
        TassisConnectionPool pool = pool(2);

        for (int i = 0; i < 3; i++) {
            try (JMSContext context = pool.createContext()) {
                context.createProducer().send(context.createQueue("orders"), "n" + i);
            }
        }

        assertEquals(1, pool.size());
        assertEquals(1, pool.idle());
        try (JMSContext context = pool.createContext(JMSContext.SESSION_TRANSACTED)) {
            assertEquals("n0", context.createConsumer(context.createQueue("orders")).receiveBody(String.class, SOON));
            // closed without a commit: rolled back
        }
        assertEquals("n0", receive("orders"));
    }

    @Test
    void contextsCreatedFromOneAnotherAreOneBorrowedConnection() {
        TassisConnectionPool pool = pool(1);
        JMSContext first = pool.createContext();
        JMSContext second = first.createContext(JMSContext.AUTO_ACKNOWLEDGE);

        assertEquals(1, pool.size());
        first.close();
        assertEquals(0, pool.idle(), "the connection still serves the second context");
        second.createProducer().send(second.createQueue("orders"), "from the second");
        second.close();

        assertEquals(1, pool.idle());
        assertEquals("from the second", receive("orders"));
    }

    @Test
    void anUnknownSessionModeBorrowsNothing() {
        TassisConnectionPool pool = pool(1);

        assertThrows(JMSRuntimeException.class, () -> pool.createContext(42));
        assertEquals(0, pool.size());
    }

    // ---- how long a connection is kept --------------------------------------

    private static void await(String what, java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, what);
            Thread.sleep(10);
        }
    }

    private TassisConnectionPool built(TassisConnectionPool.Builder builder) {
        TassisConnectionPool pool = builder.build();
        pools.add(pool);
        return pool;
    }

    @Test
    void aConnectionNobodyUsesForLongIsClosed() throws Exception {
        TassisConnectionPool pool = built(TassisConnectionPool.over(factory).idleTimeout(Duration.ofMillis(150)));
        pool.createConnection().close();
        assertEquals(1, pool.idle());

        await("the idle connection is closed", () -> pool.size() == 0);

        // and the pool opens another when one is asked for
        pool.createConnection().close();
        assertEquals(1, pool.size());
    }

    @Test
    void aConnectionInUseIsNotIdleHoweverLongItIsKept() throws Exception {
        TassisConnectionPool pool = built(TassisConnectionPool.over(factory).idleTimeout(Duration.ofMillis(100))
                .maxLifetime(Duration.ofMillis(100)));

        try (Connection connection = pool.createConnection()) {
            Thread.sleep(400);
            assertEquals(1, pool.size(), "nothing is closed under a borrower");
            connection.createSession().close();
        }

        assertEquals(0, pool.size(), "past its lifetime, it is closed when it comes back");
    }

    @Test
    void aConnectionPastItsLifetimeIsClosedThoughItIsUsedOften() throws Exception {
        TassisConnectionPool pool = built(TassisConnectionPool.over(factory).idleTimeout(Duration.ZERO)
                .maxLifetime(Duration.ofMillis(300)));
        pool.createConnection().close();
        long start = System.nanoTime();

        // borrowed again and again, so never idle for long
        while (pool.size() == 1 && System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10)) {
            pool.createConnection().close();
            Thread.sleep(10);
        }

        assertEquals(0, pool.size());
        assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(250), "not before its time");
    }

    @Test
    void zeroKeepsAConnectionForAsLongAsThePoolLasts() throws Exception {
        TassisConnectionPool pool = built(
                TassisConnectionPool.over(factory).idleTimeout(Duration.ZERO).maxLifetime(Duration.ZERO));
        pool.createConnection().close();

        Thread.sleep(300);

        assertEquals(1, pool.idle());
    }

    @Test
    void aConnectionThatIsNotGivenBackIsReportedWithWhoBorrowedIt() throws Exception {
        List<java.util.logging.LogRecord> reports = java.util.Collections.synchronizedList(new ArrayList<>());
        java.util.logging.Handler handler = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord report) {
                reports.add(report);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(TassisConnectionPool.class.getName());
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
        try {
            TassisConnectionPool pool =
                    built(TassisConnectionPool.over(factory).leakThreshold(Duration.ofMillis(100)));
            pool.createConnection().close();
            Connection forgotten = borrowedAndForgotten(pool);

            await("the connection is reported", () -> !reports.isEmpty());
            Thread.sleep(300);

            assertEquals(1, reports.size(), "once, and not the one that was given back");
            java.util.logging.LogRecord report = reports.get(0);
            assertEquals(java.util.logging.Level.WARNING, report.getLevel());
            assertTrue(java.util.Arrays.stream(report.getThrown().getStackTrace())
                    .anyMatch(frame -> frame.getMethodName().equals("borrowedAndForgotten")), "the call that borrowed it");
            // reported, not taken back
            forgotten.createSession().close();
            forgotten.close();
            assertEquals(1, pool.idle());
        } finally {
            logger.removeHandler(handler);
            logger.setUseParentHandlers(true);
        }
    }

    private static Connection borrowedAndForgotten(TassisConnectionPool pool) throws JMSException {
        return pool.createConnection();
    }

    // ---- settings ------------------------------------------------------------

    @Test
    void aPoolStartsFromTheSettingsOfItsFactory() throws Exception {
        TassisConnectionFactory configured = TassisConnectionFactory.fromUrl("tassis:vm:" + broker
                + "?pool.maxSize=1&pool.acquireTimeout=PT0.2S&pool.idleTimeout=PT0.15S");
        TassisConnectionPool pool = built(TassisConnectionPool.over(configured));

        try (Connection only = pool.createConnection()) {
            long start = System.nanoTime();
            assertThrows(ResourceAllocationException.class, pool::createConnection);
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3), "after 200 ms, not the default");
        }
        await("the idle connection is closed", () -> pool.size() == 0);
    }

    @Test
    void theBuilderHasTheLastWordOverTheSettings() throws JMSException {
        TassisConnectionFactory configured =
                TassisConnectionFactory.from(java.util.Map.of("tassis.broker", broker, "tassis.pool.maxSize", "1"));
        TassisConnectionPool pool = built(TassisConnectionPool.over(configured).maxSize(2));

        try (Connection first = pool.createConnection(); Connection second = pool.createConnection()) {
            assertEquals(2, pool.size());
        }
    }

    @Test
    void aDurationThatMakesNoSenseIsRefused() {
        TassisConnectionPool.Builder builder = TassisConnectionPool.over(factory);

        assertThrows(IllegalArgumentException.class, () -> builder.idleTimeout(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> builder.maxLifetime(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> builder.leakThreshold(Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class, () -> builder.idleTimeout(null));
    }

    // ---- closing the pool --------------------------------------------------

    @Test
    void aClosedPoolLendsNothingAndKeepsNothing() throws JMSException {
        TassisConnectionPool pool = pool(2);
        pool.createConnection().close();
        Connection lent = pool.createConnection();
        assertEquals(1, pool.size());

        pool.close();
        pool.close();

        assertThrows(IllegalStateException.class, pool::createConnection);
        assertInstanceOf(IllegalStateException.class,
                assertThrows(IllegalStateRuntimeException.class, pool::createContext).getCause());
        // what is lent stays usable, and is closed when it is given back
        assertEquals(1, pool.size());
        lent.createSession().close();
        lent.close();
        assertEquals(0, pool.size());
        assertEquals(0, pool.idle());
    }

    @Test
    void aBorrowerThatWaitsFailsWhenThePoolCloses() throws Exception {
        TassisConnectionPool pool =
                TassisConnectionPool.over(factory).maxSize(1).acquireTimeout(Duration.ofSeconds(30)).build();
        pools.add(pool);
        Connection held = pool.createConnection();

        try (ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Connection> waiting = threads.submit(() -> pool.createConnection());
            assertThrows(java.util.concurrent.TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));

            pool.close();

            Throwable failure = assertTimeoutPreemptively(Duration.ofSeconds(5),
                    () -> assertThrows(java.util.concurrent.ExecutionException.class, waiting::get)).getCause();
            assertInstanceOf(IllegalStateException.class, failure);
        }
        held.close();
        assertFalse(pool.size() > 0);
    }
}
