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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.internal.broker.BrokerConnection.Consumer;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Delivery;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Outgoing;
import io.vidocq.tassis.core.internal.broker.BrokerConnection.Subscribing;
import io.vidocq.tassis.core.internal.message.Messages;
import io.vidocq.tassis.core.internal.message.TassisTextMessage;
import io.vidocq.tassis.core.internal.selector.Selector;
import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Messages whose delivery is delayed (Jakarta Messaging 3.1 §7.9): the broker keeps them aside
 * until their time comes, so that no receive looks at them before (ROADMAP M12-4), and what an
 * application sees is what it saw when they waited in the queue among the others.
 */
@Timeout(30)
class DelayedMessagesTest {

    private static final long SOON = TimeUnit.SECONDS.toNanos(10);
    private static final long AN_HOUR = TimeUnit.HOURS.toMillis(1);

    private Broker broker;
    private BrokerConnection connection;

    @BeforeEach
    void connect() throws JMSException {
        broker = Broker.named("delayed-" + UUID.randomUUID());
        connection = broker.connect(null, null);
        connection.start();
    }

    @AfterEach
    void disconnect() {
        connection.close();
    }

    private Outgoing message(String destination, boolean topic, String text, long delay, int priority)
            throws JMSException {
        TassisTextMessage message = new TassisTextMessage();
        message.setText(text);
        message.setStringProperty("kind", text);
        message.setJMSMessageID(connection.nextMessageId());
        message.setJMSPriority(priority);
        message.setJMSDeliveryTime(delay == 0 ? 0 : System.currentTimeMillis() + delay);
        return new Outgoing(destination, topic, false, Messages.snapshot(message));
    }

    private void send(String text, long delay) throws JMSException {
        connection.send(message("orders", false, text, delay, 4));
    }

    private static String text(Delivery delivery) throws JMSException {
        return delivery == null ? null : ((TextMessage) delivery.message()).getText();
    }

    // ---- what it is for ----------------------------------------------------------

    @Test
    void aReceiveDoesNotLookAtTheMessagesThatAreNotDue() throws Exception {
        List<Outgoing> later = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            later.add(message("orders", false, "later " + i, AN_HOUR, 4));
        }
        connection.commit(later, List.of());
        Consumer consumer = connection.attach("orders", false, null);
        long before = broker.examined();

        for (int i = 0; i < 10; i++) {
            send("now " + i, 0);
            assertEquals("now " + i, text(consumer.receive(0, true)));
        }
        assertNull(consumer.receive(0, true));

        assertEquals(10, broker.examined() - before, "one message looked at for each message taken");
        assertEquals(1_000, broker.messageCount("orders"));
    }

    // ---- what must stay as it was ------------------------------------------------

    @Test
    void aDelayedMessageIsNotDeliveredBeforeItsTimeAndIsAtItsTime() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        long due = System.currentTimeMillis() + 300;
        send("later", 300);

        assertNull(consumer.receive(0, true));
        assertEquals("later", text(consumer.receive(SOON, true)));
        assertTrue(System.currentTimeMillis() >= due);
    }

    @Test
    void aMessageThatIsDueDoesNotWaitBehindOneThatIsNot() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        send("later", 300);
        send("now", 0);

        assertEquals("now", text(consumer.receive(0, true)));
        assertEquals("later", text(consumer.receive(SOON, true)));
    }

    @Test
    void delayedMessagesBecomeDueEachAtItsOwnTime() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        send("sent first, due last", 500);
        send("sent last, due first", 150);

        assertEquals("sent last, due first", text(consumer.receive(SOON, true)));
        assertEquals("sent first, due last", text(consumer.receive(SOON, true)));
    }

    @Test
    void messagesThatAreDueAreDeliveredByPriorityThenInTheOrderTheyWereSent() throws Exception {
        connection.send(message("orders", false, "first, delayed", 150, 4));
        connection.send(message("orders", false, "second, at once", 0, 4));
        connection.send(message("orders", false, "third, delayed and urgent", 100, 9));
        connection.send(message("orders", false, "fourth, delayed", 50, 4));
        Thread.sleep(300);
        Consumer consumer = connection.attach("orders", false, null);

        // §3.4.10 and §4.1: once due, a message has the place its priority and its sending give it
        assertEquals("third, delayed and urgent", text(consumer.receive(0, true)));
        assertEquals("first, delayed", text(consumer.receive(0, true)));
        assertEquals("second, at once", text(consumer.receive(0, true)));
        assertEquals("fourth, delayed", text(consumer.receive(0, true)));
    }

    @Test
    void aBrowserSeesADelayedMessageOnceItIsDueAndNotBefore() throws Exception {
        send("later", 200);
        send("now", 0);

        // §7.9: a browser must not return a message before its delivery time
        assertEquals(1, connection.browse("orders", null).size());
        assertEquals(2, broker.messageCount("orders"));
        Thread.sleep(300);
        List<String> seen = new ArrayList<>();
        for (var browsed : connection.browse("orders", null)) {
            seen.add(((TextMessage) browsed).getText());
        }
        assertEquals(List.of("later", "now"), seen);
    }

    @Test
    void aConsumerWithASelectorGetsADelayedMessageItSelectsOnceItIsDue() throws Exception {
        Consumer consumer = connection.attach("orders", false, Selector.parse("kind = 'wanted'"));
        send("unwanted", 100);
        send("wanted", 200);

        assertNull(consumer.receive(0, true));
        assertEquals("wanted", text(consumer.receive(SOON, true)));
        assertNull(consumer.receive(TimeUnit.MILLISECONDS.toNanos(50), true));
        assertEquals(1, broker.messageCount("orders"));
    }

    @Test
    void aDelayedMessagePublishedToATopicReachesItsSubscriberAtItsTime() throws Exception {
        Consumer subscriber = connection.subscribe(new Subscribing("news", false, null, false, false, false, null, false));
        connection.send(message("news", true, "later", 200, 4));
        connection.send(message("news", true, "now", 0, 4));

        assertEquals("now", text(subscriber.receive(0, true)));
        assertNull(subscriber.receive(0, true));
        assertEquals("later", text(subscriber.receive(SOON, true)));
    }

    @Test
    void aDelayedMessageGivenBackAfterItWasDeliveredIsDeliveredAgainAtOnce() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        send("later", 150);
        Delivery first = consumer.receive(SOON, false);
        assertNotNull(first);

        connection.redeliver(List.of(first));

        Delivery again = consumer.receive(0, true);
        assertEquals("later", text(again));
        assertEquals(2, again.deliveryCount());
    }

    @Test
    void aDelayedMessageThatExpiresBeforeAnybodyTakesItIsRemoved() throws Exception {
        TassisTextMessage message = new TassisTextMessage();
        message.setText("short-lived");
        message.setJMSMessageID(connection.nextMessageId());
        long now = System.currentTimeMillis();
        message.setJMSDeliveryTime(now + 100);
        message.setJMSExpiration(now + 250);
        connection.send(new Outgoing("orders", false, false, Messages.snapshot(message)));
        Consumer consumer = connection.attach("orders", false, null);

        Thread.sleep(350);

        // §3.4.9
        assertNull(consumer.receive(0, true));
        assertEquals(0, broker.messageCount("orders"));
    }

    @Test
    void aConsumerThatComesWithANewSelectorGetsWhatBecameDueBeforeItCame() throws Exception {
        send("wanted", 150);
        send("unwanted", 150);
        Thread.sleep(300);

        // Nobody has asked for anything since they became due: they are still aside.
        Consumer consumer = connection.attach("orders", false, Selector.parse("kind = 'wanted'"));

        assertEquals("wanted", text(consumer.receive(0, true)));
        assertNull(consumer.receive(0, true));
    }

    @Test
    void whatBecameDueWhileItsConnectionWasStoppedIsDeliveredOnceItStarts() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        connection.stop();
        send("later", 150);
        Thread.sleep(300);

        // §6.1.5: a stopped connection delivers nothing
        assertNull(consumer.receive(0, true));
        connection.start();

        assertEquals("later", text(consumer.receive(0, true)));
    }

    @Test
    void aMessageSentWithoutADelayIsDueAtOnceWhateverTheClockLastShown() throws Exception {
        Consumer consumer = connection.attach("orders", false, null);
        for (int i = 0; i < 5; i++) {
            // each is stamped with the time it is sent at, a little later than the last receive
            TassisTextMessage message = new TassisTextMessage();
            message.setText("n" + i);
            message.setJMSMessageID(connection.nextMessageId());
            message.setJMSDeliveryTime(System.currentTimeMillis());
            connection.send(new Outgoing("orders", false, false, Messages.snapshot(message)));

            assertEquals("n" + i, text(consumer.receive(0, true)));
            Thread.sleep(3);
        }
    }
}
