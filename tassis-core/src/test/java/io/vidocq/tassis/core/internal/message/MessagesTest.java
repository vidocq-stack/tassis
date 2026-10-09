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
package io.vidocq.tassis.core.internal.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import jakarta.jms.JMSException;
import org.junit.jupiter.api.Test;

/** Snapshots taken at send time (Jakarta Messaging 3.1 §3.9), including of foreign messages (§3.12). */
class MessagesTest {

    @Test
    void snapshotIsIndependentOfTheOriginal() throws JMSException {
        TassisTextMessage original = new TassisTextMessage();
        original.setText("hello");
        original.setIntProperty("attempt", 1);
        original.setJMSType("order");

        TassisTextMessage snapshot = (TassisTextMessage) Messages.snapshot(original);
        original.setText("changed");
        original.setIntProperty("attempt", 2);

        assertNotSame(original, snapshot);
        assertEquals("hello", snapshot.getText());
        assertEquals(1, snapshot.getIntProperty("attempt"));
        assertEquals("order", snapshot.getJMSType());
    }

    @Test
    void snapshotOfABytesMessageLeavesItWritable() throws JMSException {
        TassisBytesMessage original = new TassisBytesMessage();
        original.writeInt(3);

        TassisBytesMessage snapshot = (TassisBytesMessage) Messages.snapshot(original);
        original.writeInt(4);
        snapshot.reset();

        assertEquals(4L, snapshot.getBodyLength(), "the snapshot holds what was written when it was taken");
        assertEquals(3, snapshot.readInt());
    }

    @Test
    void foreignMessageIsRebuiltAsATassisMessage() throws JMSException {
        ForeignTextMessage foreign = new ForeignTextMessage();
        foreign.setText("from elsewhere");
        foreign.setStringProperty("origin", "other provider");
        foreign.setJMSCorrelationID("corr");
        foreign.setJMSPriority(7);

        TassisTextMessage snapshot = assertInstanceOf(TassisTextMessage.class, Messages.snapshot(foreign));

        assertEquals("from elsewhere", snapshot.getText());
        assertEquals("other provider", snapshot.getStringProperty("origin"));
        assertEquals("corr", snapshot.getJMSCorrelationID());
        assertEquals(7, snapshot.getJMSPriority());
    }
}
