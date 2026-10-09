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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.JMSException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** {@code ObjectMessage} body (Jakarta Messaging 3.1 §3.11). */
class TassisObjectMessageTest {

    /** Serializable holder of a field that is not: serialization fails at write time. */
    private static final class Unserializable implements Serializable {
        private static final long serialVersionUID = 1L;
        @SuppressWarnings("unused")
        private final Object field = new Object();
    }

    private final TassisObjectMessage message = new TassisObjectMessage();

    @Test
    void objectRoundTrips() throws JMSException {
        message.setObject("payload");

        assertEquals("payload", message.getObject());
        assertEquals("payload", message.getBody(Serializable.class));
        assertEquals("payload", message.getBody(String.class));
    }

    @Test
    void messageHoldsASnapshotOfTheObject() throws JMSException {
        ArrayList<String> list = new ArrayList<>(List.of("a"));
        message.setObject(list);

        list.add("b");

        assertEquals(List.of("a"), message.getObject(), "a later change to the object must not reach the message");
    }

    @Test
    void objectThatFailsToSerializeIsRejected() {
        assertThrows(MessageFormatException.class, () -> message.setObject(new Unserializable()));
    }

    @Test
    void bodyIsNotAssignableToAnUnrelatedType() throws JMSException {
        message.setObject("payload");

        assertFalse(message.isBodyAssignableTo(Integer.class));
        assertThrows(MessageFormatException.class, () -> message.getBody(Integer.class));
    }

    @Test
    void messageWithoutObjectHasNoBody() throws JMSException {
        assertNull(message.getObject());
        assertNull(message.getBody(Integer.class));
        assertTrue(message.isBodyAssignableTo(Integer.class));
    }

    @Test
    void bodyOfAReceivedMessageIsReadOnlyUntilCleared() throws JMSException {
        message.setObject("payload");
        message.markReceived();

        assertThrows(MessageNotWriteableException.class, () -> message.setObject("changed"));

        message.clearBody();
        assertNull(message.getObject());
        message.setObject("changed");
        assertEquals("changed", message.getObject());
    }
}
