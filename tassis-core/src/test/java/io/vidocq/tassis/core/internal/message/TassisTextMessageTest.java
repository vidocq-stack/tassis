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
import org.junit.jupiter.api.Test;

/** {@code TextMessage} body (Jakarta Messaging 3.1 §3.11). */
class TassisTextMessageTest {

    private final TassisTextMessage message = new TassisTextMessage();

    @Test
    void textRoundTrips() throws JMSException {
        message.setText("hello");

        assertEquals("hello", message.getText());
        assertEquals("hello", message.getBody(String.class));
        assertEquals("hello", message.getBody(CharSequence.class));
        assertTrue(message.isBodyAssignableTo(Object.class));
    }

    @Test
    void bodyIsNotAssignableToAnUnrelatedType() throws JMSException {
        message.setText("hello");

        assertFalse(message.isBodyAssignableTo(Integer.class));
        assertThrows(MessageFormatException.class, () -> message.getBody(Integer.class));
    }

    @Test
    void messageWithoutTextHasNoBody() throws JMSException {
        assertNull(message.getText());
        assertNull(message.getBody(Integer.class));
        assertTrue(message.isBodyAssignableTo(Integer.class));
    }

    @Test
    void bodyOfAReceivedMessageIsReadOnlyUntilCleared() throws JMSException {
        message.setText("hello");
        message.markReceived();

        assertThrows(MessageNotWriteableException.class, () -> message.setText("changed"));
        assertEquals("hello", message.getText());

        message.clearBody();
        assertNull(message.getText());
        message.setText("changed");
        assertEquals("changed", message.getText());
    }
}
