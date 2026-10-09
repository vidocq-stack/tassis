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
package io.vidocq.tassis.core.internal.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.core.internal.message.TassisMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Message selectors (Jakarta Messaging 3.1 §3.8): syntax, typing and NULL semantics. */
class SelectorTest {

    private TassisMessage message;

    @BeforeEach
    void aCar() throws JMSException {
        message = new TassisMessage();
        message.setJMSType("car");
        message.setJMSPriority(7);
        message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        message.setStringProperty("color", "blue");
        message.setIntProperty("weight", 2600);
        message.setDoubleProperty("price", 19999.5);
        message.setBooleanProperty("electric", true);
        message.setStringProperty("NumberOfOrders", "2");
        message.setStringProperty("phone", "12993");
        message.setStringProperty("underscored", "_foo");
    }

    private boolean matches(String selector) throws InvalidSelectorException {
        return Selector.parse(selector).matches(message, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "JMSType = 'car' AND color = 'blue' AND weight > 2500",
        "color <> 'red'",
        "weight >= 2600 and weight <= 2600",
        "weight BETWEEN 2000 AND 3000",
        "weight NOT BETWEEN 15 AND 19",
        "color IN ('red', 'blue')",
        "color NOT IN ('red', 'green')",
        "phone LIKE '12%3'",
        "phone NOT LIKE '12%4'",
        "color LIKE 'bl_e'",
        "underscored LIKE '\\_%' ESCAPE '\\'",
        "missing IS NULL",
        "color IS NOT NULL",
        "electric",
        "electric = TRUE",
        "NOT (color = 'red')",
        "color = 'red' OR weight > 2500",
        "weight + 400 = 3000",
        "weight * 2 / 4 - 300 = 1000",
        "-weight < 0",
        "price > weight",
        "weight = 2600.0",
        "price = 1.99995E4",
        "weight > 0x10",
        "JMSPriority > 4",
        "JMSDeliveryMode = 'NON_PERSISTENT'",
        "\tweight\n>\r2500 ",
        "color = 'blue' Or missing = 1",
    })
    void matchingSelectors(String selector) throws InvalidSelectorException {
        assertTrue(matches(selector), selector);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "color = 'red'",
        "weight < 2500",
        "color IN ('red')",
        "phone LIKE '12%4'",
        "color LIKE 'bl_'",
        "underscored LIKE '\\_%' ESCAPE '\\' AND color LIKE '\\_%' ESCAPE '\\'",
        "missing IS NOT NULL",
        "electric = FALSE",
        "JMSDeliveryMode = 'PERSISTENT'",
    })
    void nonMatchingSelectors(String selector) throws InvalidSelectorException {
        assertFalse(matches(selector), selector);
    }

    @Test
    void propertyKeepsItsOwnTypeInASelector() throws InvalidSelectorException {
        assertFalse(matches("NumberOfOrders > 1"), "§3.8.1.1: a string cannot be used in an arithmetic comparison");
        assertFalse(matches("NumberOfOrders = 2"), "values of unlike types are never equal");
        assertTrue(matches("NumberOfOrders = '2'"));
    }

    @Test
    void comparisonWithNullIsUnknownAndUnknownDoesNotMatch() throws InvalidSelectorException {
        assertFalse(matches("missing = 1"));
        assertFalse(matches("NOT (missing = 1)"), "NOT unknown is unknown");
        assertFalse(matches("missing IN ('a')"));
        assertFalse(matches("missing NOT IN ('a')"));
        assertFalse(matches("missing LIKE 'a%'"));
        assertFalse(matches("missing + 1 > 0"));
    }

    @Test
    void logicalOperatorsUseThreeValuedLogic() throws InvalidSelectorException {
        assertTrue(matches("missing = 1 OR color = 'blue'"), "unknown OR true is true");
        assertFalse(matches("missing = 1 AND color = 'blue'"), "unknown AND true is unknown");
        assertTrue(matches("NOT (missing = 1 AND color = 'red')"), "unknown AND false is false");
    }

    @Test
    void operatorsFollowThePrecedenceOfTheSpec() throws InvalidSelectorException {
        assertTrue(matches("color = 'blue' OR color = 'red' AND weight < 0"), "AND binds tighter than OR");
        assertFalse(matches("(color = 'blue' OR color = 'red') AND weight < 0"));
        assertTrue(matches("NOT color = 'red' AND weight > 0"), "NOT binds tighter than AND");
        assertTrue(matches("2 + 3 * 4 = 14"));
        assertTrue(matches("(2 + 3) * 4 = 20"));
    }

    @Test
    void stringLiteralDoublesItsQuotes() throws JMSException {
        message.setStringProperty("owner", "it's mine");

        assertTrue(matches("owner = 'it''s mine'"));
    }

    @Test
    void extremeLongLiteralsAreAccepted() throws InvalidSelectorException {
        assertTrue(matches("weight > -9223372036854775808"));
        assertTrue(matches("weight < 9223372036854775807"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "definitely not a message selector!",
        "color = ",
        "= 'blue'",
        "color = 'blue",
        "(color = 'blue'",
        "color = 'blue')",
        "color == 'blue'",
        "color IN ('a', 3)",
        "color IN ()",
        "color LIKE 5",
        "color LIKE 'a' ESCAPE 'ab'",
        "color LIKE 'a\\' ESCAPE '\\'",
        "weight BETWEEN 1",
        "color IS 'blue'",
        "'blue' IN ('blue')",
        "weight +",
        "weight > 99999999999999999999",
        "AND",
        "NOT",
        "color = 'a' color = 'b'",
        "JMSDestination = 'q'",
        "JMSRedelivered = TRUE",
        "weight + 1",
        "'blue'",
        "'a' + 1 = 2",
        "weight > 1 AND 'text'",
        "weight > 'foo'",
        "'foo' >= TRUE",
        "electric < TRUE",
        "TRUE = 0",
        "'foo' <> 1",
        "weight BETWEEN 'foo' AND 'test'",
    })
    void invalidSelectors(String selector) {
        assertThrows(InvalidSelectorException.class, () -> Selector.parse(selector), selector);
    }

    @Test
    void deliveryCountIsTheOneOfTheDeliveryBeingConsidered() throws InvalidSelectorException {
        Selector retries = Selector.parse("JMSXDeliveryCount > 1");

        assertFalse(retries.matches(message, 1));
        assertTrue(retries.matches(message, 2));
        assertTrue(retries.referencesDeliveryCount());
        assertFalse(Selector.parse("weight > 1").referencesDeliveryCount());
    }

    @Test
    void emptySelectorMeansNoSelector() throws InvalidSelectorException {
        assertEquals(null, Selector.parse(null));
        assertEquals(null, Selector.parse(""));
        assertEquals(null, Selector.parse("   "));
    }

    @Test
    void selectorsAreEqualByText() throws InvalidSelectorException {
        assertEquals(Selector.parse("color = 'blue'"), Selector.parse("color = 'blue'"));
        assertNotEquals(Selector.parse("color = 'blue'"), Selector.parse("color = 'red'"));
        assertEquals("color = 'blue'", Selector.parse("color = 'blue'").text());
    }
}
