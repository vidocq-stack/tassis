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

import io.vidocq.tassis.core.internal.message.TassisMessage;
import jakarta.jms.InvalidSelectorException;

/**
 * A compiled message selector (Jakarta Messaging 3.1 §3.8): a conditional
 * expression over a message's header fields and properties. Two selectors are
 * equal when their texts are, which is how a subscription's selector is compared
 * (§8.3).
 */
public final class Selector {

    private final String text;
    private final Expression condition;
    private final boolean referencesDeliveryCount;

    private Selector(String text, Expression condition, boolean referencesDeliveryCount) {
        this.text = text;
        this.condition = condition;
        this.referencesDeliveryCount = referencesDeliveryCount;
    }

    /**
     * Compiles a selector.
     *
     * @return the selector, or {@code null} if {@code text} is null or blank: an empty selector
     *         means no selector (§3.8.1.1)
     * @throws InvalidSelectorException if the text is not a valid selector
     */
    public static Selector parse(String text) throws InvalidSelectorException {
        if (text == null || text.isBlank()) {
            return null;
        }
        SelectorParser parser = new SelectorParser(text);
        return new Selector(text, parser.parse(), parser.referencesDeliveryCount());
    }

    /**
     * Returns whether the selector evaluates to true for this message; false and unknown do not
     * match.
     *
     * @param deliveryCount the value of {@code JMSXDeliveryCount} for the delivery being
     *        considered: 1 for a first delivery
     */
    public boolean matches(TassisMessage message, int deliveryCount) {
        return Boolean.TRUE.equals(condition.evaluate(message, deliveryCount));
    }

    /**
     * Whether the outcome depends on {@code JMSXDeliveryCount}, and so may differ from one
     * delivery of a message to the next. Any other selector gives one answer per message.
     */
    public boolean referencesDeliveryCount() {
        return referencesDeliveryCount;
    }

    /** The selector as the client wrote it. */
    public String text() {
        return text;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Selector selector && selector.text.equals(text);
    }

    @Override
    public int hashCode() {
        return text.hashCode();
    }

    @Override
    public String toString() {
        return text;
    }
}
