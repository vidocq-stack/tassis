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
package io.vidocq.tassis.amqp.internal.types;

import java.util.List;

/**
 * An AMQP {@code array}: values of one type, whose constructor is written once (§1.6.24).
 *
 * @param descriptor the descriptor of the elements if their constructor is a described one,
 *        otherwise {@code null}
 * @param elementType the primitive type of every element
 */
public record AmqpArray(Object descriptor, AmqpType elementType, List<?> elements) {

    public AmqpArray {
        elements = List.copyOf(elements);
    }

    public static AmqpArray of(AmqpType elementType, List<?> elements) {
        return new AmqpArray(null, elementType, elements);
    }

    public static AmqpArray ofSymbols(List<Symbol> symbols) {
        return new AmqpArray(null, AmqpType.SYMBOL, symbols);
    }
}
