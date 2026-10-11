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
package io.vidocq.tassis.amqp.internal.messaging;

import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A body section holding a list of values; a message may have several, which are one sequence.
 *
 * <p>AMQP 1.0 part 3, §3.2.7: {@code amqp:amqp-sequence:list}, descriptor 0x76.
 */
public record AmqpSequence(List<Object> value) implements Section {

    public static final ULong CODE = ULong.of(0x76);
    public static final Symbol NAME = Symbol.of("amqp:amqp-sequence:list");

    public AmqpSequence {
        // Not List.copyOf: a sequence may hold nulls.
        value = Collections.unmodifiableList(new ArrayList<>(value));
    }

    /** Builds it from the value a peer sent. */
    public static AmqpSequence of(Object described) throws AmqpDecodeException {
        if (!(described instanceof List<?> list)) {
            throw new AmqpDecodeException("amqp-sequence is a list");
        }
        return new AmqpSequence(new ArrayList<>(list));
    }

    @Override
    public ULong descriptorCode() {
        return CODE;
    }

    @Override
    public Symbol descriptorName() {
        return NAME;
    }

    @Override
    public Object describedValue() {
        return value;
    }
}
