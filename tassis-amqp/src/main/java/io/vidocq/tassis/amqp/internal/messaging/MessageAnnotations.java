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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Annotations that travel with the message from node to node.
 *
 * <p>AMQP 1.0 part 3, §3.2.3: {@code amqp:message-annotations:map}, descriptor 0x72. The keys are symbols or ulongs.
 */
public record MessageAnnotations(Map<Object, Object> value) implements Section {

    public static final ULong CODE = ULong.of(0x72);
    public static final Symbol NAME = Symbol.of("amqp:message-annotations:map");

    public MessageAnnotations {
        value = Collections.unmodifiableMap(new LinkedHashMap<>(value));
    }

    /** Builds it from the value a peer sent. */
    public static MessageAnnotations of(Object described) throws AmqpDecodeException {
        if (!(described instanceof Map<?, ?> map)) {
            throw new AmqpDecodeException("message-annotations is a map");
        }
        return new MessageAnnotations(new LinkedHashMap<>(map));
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
