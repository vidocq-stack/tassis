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
 * Details about the message that can only be known once the whole of it is built, such as a signature.
 *
 * <p>AMQP 1.0 part 3, §3.2.9: {@code amqp:footer:map}, descriptor 0x78. The keys are symbols or ulongs.
 */
public record Footer(Map<Object, Object> value) implements Section {

    public static final ULong CODE = ULong.of(0x78);
    public static final Symbol NAME = Symbol.of("amqp:footer:map");

    public Footer {
        value = Collections.unmodifiableMap(new LinkedHashMap<>(value));
    }

    /** Builds it from the value a peer sent. */
    public static Footer of(Object described) throws AmqpDecodeException {
        if (!(described instanceof Map<?, ?> map)) {
            throw new AmqpDecodeException("footer is a map");
        }
        return new Footer(new LinkedHashMap<>(map));
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
