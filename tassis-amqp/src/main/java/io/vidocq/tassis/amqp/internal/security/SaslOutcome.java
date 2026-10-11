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
package io.vidocq.tassis.amqp.internal.security;

import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import io.vidocq.tassis.amqp.internal.types.Fields;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.List;

/**
 * How the authentication ended: code 0 is success.
 *
 * <p>AMQP 1.0 part 5, §5.3.3.5: {@code amqp:sasl-outcome:list}, descriptor 0x44. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record SaslOutcome(Integer code, Binary additionalData) implements SaslBody, DescribedType {

    public static final ULong CODE = ULong.of(0x44);
    public static final Symbol NAME = Symbol.of("amqp:sasl-outcome:list");

    public SaslOutcome {
        if (code == null) {
            throw new IllegalArgumentException("sasl-outcome needs its field code");
        }
    }

    /** Builds it from the list of fields a peer sent. */
    public static SaslOutcome of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("sasl-outcome", described);
        return new SaslOutcome(fields.required("code", fields.ubyte(0, "code")),
                fields.get(1, "additional-data", Binary.class));
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
    public List<Object> describedValue() {
        return Fields.list(Fields.ubyte(code), additionalData);
    }
}
