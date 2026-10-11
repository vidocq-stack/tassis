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
 * The mechanism a client chooses, with its initial response.
 *
 * <p>AMQP 1.0 part 5, §5.3.3.2: {@code amqp:sasl-init:list}, descriptor 0x41. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record SaslInit(Symbol mechanism, Binary initialResponse, String hostname) implements SaslBody, DescribedType {

    public static final ULong CODE = ULong.of(0x41);
    public static final Symbol NAME = Symbol.of("amqp:sasl-init:list");

    public SaslInit {
        if (mechanism == null) {
            throw new IllegalArgumentException("sasl-init needs its field mechanism");
        }
    }

    /** Builds it from the list of fields a peer sent. */
    public static SaslInit of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("sasl-init", described);
        return new SaslInit(fields.required("mechanism", fields.get(0, "mechanism", Symbol.class)),
                fields.get(1, "initial-response", Binary.class),
                fields.get(2, "hostname", String.class));
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
        return Fields.list(mechanism, initialResponse, hostname);
    }
}
