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
package io.vidocq.tassis.amqp.internal.transport;

import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import io.vidocq.tassis.amqp.internal.types.Fields;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.List;

/**
 * Informs the peer of the state of a range of deliveries.
 *
 * <p>AMQP 1.0 part 2, §2.7.6: {@code amqp:disposition:list}, descriptor 0x15. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record Disposition(Boolean role, Long first, Long last, Boolean settled, Object state,
        Boolean batchable) implements Performative, DescribedType {

    public static final ULong CODE = ULong.of(0x15);
    public static final Symbol NAME = Symbol.of("amqp:disposition:list");

    public Disposition {
        if (role == null) {
            throw new IllegalArgumentException("disposition needs its field role");
        }
        if (first == null) {
            throw new IllegalArgumentException("disposition needs its field first");
        }
    }

    /** Builds it from the list of fields a peer sent. */
    public static Disposition of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("disposition", described);
        return new Disposition(fields.required("role", fields.get(0, "role", Boolean.class)),
                fields.required("first", fields.uint(1, "first")),
                fields.uint(2, "last"),
                fields.get(3, "settled", Boolean.class),
                fields.any(4),
                fields.get(5, "batchable", Boolean.class));
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
        return Fields.list(role, Fields.uint(first), Fields.uint(last), settled, state, batchable);
    }
}
