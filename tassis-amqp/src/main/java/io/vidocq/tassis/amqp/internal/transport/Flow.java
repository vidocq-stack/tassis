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
import java.util.Map;

/**
 * Updates the flow state of a session and, with a handle, of one of its links.
 *
 * <p>AMQP 1.0 part 2, §2.7.4: {@code amqp:flow:list}, descriptor 0x13. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record Flow(Long nextIncomingId, Long incomingWindow, Long nextOutgoingId, Long outgoingWindow, Long handle,
        Long deliveryCount, Long linkCredit, Long available, Boolean drain, Boolean echo,
        Map<Symbol, Object> properties) implements Performative, DescribedType {

    public static final ULong CODE = ULong.of(0x13);
    public static final Symbol NAME = Symbol.of("amqp:flow:list");

    public Flow {
        if (incomingWindow == null) {
            throw new IllegalArgumentException("flow needs its field incoming-window");
        }
        if (nextOutgoingId == null) {
            throw new IllegalArgumentException("flow needs its field next-outgoing-id");
        }
        if (outgoingWindow == null) {
            throw new IllegalArgumentException("flow needs its field outgoing-window");
        }
    }

    /** Builds it from the list of fields a peer sent. */
    public static Flow of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("flow", described);
        return new Flow(fields.uint(0, "next-incoming-id"),
                fields.required("incoming-window", fields.uint(1, "incoming-window")),
                fields.required("next-outgoing-id", fields.uint(2, "next-outgoing-id")),
                fields.required("outgoing-window", fields.uint(3, "outgoing-window")),
                fields.uint(4, "handle"),
                fields.uint(5, "delivery-count"),
                fields.uint(6, "link-credit"),
                fields.uint(7, "available"),
                fields.get(8, "drain", Boolean.class),
                fields.get(9, "echo", Boolean.class),
                fields.symbolKeyed(10, "properties"));
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
        return Fields.list(Fields.uint(nextIncomingId), Fields.uint(incomingWindow), Fields.uint(nextOutgoingId),
                Fields.uint(outgoingWindow), Fields.uint(handle), Fields.uint(deliveryCount),
                Fields.uint(linkCredit), Fields.uint(available), drain, echo, properties);
    }
}
