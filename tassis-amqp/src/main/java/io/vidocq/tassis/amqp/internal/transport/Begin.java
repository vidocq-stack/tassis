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
 * Begins a session on a channel.
 *
 * <p>AMQP 1.0 part 2, §2.7.2: {@code amqp:begin:list}, descriptor 0x11. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record Begin(Integer remoteChannel, Long nextOutgoingId, Long incomingWindow, Long outgoingWindow,
        Long handleMax, List<Symbol> offeredCapabilities, List<Symbol> desiredCapabilities,
        Map<Symbol, Object> properties) implements Performative, DescribedType {

    public static final ULong CODE = ULong.of(0x11);
    public static final Symbol NAME = Symbol.of("amqp:begin:list");

    public Begin {
        if (nextOutgoingId == null) {
            throw new IllegalArgumentException("begin needs its field next-outgoing-id");
        }
        if (incomingWindow == null) {
            throw new IllegalArgumentException("begin needs its field incoming-window");
        }
        if (outgoingWindow == null) {
            throw new IllegalArgumentException("begin needs its field outgoing-window");
        }
    }

    /** Builds it from the list of fields a peer sent. */
    public static Begin of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("begin", described);
        return new Begin(fields.ushort(0, "remote-channel"),
                fields.required("next-outgoing-id", fields.uint(1, "next-outgoing-id")),
                fields.required("incoming-window", fields.uint(2, "incoming-window")),
                fields.required("outgoing-window", fields.uint(3, "outgoing-window")),
                fields.uint(4, "handle-max"),
                fields.symbols(5, "offered-capabilities"),
                fields.symbols(6, "desired-capabilities"),
                fields.symbolKeyed(7, "properties"));
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
        return Fields.list(Fields.ushort(remoteChannel), Fields.uint(nextOutgoingId), Fields.uint(incomingWindow),
                Fields.uint(outgoingWindow), Fields.uint(handleMax), Fields.symbols(offeredCapabilities),
                Fields.symbols(desiredCapabilities), properties);
    }
}
