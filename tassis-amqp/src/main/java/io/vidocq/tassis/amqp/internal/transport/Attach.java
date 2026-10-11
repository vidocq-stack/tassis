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
 * Attaches a link to a session.
 *
 * <p>AMQP 1.0 part 2, §2.7.3: {@code amqp:attach:list}, descriptor 0x12. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record Attach(String name, Long handle, Boolean role, Integer sndSettleMode, Integer rcvSettleMode,
        Object source, Object target, Map<Object, Object> unsettled, Boolean incompleteUnsettled,
        Long initialDeliveryCount, ULong maxMessageSize, List<Symbol> offeredCapabilities,
        List<Symbol> desiredCapabilities, Map<Symbol, Object> properties) implements Performative, DescribedType {

    public static final ULong CODE = ULong.of(0x12);
    public static final Symbol NAME = Symbol.of("amqp:attach:list");

    public Attach {
        if (name == null) {
            throw new IllegalArgumentException("attach needs its field name");
        }
        if (handle == null) {
            throw new IllegalArgumentException("attach needs its field handle");
        }
        if (role == null) {
            throw new IllegalArgumentException("attach needs its field role");
        }
    }

    /** Builds it from the list of fields a peer sent. */
    public static Attach of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("attach", described);
        return new Attach(fields.required("name", fields.get(0, "name", String.class)),
                fields.required("handle", fields.uint(1, "handle")),
                fields.required("role", fields.get(2, "role", Boolean.class)),
                fields.ubyte(3, "snd-settle-mode"),
                fields.ubyte(4, "rcv-settle-mode"),
                fields.any(5),
                fields.any(6),
                fields.map(7, "unsettled"),
                fields.get(8, "incomplete-unsettled", Boolean.class),
                fields.uint(9, "initial-delivery-count"),
                fields.get(10, "max-message-size", ULong.class),
                fields.symbols(11, "offered-capabilities"),
                fields.symbols(12, "desired-capabilities"),
                fields.symbolKeyed(13, "properties"));
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
        return Fields.list(name, Fields.uint(handle), role, Fields.ubyte(sndSettleMode),
                Fields.ubyte(rcvSettleMode), source, target, unsettled, incompleteUnsettled,
                Fields.uint(initialDeliveryCount), maxMessageSize, Fields.symbols(offeredCapabilities),
                Fields.symbols(desiredCapabilities), properties);
    }
}
