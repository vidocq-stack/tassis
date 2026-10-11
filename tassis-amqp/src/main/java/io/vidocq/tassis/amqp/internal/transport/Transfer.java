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
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import io.vidocq.tassis.amqp.internal.types.Fields;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.List;

/**
 * Transfers a message, or a part of one, over a link. The message is the payload of the frame.
 *
 * <p>AMQP 1.0 part 2, §2.7.5: {@code amqp:transfer:list}, descriptor 0x14. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record Transfer(Long handle, Long deliveryId, Binary deliveryTag, Long messageFormat, Boolean settled,
        Boolean more, Integer rcvSettleMode, Object state, Boolean resume, Boolean aborted,
        Boolean batchable) implements Performative, DescribedType {

    public static final ULong CODE = ULong.of(0x14);
    public static final Symbol NAME = Symbol.of("amqp:transfer:list");

    public Transfer {
        if (handle == null) {
            throw new IllegalArgumentException("transfer needs its field handle");
        }
    }

    /** Builds it from the list of fields a peer sent. */
    public static Transfer of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("transfer", described);
        return new Transfer(fields.required("handle", fields.uint(0, "handle")),
                fields.uint(1, "delivery-id"),
                fields.get(2, "delivery-tag", Binary.class),
                fields.uint(3, "message-format"),
                fields.get(4, "settled", Boolean.class),
                fields.get(5, "more", Boolean.class),
                fields.ubyte(6, "rcv-settle-mode"),
                fields.any(7),
                fields.get(8, "resume", Boolean.class),
                fields.get(9, "aborted", Boolean.class),
                fields.get(10, "batchable", Boolean.class));
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
        return Fields.list(Fields.uint(handle), Fields.uint(deliveryId), deliveryTag, Fields.uint(messageFormat),
                settled, more, Fields.ubyte(rcvSettleMode), state, resume, aborted, batchable);
    }
}
