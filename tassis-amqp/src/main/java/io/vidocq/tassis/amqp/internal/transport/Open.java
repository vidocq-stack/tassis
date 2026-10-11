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
 * Negotiates the parameters of a connection: the first frame each peer sends.
 *
 * <p>AMQP 1.0 part 2, §2.7.1: {@code amqp:open:list}, descriptor 0x10. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record Open(String containerId, String hostname, Long maxFrameSize, Integer channelMax, Long idleTimeOut,
        List<Symbol> outgoingLocales, List<Symbol> incomingLocales, List<Symbol> offeredCapabilities,
        List<Symbol> desiredCapabilities, Map<Symbol, Object> properties) implements Performative, DescribedType {

    public static final ULong CODE = ULong.of(0x10);
    public static final Symbol NAME = Symbol.of("amqp:open:list");

    public Open {
        if (containerId == null) {
            throw new IllegalArgumentException("open needs its field container-id");
        }
    }

    /** Builds it from the list of fields a peer sent. */
    public static Open of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("open", described);
        return new Open(fields.required("container-id", fields.get(0, "container-id", String.class)),
                fields.get(1, "hostname", String.class),
                fields.uint(2, "max-frame-size"),
                fields.ushort(3, "channel-max"),
                fields.uint(4, "idle-time-out"),
                fields.symbols(5, "outgoing-locales"),
                fields.symbols(6, "incoming-locales"),
                fields.symbols(7, "offered-capabilities"),
                fields.symbols(8, "desired-capabilities"),
                fields.symbolKeyed(9, "properties"));
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
        return Fields.list(containerId, hostname, Fields.uint(maxFrameSize), Fields.ushort(channelMax),
                Fields.uint(idleTimeOut), Fields.symbols(outgoingLocales), Fields.symbols(incomingLocales),
                Fields.symbols(offeredCapabilities), Fields.symbols(desiredCapabilities), properties);
    }
}
