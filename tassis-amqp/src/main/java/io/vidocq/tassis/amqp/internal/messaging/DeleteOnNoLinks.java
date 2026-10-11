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
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import io.vidocq.tassis.amqp.internal.types.Fields;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.List;

/**
 * A dynamic node is deleted once no link is attached to it.
 *
 * <p>AMQP 1.0 part 3, §3.5.11: {@code amqp:delete-on-no-links:list}, descriptor 0x2c. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record DeleteOnNoLinks() implements LifetimePolicy, DescribedType {

    public static final ULong CODE = ULong.of(0x2c);
    public static final Symbol NAME = Symbol.of("amqp:delete-on-no-links:list");

    /** Builds it from the list of fields a peer sent. */
    public static DeleteOnNoLinks of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("delete-on-no-links", described);
        return new DeleteOnNoLinks();
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
        return Fields.list();
    }
}
