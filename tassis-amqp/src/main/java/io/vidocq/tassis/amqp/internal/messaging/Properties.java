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
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import io.vidocq.tassis.amqp.internal.types.Fields;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.Timestamp;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.List;

/**
 * The immutable properties of a message.
 *
 * <p>AMQP 1.0 part 3, §3.2.4: {@code amqp:properties:list}, descriptor 0x73. A field the peer left out is
 * {@code null}; the defaults the specification gives are for whoever reads the field.
 */
public record Properties(Object messageId, Binary userId, Object to, String subject, Object replyTo,
        Object correlationId, Symbol contentType, Symbol contentEncoding, Timestamp absoluteExpiryTime,
        Timestamp creationTime, String groupId, Long groupSequence,
        String replyToGroupId) implements Section, DescribedType {

    public static final ULong CODE = ULong.of(0x73);
    public static final Symbol NAME = Symbol.of("amqp:properties:list");

    /** Builds it from the list of fields a peer sent. */
    public static Properties of(Object described) throws AmqpDecodeException {
        Fields fields = Fields.of("properties", described);
        return new Properties(fields.any(0),
                fields.get(1, "user-id", Binary.class),
                fields.any(2),
                fields.get(3, "subject", String.class),
                fields.any(4),
                fields.any(5),
                fields.get(6, "content-type", Symbol.class),
                fields.get(7, "content-encoding", Symbol.class),
                fields.get(8, "absolute-expiry-time", Timestamp.class),
                fields.get(9, "creation-time", Timestamp.class),
                fields.get(10, "group-id", String.class),
                fields.uint(11, "group-sequence"),
                fields.get(12, "reply-to-group-id", String.class));
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
        return Fields.list(messageId, userId, to, subject, replyTo, correlationId, contentType, contentEncoding,
                absoluteExpiryTime, creationTime, groupId, Fields.uint(groupSequence), replyToGroupId);
    }
}
