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

import io.vidocq.tassis.amqp.internal.security.SaslBody;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.DescribedType;

/**
 * A frame (part 2, §2.3).
 *
 * @param type {@link #TYPE_AMQP} or {@link #TYPE_SASL}
 * @param channel the channel of an AMQP frame; 0 for a SASL frame, which has none
 * @param body a {@link Performative} in an AMQP frame, a {@link SaslBody} in a SASL frame, or
 *        {@code null} for the empty AMQP frame that only keeps the connection alive (§2.4.5)
 * @param payload what follows the body: the octets of a message in a transfer, otherwise empty
 */
public record Frame(int type, int channel, DescribedType body, Binary payload) {

    public static final int TYPE_AMQP = 0x00;
    public static final int TYPE_SASL = 0x01;

    private static final Binary NO_PAYLOAD = new Binary(new byte[0]);

    /** The frame a peer sends when it has nothing to say, so as not to be taken for dead. */
    public static final Frame EMPTY = new Frame(TYPE_AMQP, 0, null, NO_PAYLOAD);

    public Frame {
        if (channel < 0 || channel > 0xFFFF) {
            throw new IllegalArgumentException(channel + " is not a channel number");
        }
        boolean fits = switch (type) {
            case TYPE_AMQP -> body == null || body instanceof Performative;
            case TYPE_SASL -> body instanceof SaslBody && channel == 0;
            default -> false;
        };
        if (!fits) {
            throw new IllegalArgumentException("a frame of type " + type + " cannot carry " + body);
        }
        if (body == null && payload.length() > 0) {
            throw new IllegalArgumentException("an empty frame has no payload");
        }
    }

    public static Frame amqp(int channel, Performative performative) {
        return new Frame(TYPE_AMQP, channel, performative, NO_PAYLOAD);
    }

    public static Frame amqp(int channel, Performative performative, Binary payload) {
        return new Frame(TYPE_AMQP, channel, performative, payload);
    }

    public static Frame sasl(SaslBody body) {
        return new Frame(TYPE_SASL, 0, body, NO_PAYLOAD);
    }

    public boolean isEmpty() {
        return body == null;
    }
}
