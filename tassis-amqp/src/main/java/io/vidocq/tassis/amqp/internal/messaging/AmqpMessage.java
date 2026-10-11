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

import io.vidocq.tassis.amqp.internal.AmqpTypes;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.AmqpDecoder;
import io.vidocq.tassis.amqp.internal.types.AmqpEncoder;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A message in the format AMQP 1.0 defines (part 3, §3.2), message format 0: its sections,
 * encoded one after the other in this order. A section the message does not have is
 * {@code null}.
 *
 * @param body one or more {@link Data}, or one or more {@link AmqpSequence}, or one
 *        {@link AmqpValue}; empty for the message without a body that some peers send
 */
public record AmqpMessage(Header header, DeliveryAnnotations deliveryAnnotations,
        MessageAnnotations messageAnnotations, Properties properties, ApplicationProperties applicationProperties,
        List<Section> body, Footer footer) {

    public AmqpMessage {
        body = List.copyOf(body);
        String wrong = wrongBody(body);
        if (wrong != null) {
            throw new IllegalArgumentException(wrong);
        }
    }

    /** A message that is only its body. */
    public static AmqpMessage ofBody(Section... body) {
        return new AmqpMessage(null, null, null, null, null, List.of(body), null);
    }

    /** Says what is wrong with a body, or returns {@code null} if it is one of the three forms. */
    private static String wrongBody(List<Section> body) {
        if (body.isEmpty()) {
            return null;
        }
        Section first = body.get(0);
        if (!(first instanceof Data || first instanceof AmqpSequence || first instanceof AmqpValue)) {
            return first.descriptorName() + " is not a body section";
        }
        if (first instanceof AmqpValue && body.size() > 1) {
            return "a body is one amqp-value, not " + body.size() + " sections";
        }
        for (Section section : body) {
            if (section.getClass() != first.getClass()) {
                return "a body is made of sections of one kind, not of " + first.descriptorName() + " and "
                        + section.descriptorName();
            }
        }
        return null;
    }

    /** The octets of the message: what a transfer carries as its payload. */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        List<Section> sections = new ArrayList<>();
        sections.add(header);
        sections.add(deliveryAnnotations);
        sections.add(messageAnnotations);
        sections.add(properties);
        sections.add(applicationProperties);
        sections.addAll(body);
        sections.add(footer);
        for (Section section : sections) {
            if (section != null) {
                out.writeBytes(AmqpEncoder.encode(section));
            }
        }
        return out.toByteArray();
    }

    /** Where each kind of section comes in a message; the three kinds of body share a place. */
    private static int place(Section section) {
        return switch (section) {
            case Header h -> 0;
            case DeliveryAnnotations d -> 1;
            case MessageAnnotations m -> 2;
            case Properties p -> 3;
            case ApplicationProperties a -> 4;
            case Data d -> 5;
            case AmqpSequence s -> 5;
            case AmqpValue v -> 5;
            case Footer f -> 6;
        };
    }

    /** Reads a message from all of {@code octets}. */
    public static AmqpMessage decode(byte[] octets) throws AmqpDecodeException {
        AmqpDecoder decoder = new AmqpDecoder(ByteBuffer.wrap(octets), AmqpTypes.ALL);
        Section[] single = new Section[7];
        List<Section> body = new ArrayList<>();
        int last = -1;
        while (decoder.hasRemaining()) {
            if (!(decoder.read() instanceof Section section)) {
                throw new AmqpDecodeException("a message is made of sections, and of nothing else");
            }
            int place = place(section);
            if (place < last || place == last && place != 5) {
                throw new AmqpDecodeException(
                        "the section " + section.descriptorName() + " is repeated or out of its place");
            }
            last = place;
            if (place == 5) {
                body.add(section);
            } else {
                single[place] = section;
            }
        }
        String wrong = wrongBody(body);
        if (wrong != null) {
            throw new AmqpDecodeException(wrong);
        }
        return new AmqpMessage((Header) single[0], (DeliveryAnnotations) single[1], (MessageAnnotations) single[2],
                (Properties) single[3], (ApplicationProperties) single[4], body, (Footer) single[6]);
    }
}
