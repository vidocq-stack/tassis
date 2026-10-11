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
package io.vidocq.tassis.interop;

import java.nio.ByteBuffer;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;

/** The codec of Apache Qpid Proton-J, the implementation Tassis compares its own with. */
final class Proton {

    private Proton() {
    }

    private static DecoderImpl decoder() {
        DecoderImpl decoder = new DecoderImpl();
        EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
        return decoder;
    }

    static Object decode(byte[] octets) {
        DecoderImpl decoder = decoder();
        ByteBuffer buffer = ByteBuffer.wrap(octets);
        decoder.setByteBuffer(buffer);
        Object value = decoder.readObject();
        if (buffer.hasRemaining()) {
            throw new AssertionError("Proton-J left " + buffer.remaining() + " octets unread");
        }
        return value;
    }

    static byte[] encode(Object value) {
        DecoderImpl decoder = new DecoderImpl();
        EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
        ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
        encoder.setByteBuffer(buffer);
        encoder.writeObject(value);
        byte[] octets = new byte[buffer.position()];
        buffer.flip();
        buffer.get(octets);
        return octets;
    }
}
