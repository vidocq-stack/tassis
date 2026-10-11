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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vidocq.tassis.amqp.internal.security.SaslInit;
import io.vidocq.tassis.amqp.internal.security.SaslOutcome;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.Binary;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/**
 * Framing (AMQP 1.0 part 2, §2.3): the 8-octet frame header, AMQP and SASL frames, the empty
 * frame that keeps a connection alive (§2.4.5), the maximum frame size (§2.7.1), and the
 * protocol headers that open a connection (§2.2, part 5 §5.2 and §5.3).
 */
class FrameCodecTest {

    private static final long UNLIMITED = 0xFFFF_FFFFL;

    private static byte[] hex(String octets) {
        return HexFormat.of().parseHex(octets.replace(" ", ""));
    }

    private static String hex(byte[] octets) {
        return HexFormat.ofDelimiter(" ").formatHex(octets);
    }

    private static Frame decode(String octets) throws AmqpDecodeException {
        return FrameCodec.decode(ByteBuffer.wrap(hex(octets)), UNLIMITED);
    }

    // ---- §2.3 --------------------------------------------------------------

    @Test
    void anAmqpFrameIsItsHeaderThenThePerformative() throws AmqpDecodeException {
        // size 12, doff 2, type 0, channel 0, then close
        String octets = "00 00 00 0c 02 00 00 00 00 53 18 45";

        assertEquals(octets, hex(FrameCodec.encode(Frame.amqp(0, new Close(null)), UNLIMITED)));
        assertEquals(Frame.amqp(0, new Close(null)), decode(octets));
    }

    @Test
    void theChannelIsInTheHeader() throws AmqpDecodeException {
        String octets = "00 00 00 0c 02 00 01 02 00 53 17 45";

        assertEquals(octets, hex(FrameCodec.encode(Frame.amqp(0x0102, new End(null)), UNLIMITED)));
        assertEquals(0x0102, decode(octets).channel());
        assertEquals(octets, hex(FrameCodec.encode(decode(octets), UNLIMITED)));
        assertEquals("00 00 00 0c 02 00 ff ff 00 53 17 45",
                hex(FrameCodec.encode(Frame.amqp(0xFFFF, new End(null)), UNLIMITED)));
    }

    @Test
    void whatFollowsThePerformativeIsThePayload() throws AmqpDecodeException {
        Transfer transfer = new Transfer(0L, null, null, null, null, null, null, null, null, null, null);
        String octets = "00 00 00 12 02 00 00 05 00 53 14 c0 02 01 43 de ad be";

        Frame frame = Frame.amqp(5, transfer, Binary.of(0xde, 0xad, 0xbe));

        assertEquals(octets, hex(FrameCodec.encode(frame, UNLIMITED)));
        assertEquals(frame, decode(octets));
        assertEquals(Binary.of(), decode("00 00 00 0c 02 00 00 00 00 53 18 45").payload());
    }

    @Test
    void anEmptyFrameKeepsTheConnectionAlive() throws AmqpDecodeException {
        // §2.4.5: a frame with no body
        assertEquals("00 00 00 08 02 00 00 00", hex(FrameCodec.encode(Frame.EMPTY, UNLIMITED)));
        Frame frame = decode("00 00 00 08 02 00 00 00");
        assertNull(frame.body());
        assertTrue(frame.isEmpty());
        assertEquals(Frame.EMPTY, frame);
    }

    @Test
    void aSaslFrameHasItsOwnType() throws AmqpDecodeException {
        // part 5 §5.3.2: type 1, octets 6 and 7 ignored
        SaslOutcome outcome = new SaslOutcome(0, null);
        String octets = "00 00 00 10 02 01 00 00 00 53 44 c0 03 01 50 00";

        assertEquals(octets, hex(FrameCodec.encode(Frame.sasl(outcome), UNLIMITED)));
        assertEquals(Frame.sasl(outcome), decode(octets));
        assertEquals(Frame.sasl(outcome), decode("00 00 00 10 02 01 12 34 00 53 44 c0 03 01 50 00"));
    }

    @Test
    void anExtendedHeaderIsSkipped() throws AmqpDecodeException {
        // doff 3: four octets of extended header, which 1.0 defines no use for (§2.3.1)
        assertEquals(Frame.amqp(0, new Close(null)), decode("00 00 00 10 03 00 00 00 aa bb cc dd 00 53 18 45"));
    }

    @Test
    void eachKindOfFrameCarriesItsOwnKindOfBody() {
        // a SASL body in an AMQP frame, a performative in a SASL frame, a SASL frame without body
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 10 02 00 00 00 00 53 44 c0 03 01 50 00"));
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 0c 02 01 00 00 00 53 18 45"));
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 08 02 01 00 00"));
        // a body that is a plain value, and one that Tassis does not know
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 09 02 00 00 00 41"));
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 0c 02 00 00 00 00 53 99 45"));
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 0c 02 02 00 00 00 53 18 45"));
        assertThrows(IllegalArgumentException.class, () -> Frame.amqp(70_000, new Close(null)));
    }

    @Test
    void aMalformedHeaderIsRefused() {
        // §2.3.1: size below 8, doff below 2, doff beyond the frame
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 07 02 00 00 00"));
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 08 01 00 00 00"));
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 08 03 00 00 00"));
        // the size does not match what was handed over
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 0c 02 00 00 00 00 53 18"));
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00 0b 02 00 00 00 00 53 18 45"));
        assertThrows(AmqpDecodeException.class, () -> decode("00 00 00"));
    }

    @Test
    void aFrameLargerThanAgreedIsRefusedBothWays() throws AmqpDecodeException {
        // §2.7.1: before open is exchanged the limit is 512
        assertEquals(512, FrameCodec.MIN_MAX_FRAME_SIZE);
        Frame large = Frame.amqp(0, new Transfer(0L, null, null, null, null, null, null, null, null, null, null),
                new Binary(new byte[600]));

        assertThrows(IllegalArgumentException.class, () -> FrameCodec.encode(large, 512));
        byte[] encoded = FrameCodec.encode(large, UNLIMITED);
        assertThrows(AmqpDecodeException.class, () -> FrameCodec.decode(ByteBuffer.wrap(encoded), 512));
        assertEquals(large, FrameCodec.decode(ByteBuffer.wrap(encoded), 4096));
    }

    @Test
    void theSizeOfAFrameIsKnownFromItsFirstFourOctets() throws AmqpDecodeException {
        assertEquals(-1, FrameCodec.size(ByteBuffer.wrap(hex("00 00 00")), UNLIMITED));
        assertEquals(12, FrameCodec.size(ByteBuffer.wrap(hex("00 00 00 0c 02")), UNLIMITED));
        ByteBuffer peeked = ByteBuffer.wrap(hex("00 00 00 0c"));
        FrameCodec.size(peeked, UNLIMITED);
        assertEquals(0, peeked.position(), "looking at the size reads nothing");
        // announced sizes are checked before anything is buffered for them
        assertThrows(AmqpDecodeException.class, () -> FrameCodec.size(ByteBuffer.wrap(hex("7f ff ff ff")), 512));
        assertThrows(AmqpDecodeException.class, () -> FrameCodec.size(ByteBuffer.wrap(hex("00 00 00 04")), 512));
    }

    @Test
    void aDecodedFrameLeavesTheBufferAfterIt() throws AmqpDecodeException {
        ByteBuffer two = ByteBuffer.wrap(hex("00 00 00 0c 02 00 00 00 00 53 18 45 00 00 00 08 02 00 00 00"));

        Frame first = FrameCodec.decode(two.slice(0, 12), UNLIMITED);

        assertEquals(new Close(null), first.body());
    }

    // ---- protocol headers --------------------------------------------------

    @Test
    void theProtocolHeadersAreEightOctets() throws AmqpDecodeException {
        // "AMQP", the protocol id, then the version 1.0.0 (§2.2; part 5 §5.2.1 and §5.3.1)
        assertEquals("41 4d 51 50 00 01 00 00", hex(ProtocolHeader.AMQP.octets()));
        assertEquals("41 4d 51 50 02 01 00 00", hex(ProtocolHeader.TLS.octets()));
        assertEquals("41 4d 51 50 03 01 00 00", hex(ProtocolHeader.SASL.octets()));
        assertEquals(ProtocolHeader.SASL, ProtocolHeader.parse(hex("41 4d 51 50 03 01 00 00")));
        assertEquals(new ProtocolHeader(0, 0, 9, 1), ProtocolHeader.parse(hex("41 4d 51 50 00 00 09 01")));
        assertArrayEquals(ProtocolHeader.AMQP.octets(), ProtocolHeader.parse(ProtocolHeader.AMQP.octets()).octets());
    }

    @Test
    void whatIsNotAProtocolHeaderIsRefused() {
        assertThrows(AmqpDecodeException.class, () -> ProtocolHeader.parse(hex("47 45 54 20 2f 20 48 54")));
        assertThrows(AmqpDecodeException.class, () -> ProtocolHeader.parse(hex("41 4d 51 50 00 01 00")));
    }

    @Test
    void aSaslInitTravelsInASaslFrame() throws AmqpDecodeException {
        SaslInit init = new SaslInit(Symbol.of("ANONYMOUS"), null, null);

        Frame read = FrameCodec.decode(ByteBuffer.wrap(FrameCodec.encode(Frame.sasl(init), 512)), 512);

        assertEquals(init, read.body());
        assertEquals(Frame.TYPE_SASL, read.type());
    }
}
