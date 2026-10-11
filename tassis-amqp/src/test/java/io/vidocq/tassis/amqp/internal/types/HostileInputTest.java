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
package io.vidocq.tassis.amqp.internal.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * A decoder reads octets that come from the network: whatever they are, it answers with a value
 * or with an {@link AmqpDecodeException}, never with another exception, and never by allocating
 * what a few octets merely announce.
 */
class HostileInputTest {

    private static byte[] hex(String octets) {
        return HexFormat.of().parseHex(octets.replace(" ", ""));
    }

    private static void refused(String octets) {
        assertThrows(AmqpDecodeException.class, () -> AmqpDecoder.decode(hex(octets)), octets);
    }

    @Test
    void nothingToRead() {
        refused("");
    }

    @Test
    void anUnknownFormatCode() {
        refused("3f");
        refused("46");
        refused("99 00");
        refused("c2 01 00");
    }

    @Test
    void aValueCutShort() {
        refused("70 00 00 01");
        refused("80 00");
        refused("98 01 02 03");
        refused("a1 05 61 62");
        refused("b0 00 00");
        refused("c0");
        refused("c0 05");
        refused("00");
        refused("00 53 10");
    }

    @Test
    void aSizeLargerThanWhatFollows() {
        // four gigabytes announced, nothing behind
        refused("b0 ff ff ff ff");
        refused("b1 7f ff ff ff 61");
        refused("d0 ff ff ff ff 00 00 00 00");
        refused("f0 ff ff ff ff 00 00 00 00 40");
        refused("c0 09 01 41");
    }

    @Test
    void aCountThatTheSizeCannotHold() {
        // a list of 4 billion items in 4 octets
        refused("d0 00 00 00 04 ff ff ff ff");
        refused("c0 02 05 41");
        refused("d1 00 00 00 05 00 00 00 02 41");
    }

    @Test
    void aCompoundSmallerThanItsOwnCount() {
        refused("c0 00");
        refused("d0 00 00 00 03 00 00 00");
        refused("e0 00");
        refused("e0 01 00");
    }

    @Test
    void octetsLeftInsideACompound() {
        // size says 4 octets of content, the one item uses 2
        refused("c0 04 01 41 41 41");
    }

    @Test
    void anArrayOfElementsWithoutWidthCannotAnnounceBillions() {
        // each "true" element takes no octet at all: the count is all there is to stop it
        refused("f0 00 00 00 05 ff ff ff ff 41");
        refused("f0 00 00 00 05 7f ff ff ff 40");
        refused("f0 00 00 00 05 00 10 00 00 45");
    }

    @Test
    void aMapWithAnOddCountOrADuplicateKey() {
        refused("c1 02 01 41");
        refused("c1 09 04 a1 01 61 41 a1 01 61 42");
    }

    @Test
    void textThatIsNotUtf8AndASymbolThatIsNotAscii() {
        refused("a1 02 c3 28");
        refused("a1 01 ff");
        refused("a3 01 e9");
    }

    @Test
    void aBooleanThatIsNeitherTrueNorFalse() {
        refused("56 02");
        refused("e0 03 01 56 07");
    }

    @Test
    void aCharThatIsNotACodePoint() {
        refused("73 00 11 00 00");
        refused("73 ff ff ff ff");
    }

    @Test
    void nestingDeeperThanAnyMessageNeeds() throws AmqpDecodeException {
        StringBuilder shallow = new StringBuilder();
        for (int i = 0; i < AmqpDecoder.MAX_DEPTH - 1; i++) {
            shallow.append("00 40 ");
        }
        // described values nest without costing a size: as deep as allowed is read
        AmqpDecoder.decode(hex(shallow + "40"));

        StringBuilder deep = new StringBuilder();
        for (int i = 0; i < 100_000; i++) {
            deep.append("0040");
        }
        refused(deep + "40");
    }

    @Test
    void randomOctetsNeverBreakTheDecoder() {
        Random random = new Random(20261011);
        int refused = 0;
        for (int round = 0; round < 200_000; round++) {
            byte[] octets = new byte[1 + random.nextInt(24)];
            random.nextBytes(octets);
            try {
                AmqpDecoder.decode(octets);
            } catch (AmqpDecodeException expected) {
                refused++;
            }
        }
        assertTrue(refused > 100_000, "most random octets are not a value: " + refused);
    }

    @Test
    void whatTheEncoderWritesTheDecoderReadsWhateverIsMutatedInIt() throws AmqpDecodeException {
        byte[] good = AmqpEncoder.encode(List.of("text", UInt.of(70_000), List.of(Symbol.of("s"), new Binary(new byte[3])),
                AmqpArray.of(AmqpType.LONG, List.of(1L, 2L)), new Described(ULong.of(0x70), List.of(true))));
        assertEquals(5, ((List<?>) AmqpDecoder.decode(good)).size());

        Random random = new Random(42);
        for (int round = 0; round < 100_000; round++) {
            byte[] mutated = good.clone();
            for (int flips = 1 + random.nextInt(3); flips > 0; flips--) {
                mutated[random.nextInt(mutated.length)] = (byte) random.nextInt(256);
            }
            try {
                AmqpDecoder.decode(mutated);
            } catch (AmqpDecodeException expected) {
                // the only failure allowed
            }
        }
    }
}
