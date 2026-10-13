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

import java.util.Arrays;
import java.util.HexFormat;

/**
 * An AMQP {@code binary}: a sequence of octets, compared by content (§1.6.19).
 *
 * <p>It never changes. Built and read the ordinary way ({@link #Binary(byte[])},
 * {@link #bytes()}), it copies, so that nobody else holds its octets. The code that carries
 * messages does not: a body would be copied at each of the dozen hands it goes through.
 * {@link #wrap(byte[])} and {@link #array()} are for that code, under one rule: an array that
 * went into a binary or came out of one is never written to again.
 */
public final class Binary {

    private final byte[] bytes;

    public Binary(byte[] bytes) {
        this(bytes.clone(), null);
    }

    /** @param mine a marker to tell this constructor, which keeps the array, from the public one */
    private Binary(byte[] bytes, Void mine) {
        this.bytes = bytes;
    }

    /**
     * A binary of those octets, which are not copied: the caller gives the array up, and no
     * longer writes to it.
     */
    public static Binary wrap(byte[] givenUp) {
        return new Binary(givenUp, null);
    }

    public static Binary of(int... octets) {
        byte[] bytes = new byte[octets.length];
        for (int i = 0; i < octets.length; i++) {
            bytes[i] = (byte) octets[i];
        }
        return new Binary(bytes);
    }

    /** A copy of the octets, the caller's own. */
    public byte[] bytes() {
        return bytes.clone();
    }

    /** The octets themselves, to read: whoever writes to them breaks every holder of this binary. */
    public byte[] array() {
        return bytes;
    }

    public int length() {
        return bytes.length;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Binary binary && Arrays.equals(bytes, binary.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "0x" + HexFormat.of().formatHex(bytes);
    }
}
