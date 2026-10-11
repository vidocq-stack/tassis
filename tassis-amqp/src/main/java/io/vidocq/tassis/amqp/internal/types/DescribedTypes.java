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

/** Gives the described values a decoder reads the type Tassis knows them by, if it knows them. */
@FunctionalInterface
public interface DescribedTypes {

    /** Knows no type: every described value stays a {@link Described}. */
    DescribedTypes NONE = Described::new;

    /**
     * @param descriptor the descriptor read, usually a {@link ULong} or a {@link Symbol}
     * @param value the value it describes
     * @return the typed value, or a {@link Described} if the descriptor is unknown
     * @throws AmqpDecodeException if the descriptor is known and the value is not what it describes
     */
    Object resolve(Object descriptor, Object value) throws AmqpDecodeException;
}
