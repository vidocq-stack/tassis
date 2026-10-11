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

/**
 * A described type Tassis knows: a performative, a section of a message, a delivery state...
 * On the wire it is its descriptor followed by the value it describes (§1.2, §1.3).
 */
public interface DescribedType {

    /** The numeric descriptor, which is the one written. */
    ULong descriptorCode();

    /** The symbolic descriptor, which a peer may send instead of the numeric one. */
    Symbol descriptorName();

    /** The value described: a list of fields for a composite type, otherwise what the type restricts. */
    Object describedValue();
}
