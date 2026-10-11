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
package io.vidocq.tassis.server.internal;

import io.vidocq.tassis.amqp.internal.transport.AmqpError;
import io.vidocq.tassis.amqp.internal.types.Symbol;

/** What a peer did cannot be carried on: the connection is closed with this error (part 2, §2.8.15). */
final class AmqpFailure extends Exception {

    private static final long serialVersionUID = 1L;

    static final Symbol DECODE_ERROR = Symbol.of("amqp:decode-error");
    static final Symbol NOT_ALLOWED = Symbol.of("amqp:not-allowed");
    static final Symbol NOT_IMPLEMENTED = Symbol.of("amqp:not-implemented");
    static final Symbol NOT_FOUND = Symbol.of("amqp:not-found");
    static final Symbol INVALID_FIELD = Symbol.of("amqp:invalid-field");
    static final Symbol INTERNAL_ERROR = Symbol.of("amqp:internal-error");
    static final Symbol RESOURCE_LIMIT_EXCEEDED = Symbol.of("amqp:resource-limit-exceeded");
    static final Symbol FRAMING_ERROR = Symbol.of("amqp:connection:framing-error");
    static final Symbol MESSAGE_SIZE_EXCEEDED = Symbol.of("amqp:link:message-size-exceeded");
    static final Symbol ERRANT_LINK = Symbol.of("amqp:session:errant-link");
    static final Symbol RESOURCE_LOCKED = Symbol.of("amqp:resource-locked");
    static final Symbol UNKNOWN_TRANSACTION = Symbol.of("amqp:transaction:unknown-id");
    static final Symbol UNATTACHED_HANDLE = Symbol.of("amqp:session:unattached-handle");

    private final transient AmqpError error;

    AmqpFailure(Symbol condition, String description) {
        super(description);
        this.error = new AmqpError(condition, description, null);
    }

    AmqpError error() {
        return error;
    }
}
