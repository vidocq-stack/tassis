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
package io.vidocq.tassis.amqp.internal.jms;

import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.ULong;

/**
 * The names by which a Jakarta Messaging client and a broker understand each other over
 * AMQP 1.0, which defines none of them: those of Apache Qpid JMS, which the brokers that serve
 * JMS clients follow. The server reads them and the client writes them, from this one place.
 */
public final class Conventions {

    /** The mechanisms of the security layer (part 5, §5.3). */
    public static final Symbol PLAIN = Symbol.of("PLAIN");
    public static final Symbol ANONYMOUS = Symbol.of("ANONYMOUS");

    /** Connection capabilities. */
    public static final Symbol ANONYMOUS_RELAY = Symbol.of("ANONYMOUS-RELAY");
    public static final Symbol SOLE_CONNECTION = Symbol.of("sole-connection-for-container");
    public static final Symbol SHARED_SUBS = Symbol.of("SHARED-SUBS");
    public static final Symbol DELAYED_DELIVERY = Symbol.of("DELAYED_DELIVERY");
    /** The property of an open that says the connection is about to be refused. */
    public static final Symbol CONNECTION_ESTABLISHMENT_FAILED = Symbol.of("amqp:connection-establishment-failed");

    /** What a source or a target says it is, by a capability. */
    public static final Symbol CAPABILITY_QUEUE = Symbol.of("queue");
    public static final Symbol CAPABILITY_TOPIC = Symbol.of("topic");
    public static final Symbol CAPABILITY_TEMPORARY_QUEUE = Symbol.of("temporary-queue");
    public static final Symbol CAPABILITY_TEMPORARY_TOPIC = Symbol.of("temporary-topic");
    public static final Symbol CAPABILITY_SHARED = Symbol.of("shared");
    public static final Symbol CAPABILITY_GLOBAL = Symbol.of("global");

    /** The distribution mode of a link that looks at a queue without consuming it. */
    public static final Symbol DISTRIBUTION_COPY = Symbol.of("copy");
    /** The expiry policy of the source of a durable subscription. */
    public static final Symbol EXPIRY_NEVER = Symbol.of("never");
    /** What the coordinator of the server can do (part 4, §4.5.1). */
    public static final Symbol LOCAL_TRANSACTIONS = Symbol.of("amqp:local-transactions");

    /** The filters of a source, and the descriptors of what they hold. */
    public static final Symbol FILTER_SELECTOR = Symbol.of("jms-selector");
    public static final Symbol FILTER_NO_LOCAL = Symbol.of("no-local");
    public static final ULong SELECTOR_DESCRIPTOR = ULong.of(0x0000468C00000004L);
    public static final ULong NO_LOCAL_DESCRIPTOR = ULong.of(0x0000468C00000003L);

    /** What separates the name of a subscription from what tells its links apart, in a link name. */
    public static final char SUBSCRIPTION_NAME_END = '|';
    /** What follows that separator in the name of a link whose subscription name is the broker's. */
    public static final String GLOBAL_SUFFIX = "global";

    /** How a Tassis broker names the temporary destinations it creates. */
    public static final String TEMPORARY_QUEUE_NAME = "temporary-queue:";
    public static final String TEMPORARY_TOPIC_NAME = "temporary-topic:";

    private Conventions() {
    }
}
