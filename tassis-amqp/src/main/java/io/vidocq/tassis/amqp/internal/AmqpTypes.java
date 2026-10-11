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
package io.vidocq.tassis.amqp.internal;

import io.vidocq.tassis.amqp.internal.messaging.Accepted;
import io.vidocq.tassis.amqp.internal.messaging.AmqpSequence;
import io.vidocq.tassis.amqp.internal.messaging.AmqpValue;
import io.vidocq.tassis.amqp.internal.messaging.ApplicationProperties;
import io.vidocq.tassis.amqp.internal.messaging.Data;
import io.vidocq.tassis.amqp.internal.messaging.DeleteOnClose;
import io.vidocq.tassis.amqp.internal.messaging.DeleteOnNoLinks;
import io.vidocq.tassis.amqp.internal.messaging.DeleteOnNoLinksOrMessages;
import io.vidocq.tassis.amqp.internal.messaging.DeleteOnNoMessages;
import io.vidocq.tassis.amqp.internal.messaging.DeliveryAnnotations;
import io.vidocq.tassis.amqp.internal.messaging.Footer;
import io.vidocq.tassis.amqp.internal.messaging.Header;
import io.vidocq.tassis.amqp.internal.messaging.MessageAnnotations;
import io.vidocq.tassis.amqp.internal.messaging.Modified;
import io.vidocq.tassis.amqp.internal.messaging.Properties;
import io.vidocq.tassis.amqp.internal.messaging.Received;
import io.vidocq.tassis.amqp.internal.messaging.Rejected;
import io.vidocq.tassis.amqp.internal.messaging.Released;
import io.vidocq.tassis.amqp.internal.messaging.Source;
import io.vidocq.tassis.amqp.internal.messaging.Target;
import io.vidocq.tassis.amqp.internal.security.SaslChallenge;
import io.vidocq.tassis.amqp.internal.security.SaslInit;
import io.vidocq.tassis.amqp.internal.security.SaslMechanisms;
import io.vidocq.tassis.amqp.internal.security.SaslOutcome;
import io.vidocq.tassis.amqp.internal.security.SaslResponse;
import io.vidocq.tassis.amqp.internal.transactions.Coordinator;
import io.vidocq.tassis.amqp.internal.transactions.Declare;
import io.vidocq.tassis.amqp.internal.transactions.Declared;
import io.vidocq.tassis.amqp.internal.transactions.Discharge;
import io.vidocq.tassis.amqp.internal.transactions.TransactionalState;
import io.vidocq.tassis.amqp.internal.transport.AmqpError;
import io.vidocq.tassis.amqp.internal.transport.Attach;
import io.vidocq.tassis.amqp.internal.transport.Begin;
import io.vidocq.tassis.amqp.internal.transport.Close;
import io.vidocq.tassis.amqp.internal.transport.Detach;
import io.vidocq.tassis.amqp.internal.transport.Disposition;
import io.vidocq.tassis.amqp.internal.transport.End;
import io.vidocq.tassis.amqp.internal.transport.Flow;
import io.vidocq.tassis.amqp.internal.transport.Open;
import io.vidocq.tassis.amqp.internal.transport.Transfer;
import io.vidocq.tassis.amqp.internal.types.AmqpDecodeException;
import io.vidocq.tassis.amqp.internal.types.Described;
import io.vidocq.tassis.amqp.internal.types.DescribedType;
import io.vidocq.tassis.amqp.internal.types.DescribedTypes;
import io.vidocq.tassis.amqp.internal.types.Symbol;
import io.vidocq.tassis.amqp.internal.types.ULong;
import java.util.HashMap;
import java.util.Map;

/**
 * The described types of AMQP 1.0 that Tassis knows, by their numeric and by their symbolic
 * descriptor: a peer may use either (part 1, §1.3). A descriptor that is not here is not an
 * error: the value stays a {@link Described}, as the specification asks of what one does not
 * understand.
 */
public final class AmqpTypes implements DescribedTypes {

    /** Every type of the transport, messaging, transactions and security parts. */
    public static final AmqpTypes ALL = new AmqpTypes();

    /** Builds a type from the value its descriptor describes. */
    @FunctionalInterface
    private interface Factory {

        DescribedType of(Object described) throws AmqpDecodeException;
    }

    private final Map<Object, Factory> factories = new HashMap<>();

    private AmqpTypes() {
        register(Open.CODE, Open.NAME, Open::of);
        register(Begin.CODE, Begin.NAME, Begin::of);
        register(Attach.CODE, Attach.NAME, Attach::of);
        register(Flow.CODE, Flow.NAME, Flow::of);
        register(Transfer.CODE, Transfer.NAME, Transfer::of);
        register(Disposition.CODE, Disposition.NAME, Disposition::of);
        register(Detach.CODE, Detach.NAME, Detach::of);
        register(End.CODE, End.NAME, End::of);
        register(Close.CODE, Close.NAME, Close::of);
        register(AmqpError.CODE, AmqpError.NAME, AmqpError::of);
        register(Received.CODE, Received.NAME, Received::of);
        register(Accepted.CODE, Accepted.NAME, Accepted::of);
        register(Rejected.CODE, Rejected.NAME, Rejected::of);
        register(Released.CODE, Released.NAME, Released::of);
        register(Modified.CODE, Modified.NAME, Modified::of);
        register(Source.CODE, Source.NAME, Source::of);
        register(Target.CODE, Target.NAME, Target::of);
        register(DeleteOnClose.CODE, DeleteOnClose.NAME, DeleteOnClose::of);
        register(DeleteOnNoLinks.CODE, DeleteOnNoLinks.NAME, DeleteOnNoLinks::of);
        register(DeleteOnNoMessages.CODE, DeleteOnNoMessages.NAME, DeleteOnNoMessages::of);
        register(DeleteOnNoLinksOrMessages.CODE, DeleteOnNoLinksOrMessages.NAME, DeleteOnNoLinksOrMessages::of);
        register(Header.CODE, Header.NAME, Header::of);
        register(Properties.CODE, Properties.NAME, Properties::of);
        register(Coordinator.CODE, Coordinator.NAME, Coordinator::of);
        register(Declare.CODE, Declare.NAME, Declare::of);
        register(Discharge.CODE, Discharge.NAME, Discharge::of);
        register(Declared.CODE, Declared.NAME, Declared::of);
        register(TransactionalState.CODE, TransactionalState.NAME, TransactionalState::of);
        register(SaslMechanisms.CODE, SaslMechanisms.NAME, SaslMechanisms::of);
        register(SaslInit.CODE, SaslInit.NAME, SaslInit::of);
        register(SaslChallenge.CODE, SaslChallenge.NAME, SaslChallenge::of);
        register(SaslResponse.CODE, SaslResponse.NAME, SaslResponse::of);
        register(SaslOutcome.CODE, SaslOutcome.NAME, SaslOutcome::of);
        register(DeliveryAnnotations.CODE, DeliveryAnnotations.NAME, DeliveryAnnotations::of);
        register(MessageAnnotations.CODE, MessageAnnotations.NAME, MessageAnnotations::of);
        register(ApplicationProperties.CODE, ApplicationProperties.NAME, ApplicationProperties::of);
        register(Footer.CODE, Footer.NAME, Footer::of);
        register(Data.CODE, Data.NAME, Data::of);
        register(AmqpSequence.CODE, AmqpSequence.NAME, AmqpSequence::of);
        register(AmqpValue.CODE, AmqpValue.NAME, AmqpValue::of);
    }

    private void register(ULong code, Symbol name, Factory factory) {
        if (factories.put(code, factory) != null || factories.put(name, factory) != null) {
            throw new IllegalStateException("two types share the descriptor " + code + " or " + name);
        }
    }

    @Override
    public Object resolve(Object descriptor, Object value) throws AmqpDecodeException {
        Factory factory = factories.get(descriptor);
        return factory == null ? new Described(descriptor, value) : factory.of(value);
    }
}
