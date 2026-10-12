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
package io.vidocq.tassis.core.internal.client;

import jakarta.jms.IllegalStateException;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidClientIDRuntimeException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidDestinationRuntimeException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.InvalidSelectorRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.MessageNotWriteableRuntimeException;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.ResourceAllocationRuntimeException;
import jakarta.jms.TransactionInProgressException;
import jakarta.jms.TransactionInProgressRuntimeException;
import jakarta.jms.TransactionRolledBackException;
import jakarta.jms.TransactionRolledBackRuntimeException;

/**
 * The simplified API reports failures with unchecked exceptions, each the
 * counterpart of a checked one of the classic API (Jakarta Messaging 3.1 §10.2).
 */
public final class Unchecked {

    /** An action of the classic API. */
    @FunctionalInterface
    interface Action {

        void run() throws JMSException;
    }

    /** A call of the classic API that returns something. */
    @FunctionalInterface
    interface Call<T> {

        T get() throws JMSException;
    }

    private Unchecked() {
    }

    static void run(Action action) {
        try {
            action.run();
        } catch (JMSException e) {
            throw of(e);
        }
    }

    static <T> T get(Call<T> call) {
        try {
            return call.get();
        } catch (JMSException e) {
            throw of(e);
        }
    }

    /** Returns the unchecked counterpart of a checked exception, which becomes its cause. */
    public static JMSRuntimeException of(JMSException e) {
        String reason = e.getMessage();
        String code = e.getErrorCode();
        return switch (e) {
            case IllegalStateException x -> new IllegalStateRuntimeException(reason, code, e);
            case InvalidClientIDException x -> new InvalidClientIDRuntimeException(reason, code, e);
            case InvalidDestinationException x -> new InvalidDestinationRuntimeException(reason, code, e);
            case InvalidSelectorException x -> new InvalidSelectorRuntimeException(reason, code, e);
            case JMSSecurityException x -> new JMSSecurityRuntimeException(reason, code, e);
            case MessageFormatException x -> new MessageFormatRuntimeException(reason, code, e);
            case MessageNotWriteableException x -> new MessageNotWriteableRuntimeException(reason, code, e);
            case ResourceAllocationException x -> new ResourceAllocationRuntimeException(reason, code, e);
            case TransactionInProgressException x -> new TransactionInProgressRuntimeException(reason, code, e);
            case TransactionRolledBackException x -> new TransactionRolledBackRuntimeException(reason, code, e);
            default -> new JMSRuntimeException(reason, code, e);
        };
    }
}
