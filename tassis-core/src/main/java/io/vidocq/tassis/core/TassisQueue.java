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
package io.vidocq.tassis.core;

import jakarta.jms.Queue;
import java.util.Objects;

/**
 * A queue of the Tassis broker: an administered object identified by its name
 * (Jakarta Messaging 3.1 §5.2). The broker creates the queue on first use.
 */
public final class TassisQueue implements Queue {

    private final String name;

    public TassisQueue(String name) {
        if (Objects.requireNonNull(name, "name").isEmpty()) {
            throw new IllegalArgumentException("a queue name must not be empty");
        }
        this.name = name;
    }

    @Override
    public String getQueueName() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TassisQueue queue && queue.name.equals(name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }
}
