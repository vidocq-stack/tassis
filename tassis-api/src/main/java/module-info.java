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
/**
 * Tassis API: aggregator module re-exposing the Jakarta Messaging 3.1 spec.
 * {@code jakarta.jms-api} already ships its own {@code module-info} (module
 * {@code jakarta.messaging}, package {@code jakarta.jms}), so no Vidocq fork is
 * needed. This module exists purely so sibling modules depend on
 * {@code io.vidocq.tassis.api} rather than directly on the spec artifact.
 */
module io.vidocq.tassis.api {
    requires transitive jakarta.messaging;
}
