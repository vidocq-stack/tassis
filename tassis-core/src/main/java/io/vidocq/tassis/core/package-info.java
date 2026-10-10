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
 * The administered objects of Tassis, a Jakarta Messaging 3.1 provider: what an
 * application creates to reach a broker and to name its destinations.
 *
 * <ul>
 * <li>{@link io.vidocq.tassis.core.TassisConnectionFactory}, the connection factory, configured
 * in code, by settings or by a connection URL;</li>
 * <li>{@link io.vidocq.tassis.core.TassisQueue} and {@link io.vidocq.tassis.core.TassisTopic},
 * the destinations, which exist on the broker from their first use.</li>
 * </ul>
 *
 * <p>A Java SE application creates them with {@code new}; no JNDI lookup is involved. Everything
 * else an application uses is the {@code jakarta.jms} API, which this module makes readable.
 * The classes that implement it live in packages this module does not export.
 */
package io.vidocq.tassis.core;
