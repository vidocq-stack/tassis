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
package io.vidocq.tassis.tck;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.sun.ts.tests.jms.common.JmsTool;
import org.junit.jupiter.api.Test;

/**
 * Checks the wiring between the TCK and Tassis, outside any TCK test: the TCK
 * tests swallow the exception of a failed setup, this one lets it through.
 */
class PortingSmokeTest {

    @Test
    void jmsToolSetsUpAQueueClient() throws Exception {
        JmsTool tool = new JmsTool(JmsTool.QUEUE, System.getProperty("user"), System.getProperty("password"),
                System.getProperty("platform.mode"));
        try {
            assertNotNull(tool.getDefaultQueueConnection());
            assertNotNull(tool.getDefaultQueue());
        } finally {
            tool.closeAllConnections(new java.util.ArrayList<>());
        }
    }
}
