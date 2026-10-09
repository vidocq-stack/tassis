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

import jakarta.jms.ConnectionMetaData;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/** What the provider says about itself (Jakarta Messaging 3.1 §6.1.6). */
final class TassisConnectionMetaData implements ConnectionMetaData {

    static final TassisConnectionMetaData INSTANCE = new TassisConnectionMetaData();

    private final String providerVersion;
    private final int providerMajorVersion;
    private final int providerMinorVersion;

    private TassisConnectionMetaData() {
        // The module version is the build's project version; absent when run from the class path.
        var descriptor = TassisConnectionMetaData.class.getModule().getDescriptor();
        providerVersion = descriptor == null ? "0.0" : descriptor.rawVersion().orElse("0.0");
        String[] parts = providerVersion.split("[.-]");
        providerMajorVersion = number(parts, 0);
        providerMinorVersion = number(parts, 1);
    }

    private static int number(String[] parts, int index) {
        try {
            return index < parts.length ? Integer.parseInt(parts[index]) : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public String getJMSVersion() {
        return "3.1";
    }

    @Override
    public int getJMSMajorVersion() {
        return 3;
    }

    @Override
    public int getJMSMinorVersion() {
        return 1;
    }

    @Override
    public String getJMSProviderName() {
        return "Tassis";
    }

    @Override
    public String getProviderVersion() {
        return providerVersion;
    }

    @Override
    public int getProviderMajorVersion() {
        return providerMajorVersion;
    }

    @Override
    public int getProviderMinorVersion() {
        return providerMinorVersion;
    }

    /**
     * {@code JMSXDeliveryCount} is set by the provider on every delivery (§3.5.11).
     * {@code JMSXGroupID} and {@code JMSXGroupSeq} are set by the client and carried like any
     * other property; Tassis attaches no delivery guarantee to them.
     */
    @Override
    public Enumeration<String> getJMSXPropertyNames() {
        return Collections.enumeration(List.of("JMSXDeliveryCount", "JMSXGroupID", "JMSXGroupSeq"));
    }
}
