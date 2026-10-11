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
package io.vidocq.tassis.interop;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * A certificate made for a test, and the two TLS contexts that go with it. The JDK has no API
 * that issues a certificate, so its own {@code keytool} is run: no key is kept in the
 * repository, and no library is needed.
 *
 * @param server shows the certificate: what a {@code TassisServer} is given
 * @param client trusts that certificate and nothing else: what a client is given
 */
record TestCertificates(SSLContext server, SSLContext client) {

    private static final char[] PASSWORD = "changeit".toCharArray();

    /** A self-signed certificate issued to that DNS name. */
    static TestCertificates issuedTo(String name) {
        try {
            Path directory = Files.createTempDirectory("tassis-tls");
            Path store = directory.resolve("server.p12");
            Path output = directory.resolve("keytool.txt");
            Process keytool = null;
            try {
                keytool = new ProcessBuilder(
                        Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                        "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                        "-dname", "CN=" + name, "-ext", "san=dns:" + name,
                        "-keystore", store.toString(), "-storetype", "PKCS12",
                        "-storepass", new String(PASSWORD), "-keypass", new String(PASSWORD))
                        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
                // To a file, read afterwards: a keytool that hangs must not hang the test with it.
                if (!keytool.waitFor(60, TimeUnit.SECONDS) || keytool.exitValue() != 0) {
                    throw new IllegalStateException("keytool could not make a certificate: "
                            + Files.readString(output, StandardCharsets.UTF_8));
                }
                KeyStore keys = KeyStore.getInstance("PKCS12");
                try (InputStream in = Files.newInputStream(store)) {
                    keys.load(in, PASSWORD);
                }
                KeyManagerFactory showing = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                showing.init(keys, PASSWORD);
                SSLContext server = SSLContext.getInstance("TLS");
                server.init(showing.getKeyManagers(), null, null);

                KeyStore trusted = KeyStore.getInstance("PKCS12");
                trusted.load(null, null);
                trusted.setCertificateEntry("server", keys.getCertificate("server"));
                TrustManagerFactory trusting = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trusting.init(trusted);
                SSLContext client = SSLContext.getInstance("TLS");
                client.init(null, trusting.getTrustManagers(), null);
                return new TestCertificates(server, client);
            } finally {
                if (keytool != null && keytool.isAlive()) {
                    keytool.destroyForcibly();
                }
                Files.deleteIfExists(output);
                Files.deleteIfExists(store);
                Files.deleteIfExists(directory);
            }
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("no certificate for the test", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while making a certificate", e);
        }
    }
}
