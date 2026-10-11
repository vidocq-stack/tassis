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

import io.vidocq.tassis.core.internal.broker.BrokerConnection;
import io.vidocq.tassis.core.internal.broker.Transports;
import io.vidocq.tassis.core.internal.client.Domain;
import io.vidocq.tassis.core.internal.client.TassisConnection;
import io.vidocq.tassis.core.internal.client.TassisContext;
import io.vidocq.tassis.core.internal.settings.Settings;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidClientIDRuntimeException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * The Tassis {@link ConnectionFactory}: the administered object a client uses to
 * reach an in-VM broker (Jakarta Messaging 3.1 §5.3). A Java SE client
 * instantiates it directly; no JNDI lookup is involved.
 *
 * <p>A broker is identified by its name within the JVM: factories created with
 * the same name connect to the same broker, which is created on first use.
 *
 * <p>A factory is configured in code, with the constructors and the {@code with...} methods, or
 * from outside the application: settings by key ({@link #from(Map)}, {@link #from(Properties)},
 * {@link #named(String, Map)}) or a connection URL ({@link #fromUrl(String)}).
 *
 * <table>
 * <caption>Settings</caption>
 * <tr><th>Key</th><th>URL</th><th>Default</th><th>Meaning</th></tr>
 * <tr><td>{@code tassis.url}</td><td></td><td></td><td>a connection URL holding the settings
 * below; a setting given by its own key overrides it</td></tr>
 * <tr><td>{@code tassis.broker}</td><td>{@code tassis:vm:<broker>}</td><td>{@code default}</td>
 * <td>the name of the in-VM broker</td></tr>
 * <tr><td>{@code tassis.server}</td><td>{@code tassis:amqp://<host>[:<port>]}</td><td>none</td>
 * <td>a broker of the network, opened by a {@code TassisServer}, as {@code amqp://<host>[:<port>]}
 * (port 5672 unless told otherwise); needs the module {@code io.vidocq.tassis.amqp}. Without
 * it, the broker is the in-VM one</td></tr>
 * <tr><td>{@code tassis.clientId}</td><td>{@code clientId}</td><td>none</td>
 * <td>the client identifier of the connections (§6.1.2)</td></tr>
 * <tr><td>{@code tassis.listener.maxDeliveries}</td><td>{@code listener.maxDeliveries}</td>
 * <td>10</td><td>deliveries of a message to a listener that keeps failing before the message
 * is dropped (§8.7)</td></tr>
 * </table>
 *
 * <p>The user name and the password a connection is created with are checked by the broker
 * once users are declared on it, with {@link TassisBroker}; until then any are accepted
 * (§6.1.1 leaves authentication to the provider).
 */
public final class TassisConnectionFactory
        implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {

    /** Name of the broker the no-argument constructor connects to. */
    public static final String DEFAULT_BROKER = Settings.DEFAULT_BROKER;

    private final Settings settings;

    public TassisConnectionFactory() {
        this(Settings.DEFAULTS);
    }

    public TassisConnectionFactory(String brokerName) {
        this(Settings.DEFAULTS.withBroker(Objects.requireNonNull(brokerName, "brokerName")));
    }

    private TassisConnectionFactory(Settings settings) {
        this.settings = settings;
    }

    /**
     * Returns a factory configured by the settings whose key starts with {@code tassis.}; the
     * other entries are ignored.
     *
     * @throws IllegalArgumentException if a {@code tassis.} key is unknown or its value invalid
     */
    public static TassisConnectionFactory from(Map<String, String> settings) {
        return new TassisConnectionFactory(Settings.from(settings, null));
    }

    /**
     * Like {@link #from(Map)}, for properties: those of a file, or the system properties, as in
     * {@code TassisConnectionFactory.from(System.getProperties())}.
     */
    public static TassisConnectionFactory from(Properties properties) {
        return new TassisConnectionFactory(Settings.from(strings(properties), null));
    }

    /**
     * Returns the factory of that name: its settings are the keys
     * {@code tassis.factory.<name>.<key>}, completed by the settings that carry no name.
     *
     * @throws IllegalArgumentException if the name is empty, or a key unknown or its value invalid
     */
    public static TassisConnectionFactory named(String name, Map<String, String> settings) {
        return new TassisConnectionFactory(Settings.from(settings, Objects.requireNonNull(name, "name")));
    }

    /** Like {@link #named(String, Map)}, for properties. */
    public static TassisConnectionFactory named(String name, Properties properties) {
        return new TassisConnectionFactory(Settings.from(strings(properties), Objects.requireNonNull(name, "name")));
    }

    /**
     * Returns a factory configured by a connection URL: {@code tassis:vm:<broker>} for a broker of
     * this JVM or {@code tassis:amqp://<host>[:<port>]} for one of the network, followed by
     * {@code [?clientId=...&listener.maxDeliveries=...]}, values percent-encoded.
     *
     * @throws IllegalArgumentException if the URL is not one, or names an unknown parameter
     */
    public static TassisConnectionFactory fromUrl(String url) {
        return new TassisConnectionFactory(Settings.fromUrl(url));
    }

    private static Map<String, String> strings(Properties properties) {
        Map<String, String> strings = new LinkedHashMap<>();
        for (String name : properties.stringPropertyNames()) {
            strings.put(name, properties.getProperty(name));
        }
        return strings;
    }

    /**
     * Returns a factory for the same broker whose connections are created with this client
     * identifier, the preferred way to assign one (§6.1.2). Such a connection rejects
     * {@code setClientID}, and only one of them can be open at a time.
     */
    public TassisConnectionFactory withClientID(String clientId) {
        return new TassisConnectionFactory(settings.withClientId(Objects.requireNonNull(clientId, "clientId")));
    }

    /**
     * Returns a factory for the same broker whose sessions deliver a message at most this many
     * times to a listener that keeps failing, then drop it (§8.7 leaves the number to the
     * provider). The default is 10.
     */
    public TassisConnectionFactory withMaxListenerDeliveries(int maximum) {
        return new TassisConnectionFactory(settings.withMaxListenerDeliveries(maximum));
    }

    /** The connection URL of this factory: {@link #fromUrl(String)} gives an equivalent factory. */
    public String toUrl() {
        return settings.toUrl();
    }

    @Override
    public String toString() {
        return toUrl();
    }

    /**
     * @param userName the user name the connection is created with, or {@code null} for none
     * @throws JMSSecurityException if the broker rejects the credentials (§6.1.1, §10.3)
     */
    private TassisConnection connect(Domain domain, String userName, String password) throws JMSException {
        BrokerConnection link = Transports.connect(settings, userName, password);
        try {
            return new TassisConnection(link, domain, settings.clientId(), settings.maxListenerDeliveries());
        } catch (JMSException | RuntimeException e) {
            // The client identifier of the factory was refused: nothing must stay open.
            link.close();
            throw e;
        }
    }

    @Override
    public Connection createConnection() throws JMSException {
        return connect(Domain.UNIFIED, null, null);
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return connect(Domain.UNIFIED, userName, password);
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return connect(Domain.QUEUE, null, null);
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return connect(Domain.QUEUE, userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return connect(Domain.TOPIC, null, null);
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return connect(Domain.TOPIC, userName, password);
    }

    // ---- simplified API ---------------------------------------------------

    private JMSContext context(String userName, String password, int sessionMode) {
        TassisContext.checkSessionMode(sessionMode);
        try {
            return new TassisContext(connect(Domain.UNIFIED, userName, password), sessionMode);
        } catch (JMSException e) {
            String reason = e.getMessage();
            String code = e.getErrorCode();
            throw switch (e) {
                case InvalidClientIDException x -> new InvalidClientIDRuntimeException(reason, code, e);
                case JMSSecurityException x -> new JMSSecurityRuntimeException(reason, code, e);
                default -> new JMSRuntimeException(reason, code, e);
            };
        }
    }

    @Override
    public JMSContext createContext() {
        return context(null, null, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        return context(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        return context(userName, password, sessionMode);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return context(null, null, sessionMode);
    }
}
