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
package io.vidocq.tassis.core.internal.settings;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * What configures a connection factory, whatever it was read from: settings by
 * key, a connection URL, or code. The keys and the URL are two spellings of the
 * same thing, and every new setting is added here, to both.
 *
 * @param broker the name of the in-VM broker to connect to, when there is no {@code server}
 * @param server the broker of the network to connect to, as {@code <transport>://<host>:<port>},
 *        or {@code null} for a broker of this JVM
 * @param clientId the client identifier of the connections, or {@code null} for none
 * @param maxListenerDeliveries how many times a message is delivered to a listener that keeps
 *        failing before it is dropped
 */
public record Settings(String broker, String server, String clientId, int maxListenerDeliveries) {

    public static final String PREFIX = "tassis.";
    public static final String URL = PREFIX + "url";
    public static final String BROKER = PREFIX + "broker";
    public static final String SERVER = PREFIX + "server";
    public static final String CLIENT_ID = PREFIX + "clientId";
    public static final String MAX_LISTENER_DELIVERIES = PREFIX + "listener.maxDeliveries";

    public static final String DEFAULT_BROKER = "default";
    public static final int DEFAULT_MAX_LISTENER_DELIVERIES = 10;

    /** Where the settings of the factory called {@code name} are: {@code tassis.factory.<name>.}. */
    private static final String FACTORY_PREFIX = PREFIX + "factory.";

    /** The scheme of a connection URL, before the transport. */
    private static final String URL_SCHEME = "tassis:";
    /** The transport that needs no network: the broker of this JVM. */
    private static final String URL_PREFIX = URL_SCHEME + "vm:";
    /** The transports to a broker of the network, and the port each uses unless told otherwise. */
    private static final Map<String, Integer> SERVER_TRANSPORTS = Map.of("amqp", 5672);

    private static final Set<String> KEYS = Set.of(URL, BROKER, SERVER, CLIENT_ID, MAX_LISTENER_DELIVERIES);

    public static final Settings DEFAULTS = new Settings(DEFAULT_BROKER, null, null, DEFAULT_MAX_LISTENER_DELIVERIES);

    public Settings {
        if (broker == null || broker.isEmpty()) {
            throw new IllegalArgumentException(BROKER + " must not be empty");
        }
        if (server != null) {
            server = normalizedServer(server);
        }
        if (clientId != null && clientId.isEmpty()) {
            throw new IllegalArgumentException(CLIENT_ID + " must not be empty");
        }
        if (maxListenerDeliveries < 1) {
            throw new IllegalArgumentException(
                    MAX_LISTENER_DELIVERIES + " must be at least 1, not " + maxListenerDeliveries);
        }
    }

    public Settings withBroker(String newBroker) {
        return new Settings(newBroker, server, clientId, maxListenerDeliveries);
    }

    public Settings withServer(String newServer) {
        return new Settings(broker, newServer, clientId, maxListenerDeliveries);
    }

    public Settings withClientId(String newClientId) {
        return new Settings(broker, server, newClientId, maxListenerDeliveries);
    }

    public Settings withMaxListenerDeliveries(int newMaximum) {
        return new Settings(broker, server, clientId, newMaximum);
    }

    /** The transport of the server, {@code amqp} for one; {@code null} for a broker of this JVM. */
    public String serverTransport() {
        return server == null ? null : server.substring(0, server.indexOf("://"));
    }

    /** The host of the server, an IPv6 address without its brackets. */
    public String serverHost() {
        String host = server.substring(server.indexOf("://") + 3, server.lastIndexOf(':'));
        return host.startsWith("[") ? host.substring(1, host.length() - 1) : host;
    }

    public int serverPort() {
        return Integer.parseInt(server.substring(server.lastIndexOf(':') + 1));
    }

    /** Checks a server address and gives it its port if it has none: {@code <transport>://<host>:<port>}. */
    private static String normalizedServer(String server) {
        int separator = server.indexOf("://");
        String transport = separator < 0 ? "" : server.substring(0, separator);
        Integer defaultPort = SERVER_TRANSPORTS.get(transport);
        if (defaultPort == null) {
            throw new IllegalArgumentException(SERVER + " is <transport>://<host>[:<port>] with a transport among "
                    + SERVER_TRANSPORTS.keySet() + ", unlike '" + server + "'");
        }
        String address = server.substring(separator + 3);
        String host;
        String rest;
        boolean valid;
        if (address.startsWith("[")) {
            // An IPv6 address is written in brackets, as in a URL: its colons are not the port's.
            int end = address.indexOf(']');
            host = end < 0 ? "" : address.substring(0, end + 1);
            rest = end < 0 ? "" : address.substring(end + 1);
            valid = end > 1 && host.substring(1, end).chars()
                    .allMatch(c -> Character.digit(c, 16) >= 0 || c == ':' || c == '.');
        } else {
            int colon = address.indexOf(':');
            host = colon < 0 ? address : address.substring(0, colon);
            rest = colon < 0 ? "" : address.substring(colon);
            valid = !host.isEmpty()
                    && host.chars().allMatch(c -> Character.isLetterOrDigit(c) || c == '.' || c == '-');
        }
        if (!valid) {
            throw new IllegalArgumentException(SERVER + " names no host a connection can be made to: '" + server + "'");
        }
        int port = defaultPort;
        if (!rest.isEmpty()) {
            try {
                port = rest.startsWith(":") ? Integer.parseInt(rest.substring(1)) : -1;
            } catch (NumberFormatException e) {
                port = -1;
            }
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException(SERVER + " has no valid port in '" + server + "'");
            }
        }
        return transport + "://" + host + ":" + port;
    }

    /**
     * Reads the settings of a connection factory.
     *
     * <p>Only the keys under {@code tassis.} are looked at, so a set of properties that holds
     * other things, such as the system properties, can be passed as it is. Among them an
     * unknown key is an error rather than a setting silently ignored.
     *
     * @param factory the name of the factory, or {@code null} for the one that has none. A named
     *        factory takes its settings from {@code tassis.factory.<name>.<key>} and, for those
     *        it does not define, from the unnamed ones
     */
    public static Settings from(Map<String, String> settings, String factory) {
        if (factory != null && factory.isEmpty()) {
            throw new IllegalArgumentException("a connection factory name must not be empty");
        }
        Map<String, String> own = new LinkedHashMap<>();
        Map<String, String> named = new LinkedHashMap<>();
        String namedPrefix = factory == null ? null : FACTORY_PREFIX + factory + ".";
        for (Map.Entry<String, String> setting : settings.entrySet()) {
            String key = setting.getKey();
            if (namedPrefix != null && key.startsWith(namedPrefix)) {
                named.put(PREFIX + key.substring(namedPrefix.length()), setting.getValue());
            } else if (key.startsWith(PREFIX) && !key.startsWith(FACTORY_PREFIX)) {
                own.put(key, setting.getValue());
            }
        }
        return apply(apply(DEFAULTS, own, ""), named, factory == null ? "" : "factory." + factory + ".");
    }

    private static Settings apply(Settings base, Map<String, String> keys, String shownPrefix) {
        for (String key : keys.keySet()) {
            if (!KEYS.contains(key)) {
                throw new IllegalArgumentException("unknown setting " + shown(key, shownPrefix)
                        + "; the settings are " + String.join(", ", KEYS.stream().sorted().toList()));
            }
        }
        Settings result = base;
        // The URL first: a key of its own says more precisely what is meant, and wins.
        if (keys.containsKey(URL)) {
            result = fromUrl(keys.get(URL), result);
        }
        if (keys.containsKey(BROKER)) {
            result = result.withBroker(keys.get(BROKER));
        }
        if (keys.containsKey(SERVER)) {
            result = result.withServer(keys.get(SERVER));
        }
        if (keys.containsKey(CLIENT_ID)) {
            result = result.withClientId(keys.get(CLIENT_ID));
        }
        if (keys.containsKey(MAX_LISTENER_DELIVERIES)) {
            result = result.withMaxListenerDeliveries(
                    number(shown(MAX_LISTENER_DELIVERIES, shownPrefix), keys.get(MAX_LISTENER_DELIVERIES)));
        }
        return result;
    }

    private static String shown(String key, String shownPrefix) {
        return PREFIX + shownPrefix + key.substring(PREFIX.length());
    }

    private static int number(String key, String value) {
        try {
            int number = Integer.parseInt(value.trim());
            if (number < 1) {
                throw new IllegalArgumentException(key + " must be at least 1, not " + value);
            }
            return number;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a whole number, not '" + value + "'");
        }
    }

    /**
     * Reads a connection URL: {@code tassis:vm:<broker>} for a broker of this JVM, or
     * {@code tassis:amqp://<host>[:<port>]} for one of the network, each followed by
     * {@code [?clientId=...&listener.maxDeliveries=...]}.
     */
    public static Settings fromUrl(String url) {
        return fromUrl(url, DEFAULTS);
    }

    private static Settings fromUrl(String url, Settings base) {
        if (url == null || !url.startsWith(URL_SCHEME)) {
            throw new IllegalArgumentException("a connection URL starts with " + URL_SCHEME + ", unlike '" + url + "'");
        }
        boolean inVm = url.startsWith(URL_PREFIX);
        String rest = url.substring(inVm ? URL_PREFIX.length() : URL_SCHEME.length());
        int query = rest.indexOf('?');
        String where = query < 0 ? rest : rest.substring(0, query);
        Settings result = inVm ? base.withBroker(decode(url, where)).withServer(null) : base.withServer(where);
        if (query < 0) {
            return result;
        }
        Map<String, String> parameters = new LinkedHashMap<>();
        for (String parameter : rest.substring(query + 1).split("&", -1)) {
            int equals = parameter.indexOf('=');
            if (equals <= 0) {
                throw new IllegalArgumentException("'" + parameter + "' is not a name=value parameter in " + url);
            }
            String key = PREFIX + parameter.substring(0, equals);
            if (key.equals(URL) || key.equals(BROKER) || key.equals(SERVER) || !KEYS.contains(key)) {
                throw new IllegalArgumentException(
                        "unknown parameter '" + parameter.substring(0, equals) + "' in " + url);
            }
            if (parameters.put(key, decode(url, parameter.substring(equals + 1))) != null) {
                throw new IllegalArgumentException(
                        "parameter '" + parameter.substring(0, equals) + "' is given twice in " + url);
            }
        }
        return apply(result, parameters, "");
    }

    private static String decode(String url, String encoded) {
        try {
            return URLDecoder.decode(encoded.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("'" + encoded + "' is not correctly percent-encoded in " + url);
        }
    }

    /** The settings as a connection URL, which {@link #fromUrl(String)} reads back. Defaults are left out. */
    public String toUrl() {
        StringBuilder url = server != null ? new StringBuilder(URL_SCHEME).append(server)
                : new StringBuilder(URL_PREFIX).append(encode(broker));
        char separator = '?';
        if (clientId != null) {
            url.append(separator).append("clientId=").append(encode(clientId));
            separator = '&';
        }
        if (maxListenerDeliveries != DEFAULT_MAX_LISTENER_DELIVERIES) {
            url.append(separator).append("listener.maxDeliveries=").append(maxListenerDeliveries);
        }
        return url.toString();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
