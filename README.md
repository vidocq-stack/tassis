# Tassis

**Jakarta Messaging 3.1** provider for the
[Vidocq](https://codefloe.com/Vidocq/vidocq) runtime.

> Named after the **Tassis** family (later Thurn und Taxis), who ran the relay postal
> service that carried messages across Europe from the end of the 15th century.

## Status

🚧 **M7** — the classic and simplified APIs on an in-VM broker: queues and topics, acknowledgement
modes, local transactions, durable and shared subscriptions, selectors, message listeners,
asynchronous send, `JMSContext`. Messages live in memory, authentication and the network transport
are not implemented yet; see [ROADMAP.md](ROADMAP.md) and [TCK.md](TCK.md).

## Ecosystem rules

- **Zero third-party dependency** — only the Jakarta Messaging 3.1 API.
- **Strict Java Modules** — proper `module-info.java`, minimal `exports`, no classpath.
- **TDD** — test (or TCK scenario) first, then code.
- **Virtual threads** for any I/O.
- Bugs tracked in [BUG.md](BUG.md), benchmarks in [BENCH.md](BENCH.md).

## Configuration

A connection factory is configured in code, from settings, or from a connection URL:

```java
// in code
ConnectionFactory inCode = new TassisConnectionFactory("orders").withClientID("reader");

// from settings: only the keys under "tassis." are read, so this works with -Dtassis.broker=orders
ConnectionFactory fromSettings = TassisConnectionFactory.from(System.getProperties());

// from a connection URL
ConnectionFactory fromUrl = TassisConnectionFactory.fromUrl("tassis:vm:orders?clientId=reader");
```

| Key | In a URL | Default | Meaning |
|---|---|---|---|
| `tassis.url` | | | A connection URL holding the settings below; a setting given by its own key overrides it |
| `tassis.broker` | `tassis:vm:<broker>` | `default` | The name of the in-VM broker |
| `tassis.clientId` | `clientId` | none | The client identifier of the connections |
| `tassis.listener.maxDeliveries` | `listener.maxDeliveries` | `10` | Deliveries of a message to a listener that keeps failing before the message is dropped |

Several factories can be defined by name, `tassis.factory.<name>.<key>`, and obtained with
`TassisConnectionFactory.named(name, settings)`; a named factory inherits the settings that carry
no name. An unknown key or an invalid value is an error that names the key.

## Build

```bash
sdk env          # Java 25 (Temurin) + Maven 3.9.16, pinned by .sdkmanrc
./mvnw -ntp clean install
```

## License

Triple-licensed, at your option: **EPL-2.0 OR EUPL-1.2 OR GPL-2.0-or-later**.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).
