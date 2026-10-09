# Tassis

**Jakarta Messaging 3.1** provider for the
[Vidocq](https://codefloe.com/Vidocq/vidocq) runtime.

> Named after the **Tassis** family (later Thurn und Taxis), who ran the relay postal
> service that carried messages across Europe from the end of the 15th century.

## Status

🚧 **M4** — queues and topics on an in-VM broker through the classic API: acknowledgement modes, local
transactions, durable and shared subscriptions. Selectors, listeners and the simplified API are not
implemented yet; see [ROADMAP.md](ROADMAP.md).

## Ecosystem rules

- **Zero third-party dependency** — only the Jakarta Messaging 3.1 API.
- **Strict Java Modules** — proper `module-info.java`, minimal `exports`, no classpath.
- **TDD** — test (or TCK scenario) first, then code.
- **Virtual threads** for any I/O.
- Bugs tracked in [BUG.md](BUG.md), benchmarks in [BENCH.md](BENCH.md).

## Build

```bash
sdk env          # Java 25 (Temurin) + Maven 3.9.16, pinned by .sdkmanrc
./mvnw -ntp clean install
```

## License

Triple-licensed, at your option: **EPL-2.0 OR EUPL-1.2 OR GPL-2.0-or-later**.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).
