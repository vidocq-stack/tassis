# Tassis — Implementation plan

> Jakarta Messaging 3.1 provider in the Vidocq style: zero third-party dependency,
> JDK 25, virtual threads, strict Java Modules. Tassis is a *provider*, so it ships its
> own broker: in-VM first, network transport later behind a transport SPI.
>
> Spec reference: [Jakarta Messaging 3.1](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html).
> API artifact: `jakarta.jms:jakarta.jms-api:3.1.0`, module `jakarta.messaging`, package `jakarta.jms`.

## Design decisions

| Topic | Decision |
|---|---|
| Nature | Home-grown broker, **in-VM** first. Network transport is a later milestone (M11) behind a transport SPI. |
| Persistence | **In-memory** behind a `MessageStore` SPI, then a home-grown **append-only journal** (M10). |
| Integrations | **CDI through Vauban** only (`JMSContext` injection, M9). |
| Out of scope | XA / `mansart-transactions`, a `vidocq` runtime extension, CDI message listeners (MDB substitute). Not on this roadmap. |
| Zero third-party dependency | `tassis-core` compiles against `jakarta.jms-api` only. |
| Virtual threads | No `synchronized` around blocking calls, no `ThreadLocal`. Session delivery runs on virtual threads. |
| Strict TDD | Red → green → refactor, each test citing the spec section it covers. |

## Spike findings (2026-10-09)

Three spikes ran before any production code. Their conclusions shape the milestones below.

### 1. API artifact

`jar --describe-module` on `jakarta.jms-api-3.1.0.jar`:

```
jakarta.messaging@3.1.0
exports jakarta.jms
requires jakarta.annotation static
requires java.base mandated
requires java.transaction.xa static transitive
```

The artifact is a real Java module, so `tassis-api` is a plain aggregator (`requires transitive
jakarta.messaging`), like `erasmus-api`. No fork. 3.1.0 is the only 3.1 release on Maven Central.

### 2. TCK

The TCK exists in two shapes:

| Shape | Harness | Verdict |
|---|---|---|
| `jakarta-messaging-tck-3.1.0.zip` (download.eclipse.org, 14 MB) | JavaTest + Ant, `ts.jte` | Not used, except for its signature file `jakarta.jms.sig` |
| `jakarta.tck:jms-tck:11.0.3` (Maven Central) | JUnit 5, "Jakarta Messaging Outside container", compiled against API 3.1.0 | **Retained** |

- **No JNDI needed.** Tests obtain administered objects through
  `com.sun.ts.lib.porting.TSJMSObjectsInterface` (`getQueue`, `getTopic`,
  `getQueueConnectionFactory`, `getTopicConnectionFactory`, `getConnectionFactory`). The
  implementation class is selected by the system property `porting.ts.jmsObjects.class.1`.
  The interface and `JmsTool` live in `jakarta.tck:common`.
- **Volume**: 1160 `@Test` methods in the sources jar. 905 are in `core` (644) and `core20`
  (261); 255 are in `ee` / `ee20` and need EJB, MDB or a web container, which Vidocq does not have.
- **No XA and no restart** in `core` / `core20`: no reference to `XAConnection`, `XASession`
  or `XAResource`, no broker-restart test. The journal (M10) does not gate the TCK.
- **Constraints read from `JmsTool`**: `createConnection(user, password)` is used throughout
  (system properties `user`, `password`, `jms_timeout`, `platform.mode`), and a
  `DURABLE_SUB_CONNECTION_FACTORY` implies a connection factory with a preconfigured client ID.
- **Still unverified**: that all 905 tests run in `standalone` platform mode (test classes are
  named `*IT`; 127 tests sit in `appclient` packages), and the weight of the transitive
  dependencies of `jakarta.tck:common` (ant, derby, commons-httpclient), which must stay in the
  test scope of `tassis-tck`. First task of M2's TCK wiring.

### 3. Mandatory vs optional for a Java SE provider

| Mandatory | Optional |
|---|---|
| Both messaging domains (the sentence allowing a single one was removed from the spec) | XA and the chapter 11 interfaces (`ConnectionConsumer`, `ServerSessionPool`) |
| `JMSXDeliveryCount` (§3.5.11) | `JMSXUserID`, `JMSXAppID`, `byte[]` values for `JMSCorrelationID` |
| Classic, simplified and legacy domain-specific APIs (§2.7 to §2.9) | Jakarta EE support (chapters 12 and 13), including `JMSContext` injection (§12.4) |

## Module layout

```
tassis-api         ← Aggregator re-exposing jakarta.jms-api
tassis-core        ← The provider: client API, in-VM broker, messages, selectors, store
tassis-cdi-vauban  ← Placeholder: JMSContext injection (M9)
tassis-bench       ← Placeholder: JMH (M12)
tassis-examples    ← Placeholder
tassis-tck         ← Placeholder: official TCK runner, in-reactor behind the `tck` profile (wired in M2)
```

Planned internal packages of `tassis-core` (`io.vidocq.tassis.core.internal.*`, not exported):
`message` (types and conversions), `client` (Connection/Session/Producer/Consumer/JMSContext),
`broker` (destinations, subscriptions, dispatch), `selector` (lexer/parser/evaluator),
`store` (`MessageStore` SPI + in-memory implementation), `transport` (SPI + in-VM implementation).

## Phases

Every milestone from M2 onward lists the TCK packages it must turn green as its exit
criterion; the exact package-to-milestone mapping is written when the harness is wired (M2).

### M0 — Bootstrap ✅

- [x] `.sdkmanrc`, `.gitignore`, `.mvn/`, Maven wrapper, Forgejo workflows, licence files
- [x] Parent `pom.xml` (Model 4.0.0, parent `io.vidocq:vidocq-parent:0.4.0-SNAPSHOT`)
- [x] Module skeletons with `module-info.java`: `tassis-api`, `tassis-core`, `tassis-cdi-vauban`;
      placeholders `tassis-bench`, `tassis-examples`; `tassis-tck` behind the `tck` profile
- [x] Smoke test: `new TassisConnectionFactory().createConnection()` returns a connection
- [x] `CLAUDE.md`, `AGENTS.md`, `README.md`, `ROADMAP.md`, `BUG.md`, `BENCH.md`

**Deliverable:** buildable reactor, `./mvnw -ntp clean install` and `./mvnw -ntp -Ptck clean install` green.

### M1 — Message model

`Message` and the five body types (Text, Bytes, Map, Stream, Object), header fields (§3.4),
properties and their conversion table (§3.5), read-only / write-only body modes, `getBody` and
`isBodyAssignableTo` (§3.11). Pure unit tests, one class per message type.

### M2 — Point-to-point, and TCK wiring

Classic API on an in-VM queue: `send`, `receive` (blocking, timeout, `receiveNoWait`),
connection start/stop/close (§6.1), ordering, priority, expiration (§7.8), delivery delay (§7.9),
temporary queues, `QueueBrowser`, `ConnectionMetaData`. Includes the **legacy queue API**
(`QueueConnectionFactory`, `QueueConnection`, `QueueSession`, `QueueSender`, `QueueReceiver`)
and **`createConnection(user, password)`**, both required by the TCK.

TCK wiring: `tassis-tck` gets `jakarta.tck:jms-tck` + `jakarta.tck:common` in test scope, a
`TSJMSObjectsInterface` implementation, and `run-official-tck-messaging-3.1.sh`. Settle the two
unverified points of spike 2 here.

### M3 — Acknowledgement and local transactions

`AUTO_ACKNOWLEDGE`, `CLIENT_ACKNOWLEDGE`, `DUPS_OK_ACKNOWLEDGE`, `SESSION_TRANSACTED` (§6.2),
commit, rollback, `recover`, `JMSRedelivered`, `JMSXDeliveryCount`.

### M4 — Publish/subscribe

Topics, non-durable, durable and shared subscriptions (§4.2), `noLocal`, client identifier
(§6.1.2) including a **client ID preconfigured on the connection factory**, `unsubscribe`,
temporary topics. Includes the **legacy topic API** (`TopicConnectionFactory`, `TopicSession`,
`TopicPublisher`, `TopicSubscriber`).

### M5 — Message selectors

The SQL-92 subset of §3.8: literals, identifiers, comparison and arithmetic operators,
`LIKE` / `ESCAPE`, `IN`, `BETWEEN`, `IS NULL`, three-valued logic. Hand-written lexer and
parser, no parser generator.

### M6 — Asynchronous delivery

`MessageListener` (§8.7), the session threading model (§2.14: serial delivery, on a virtual
thread), asynchronous send and `CompletionListener` (§7.3), `ExceptionListener` (§6.1.7), and
the calls forbidden from within callbacks.

### M7 — Simplified API

`JMSContext`, `JMSProducer`, `JMSConsumer` (§2.8), `JMSRuntimeException` mapping (§10),
autostart. Tests mirror M2 to M6.

### M8 — TCK complete

905 / 905 on `core` + `core20`, plus the signature test against `jakarta.jms.sig`. Any
challenge (disabled test, spec-interpretation divergence) goes in a `TCK.md` with the spec
citation and a reactivation plan.

### M9 — CDI through Vauban

`tassis-cdi-vauban`: `@Inject JMSContext` with `@JMSConnectionFactory`, `@JMSSessionMode`,
`@JMSPasswordCredential` and the scopes of §12.4, through a `BuildCompatibleExtension`.
**Not covered by the TCK** (its CDI tests need EJB or a web container): proven by our own
module-path integration tests.

### M10 — Journal

`MessageStore` backed by an append-only journal: `PERSISTENT` messages and durable
subscriptions survive a broker stop, with crash-injection tests. Until then `PERSISTENT` is
accepted but not durable across restarts; this is a documented limitation.

### M11 — Network transport

Broker server on virtual threads, wire protocol, client/broker module split. The protocol
(home-grown or a standard one) is chosen by a dedicated study when the milestone opens.

### M12 — Benchmarks and documentation

JMH in `tassis-bench`, every figure recorded in `BENCH.md`. Antora component under `docs/en`,
mirroring Vauban's layout, with `tck` and `performance` sections.

## Known risks

- **TCK standalone coverage** is assumed, not yet proven (spike 2, "still unverified").
- **No JNDI in Vidocq**: `@JMSConnectionFactoryDefinition` / `@JMSDestinationDefinition` are
  Jakarta EE resource definitions and stay unimplemented unless a milestone is opened for them.
- **`requires static transitive java.transaction.xa`** in the API module: the XA types are
  visible at compile time although Tassis does not implement them.
