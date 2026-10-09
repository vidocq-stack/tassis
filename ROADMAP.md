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

### M1 — Message model ✅

- [x] `Message` header fields (§3.4), including `JMSCorrelationID` as bytes
- [x] Properties and their conversion table (§3.5), read-only once received, `clearProperties`
- [x] The five body types: Text, Object, Map, Bytes, Stream (§3.11), with the Map / Stream
      conversion table (§3.11.3) and the write-only / read-only modes of Bytes and Stream
- [x] `getBody` and `isBodyAssignableTo`

**Deliverable:** `io.vidocq.tassis.core.internal.message`, 54 unit tests, one class per message type.
Messages are instantiated directly by the tests; `Session.createXxxMessage` arrives with M2.

Choices made where the spec leaves room, to be confirmed against the TCK from M2 on:

- A property name that is not a selector identifier, or is a selector reserved word, is
  rejected with `MessageFormatException` (§3.5.1 requires the rule, not an exception type).
- An empty `MapMessage` has a body (an empty `Map`); an empty `BytesMessage` has none.
- `JMSCorrelationID` set as bytes reads back as a UTF-8 string, and conversely.
- `Message.acknowledge()` throws `UnsupportedOperationException` until M3.
- `ObjectMessage` deserializes with plain `ObjectInputStream`: no deserialization filter yet.

### M2 — Point-to-point, and TCK wiring ✅

- [x] In-VM broker (`internal.broker.Broker`): named queues created on first use, delivery by
      priority then send order, expiration (§3.4.9), delivery delay (§7.9)
- [x] Classic API: `Connection` (start / stop / close, §6.1), `Session`, `MessageProducer`
      (identified and unidentified), `MessageConsumer` (`receive`, timeout, `receiveNoWait`)
- [x] A send stores a snapshot of the message (§3.9); a foreign message is rebuilt through the
      `jakarta.jms` interfaces (§3.12)
- [x] Temporary queues (§6.2.2), `QueueBrowser` (§4.1.3), `ConnectionMetaData` (§6.1.6)
- [x] Legacy queue API (§2.9): `QueueConnectionFactory`, `QueueConnection`, `QueueSession`,
      `QueueSender`, `QueueReceiver`
- [x] `createConnection(user, password)`: any credentials are accepted, the in-VM broker has no
      user registry
- [x] TCK wired: `tassis-tck`, `run-official-tck-messaging-3.1.sh`, `TCK.md`

**Deliverable:** 33 unit tests (`PointToPointTest`, `QueueFacilitiesTest`, `MessagesTest`,
`CloseRaceTest`) and **199 / 199** on the TCK selection of 16 queue packages (see `TCK.md`).

The `virtual-threads-reviewer` pass found no deadlock, lost wake-up or pinning, and one real
defect, fixed here with `CloseRaceTest` as its regression test: a close racing with a create
could leave a consumer attached for ever. Hence the add-then-check in `TassisSession.register`
and `TassisConnection`, and close paths that never throw and never return early.

What M2 settles and leaves open:

- **Spike 2's open points.** The `core` queue packages run as-is in `standalone` mode, no JNDI.
  Two things the spike had missed: the harness reads the porting class from a property *file*,
  not from the system properties, and `jakarta.tck:common` 11.0.0-RC4 has a defect that requires
  pinning 11.0.8 (both in `TCK.md`). `appclient` packages and `core20` are not tried yet.
- **M1's open choices** hold against the 199 tests: nothing in the selected packages contradicts them.
- **Sessions** are non-transacted with `AUTO_ACKNOWLEDGE` or `DUPS_OK_ACKNOWLEDGE` only; a
  message leaves its queue the moment a consumer takes it. `CLIENT_ACKNOWLEDGE` and transacted
  sessions throw `UnsupportedOperationException` until M3.
- **One broker lock** guards the broker and the delivery-related state of its clients. It is the
  simplest design that honours `stop()` and `close()`; M6 (listeners) and M12 (benchmarks) decide
  whether it must be split. Known costs: every state change wakes every waiting receive of the
  broker, a receive scans its queue from the head past delayed messages, and expired messages
  are only purged when a started consumer scans the queue.
- **Brokers live in a static registry** keyed by name and are never removed: a JVM that creates
  many differently named brokers keeps them all. To be revisited with M11.
- **Producer hints** `setDisableMessageID` / `setDisableMessageTimestamp` are recorded and
  ignored: every message gets an ID and a timestamp, which the spec allows.
- **`QueueSession` restrictions** of §9.2 (topic methods must throw `IllegalStateException`) come
  with M4, when those methods exist.

### M3 — Acknowledgement and local transactions ✅

- [x] The four session modes (§6.2): `AUTO_ACKNOWLEDGE`, `DUPS_OK_ACKNOWLEDGE` (same as auto),
      `CLIENT_ACKNOWLEDGE`, `SESSION_TRANSACTED`
- [x] `Message.acknowledge()` acknowledges everything the session has consumed (§6.2.10);
      ignored outside client-acknowledged sessions
- [x] Local transactions (§6.2.7): sends held until `commit`, which is all-or-nothing;
      `rollback` discards them and gives the consumed messages back
- [x] `recover` (§6.2.11); closing a session rolls its transaction back and acknowledges nothing
- [x] Redelivery: `JMSRedelivered` (§3.4.7) and `JMSXDeliveryCount` (§3.5.11) on every delivery;
      a message given back returns to its place in the queue
- [x] `ConnectionMetaData.getJMSXPropertyNames`

**Deliverable:** `AcknowledgementTest` (13 tests, 101 unit tests in all) and **219 / 219** on the
TCK selection, now 18 packages (see `TCK.md`).

How it works: `Broker.receive` hands out a `Delivery`, a per-delivery copy of the message plus
the stored original. The consumer's session owns the delivery until it settles it or gives it
back with `Broker.redeliver`. An automatically acknowledged session settles on delivery.

Choices and limits:

- **Acknowledging is forgetting.** With an in-memory store, a settled message needs no broker
  call. M10 (journal) will have to record settlement.
- **`DUPS_OK_ACKNOWLEDGE` behaves exactly like `AUTO_ACKNOWLEDGE`**: no lazy acknowledgement, so
  no duplicates.
- **`JMSXGroupID` and `JMSXGroupSeq`** are listed as supported and carried like any property.
  Tassis attaches no delivery guarantee to them; the spec defines none.
- **`createSession(false, SESSION_TRANSACTED)`** is rejected with a `JMSException`.
- **No redelivery limit and no dead-letter queue**: a message that is always rolled back is
  redelivered for ever. Neither is in the spec.
- **A redelivered message is immediately available**: there is no redelivery delay.

### M4 — Publish/subscribe ✅

- [x] Topics (`TassisTopic`); a queue and a topic of the same name are two destinations
- [x] The four kinds of subscription (§8.3): unshared non-durable, shared non-durable, unshared
      durable, shared durable, with their creation, reuse, replacement and conflict rules
- [x] `noLocal`, `unsubscribe`, temporary topics (§6.2.2)
- [x] Client identifier (§6.1.2): `setClientID` before any other use, unique among open
      connections, or assigned by `TassisConnectionFactory.withClientID`
- [x] Legacy topic API (§2.9) and the domain restrictions of §9.2 on `QueueSession`,
      `TopicSession` and `QueueConnection`

**Deliverable:** `PublishSubscribeTest` (23 tests, 124 unit tests in all) and **458 / 458** on the
TCK selection, now 35 packages (see `TCK.md`).

How it works: the broker keeps messages in a `Storage`, one per queue and one per subscription.
Publishing to a topic adds the message to the storage of each of its subscriptions; from there
a subscription behaves like a queue, so receive, acknowledgement, transactions and redelivery
are the code of M2 and M3 unchanged.

Choices and limits:

- **A topic with no subscription drops what is published to it**, which is the pub/sub model.
- **Durable subscriptions live in the broker's memory**: they survive connections, not the JVM.
  Durability across restarts is M10.
- **`noLocal` compares client identifiers** when the connection has one, the connection object
  otherwise, so that a durable `noLocal` subscription keeps ignoring its owner after a reconnect.
- **`setClientID` is too late** after `createSession`, `start`, `stop`, `getMetaData` or the
  exception listener accessors; `getClientID` does not count as a use.
- **Subscription sameness** is topic and `noLocal` for now; M5 adds the selector to it.
- **TCK run order.** `tassis-tck` runs test methods alphabetically because one TCK class is
  order-dependent (see `TCK.md`).

### M5 — Message selectors ✅

- [x] The selector syntax of §3.8.1.1: string, exact and approximate numeric and boolean
      literals, identifiers, comparison and arithmetic operators, `BETWEEN`, `IN`, `LIKE` with
      `ESCAPE`, `IS NULL`, parentheses, case-insensitive keywords
- [x] NULL semantics and three-valued logic (§3.8.1.2); `JMSDeliveryMode` as a string (§3.8.1.3)
- [x] Selectors on queue consumers and browsers (§4.1.2): a rejected message stays on the queue
- [x] Selectors on the four kinds of subscription (§4.2.2): a rejected message never enters the
      subscription, and the selector is part of what makes two subscriptions the same (§8.3)
- [x] `InvalidSelectorException` wherever a selector is given

**Deliverable:** `internal.selector` (hand-written lexer and recursive-descent parser, no parser
generator), `SelectorTest` (78 tests) and `SelectorDeliveryTest` (11), 213 unit tests in all, and
**598 / 598** on the TCK selection: 37 packages of `core`, no test negated (see `TCK.md`).

Choices and limits:

- **Semantic checks made when the selector is given**, which the spec leaves optional and the
  TCK requires: only numbers have an order (`myProp > 'foo'` is invalid), two operands of known
  and unlike types cannot be compared (`TRUE = 0`), a logical operand cannot be a number or a
  string, an arithmetic operand cannot be a string or a boolean. An identifier's type is only
  known per message: there a mismatch yields false, or unknown in arithmetic.
- **Header field references** are limited to the six of §3.8.1.1; another `JMS`-prefixed name
  that is not `JMSX…` or `JMS_…` is rejected.
- **Exact literals** accept the Java decimal, hexadecimal and octal forms with an optional `L`.
  Exact arithmetic wraps on overflow, as Java's does; an exact division by zero is unknown.
- **A property keeps its own type**: `Byte`, `Short`, `Integer` and `Long` are exact, `Float` and
  `Double` approximate (Java numeric promotion). No property conversion applies.
- **Selectors are evaluated under the broker lock**, once per message and per selector: when the
  message is sent (queue selectors in use, subscription selectors) or when a selector is first
  used on a queue. M6 replaced the per-receive scan with an index.
- **`JMSXDeliveryCount` in a selector** is the count of the delivery being considered, 1 for a
  first delivery. A queue consumer or browser checks it on each delivery. A subscription selects
  at publication, with a count of 1, and checks again on redelivery when its selector refers to
  the count: a message that no longer matches leaves the subscription.

### M6 — Asynchronous delivery ✅

- [x] `MessageListener` (§8.7): one dispatcher virtual thread per consumer that has a listener
- [x] Serial execution of a session's listeners (§6.2.13); no listener call while the connection
      is stopped, and `stop` returns only once the listener in progress has returned (§6.1.5)
- [x] Acknowledgement with listeners: an automatically acknowledged message is settled when
      `onMessage` returns, so `recover` inside the listener redelivers it; a `RuntimeException`
      redelivers it at once; client-acknowledged and transacted sessions move on to the next message
- [x] What a callback may not do: `Session.close`, `Connection.close` and `Connection.stop` from a
      listener on its own session or connection throw `IllegalStateException`; a listener may close
      its own consumer
- [x] Asynchronous send and `CompletionListener` (§7.3); `close`, `commit` and `rollback` wait for
      the callbacks and refuse to be called from one
- [x] `ExceptionListener` (§6.1.7) is stored and returned
- [x] Per-selector index on queues

**Deliverable:** `AsynchronousDeliveryTest` (20 tests), three index tests in
`SelectorDeliveryTest`, 236 unit tests in all, and **642 / 642** on the TCK: the whole of `core`,
46 packages (see `TCK.md`).

The `virtual-threads-reviewer` pass confirmed the lock order and found no deadlock on the paths
above. It found defects that are fixed here: a listener throwing an `Error` left its message in
limbo and its consumer deaf; a consumer could briefly have two dispatchers; a message taken while
the session was closing came back marked as redelivered though no listener had seen it; `start` on
a started connection could make listeners of two sessions wait for each other; and expired
messages were no longer removed from a queue whose consumers all read through a selector view.

How it works:

- **Listeners.** A consumer's dispatcher takes one message at a time with `Broker.receive`, then
  asks its session for the turn (`TassisSession.dispatch`), which holds the session's `listeners`
  lock around `onMessage`. While the connection is stopped the dispatcher keeps its message and
  waits; if the consumer is closed or loses its listener first, the message goes back to the
  broker as if it had never been taken (`Broker.returnUndelivered`).
- **Asynchronous send.** A send to the in-VM broker is complete when it returns, so it is done
  entirely in the calling thread, where a failure is thrown. Only the notification is
  asynchronous: `onCompletion` runs on a single virtual thread per session, in send order.
  `onException` is never called: nothing can fail after the send has returned.
- **Selector index.** Each selector in use on a queue has a view: the ordered messages it accepts,
  built by one scan when its first consumer attaches and kept up to date afterwards. A message is
  evaluated once per selector, and a receive takes the head of its view.

Choices and limits:

- **A listener that always fails does not loop for ever**: after 10 deliveries
  (`TassisSession.MAX_LISTENER_DELIVERIES`) the message is dropped and an error is logged through
  `System.Logger`. §8.7 leaves the number of redeliveries to the provider. There is no
  dead-letter queue.
- **`stop` and `close` from a callback fail** with `IllegalStateException`: of the two behaviours
  the spec permits, the one that cannot deadlock.
- **`Connection.stop` from a completion listener is not refused**: the spec only names message
  listeners. If a listener of the same connection is at that moment waiting for that completion
  callback (inside `commit`, say), the two wait for each other. It is a circular wait in the
  application's own callbacks, left as it is.
- **A consumer with a listener refuses `receive`** with `IllegalStateException`; the spec calls
  mixing the two undefined.
- **`ExceptionListener` is never called**: the in-VM broker has no connection to lose. It will
  matter with the network transport (M11).
- **Selectors on `JMSXDeliveryCount` are not indexed**: their answer changes between deliveries,
  so their consumers still scan the queue.
- **Still one broker lock.** Listeners and the index did not need it split, and no benchmark has
  asked for it yet: the decision stays with M12.

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
