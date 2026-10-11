# Tassis — Implementation plan

> Jakarta Messaging 3.1 provider in the Vidocq style: zero third-party dependency,
> JDK 25, virtual threads, strict Java Modules. Tassis is a *provider*, so it ships its
> own broker: in-VM today, a network transport later (M11).
>
> Spec reference: [Jakarta Messaging 3.1](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html).
> API artifact: `jakarta.jms:jakarta.jms-api:3.1.0`, module `jakarta.messaging`, package `jakarta.jms`.

## Design decisions

| Topic | Decision |
|---|---|
| Nature | Home-grown broker, **in-VM** today: the client objects reach the broker of their JVM through the internal interface `BrokerConnection` (M11.2). The network transport (M11.3) speaks **AMQP 1.0**, decided on 2026-10-10, and will be another implementation of it. |
| Authentication | A user registry on each broker, administered with `TassisBroker` (M11.1). A broker is open until a first user is declared, then secured for good. No authorization: every user has the same rights. |
| Persistence | **In-memory, by decision** (2026-10-10): messages live in the broker's memory, in `Broker.Storage`, and the broker writes nothing to the file system. No store SPI exists. The journal that M10 planned is abandoned. |
| Integrations | **CDI through Vauban** only (`JMSContext` injection, M9). |
| Transactions | **Local transactions** in `tassis-core` (M3). **XA and JTA** through `mansart-transactions` are a later milestone (M13), outside `tassis-core`'s dependencies. |
| Out of scope | A `vidocq` runtime extension, CDI message listeners (MDB substitute), persistence on the file system. Not on this roadmap. |
| Zero third-party dependency | `tassis-core` compiles against `jakarta.jms-api` only. |
| Virtual threads | No `synchronized` around blocking calls, no `ThreadLocal`. Session delivery runs on virtual threads. |
| Strict TDD | Red → green → refactor, each test citing the spec section it covers. |

## Spike findings (2026-10-09)

> **A record of 2026-10-09, kept as written.** It says what was known before any production
> code. For the current state of the TCK (runner, counts, deviations) see [TCK.md](TCK.md). Where
> a finding has been settled or corrected since, a note dated 2026-10-10 says so below.

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
  *Note of 2026-10-10: 905 is a count of annotations. The real total is 903 tests: in two
  classes of `core` the annotation also sits on a private helper that JUnit does not run (see
  `TCK.md`).*
- **No XA and no restart** in `core` / `core20`: no reference to `XAConnection`, `XASession`
  or `XAResource`, no broker-restart test. An in-memory broker is therefore enough for the TCK.
- **Constraints read from `JmsTool`**: `createConnection(user, password)` is used throughout
  (system properties `user`, `password`, `jms_timeout`, `platform.mode`), and a
  `DURABLE_SUB_CONNECTION_FACTORY` implies a connection factory with a preconfigured client ID.
- **Still unverified**: that all 905 tests run in `standalone` platform mode (test classes are
  named `*IT`; 127 tests sit in `appclient` packages), and the weight of the transitive
  dependencies of `jakarta.tck:common` (ant, derby, commons-httpclient), which must stay in the
  test scope of `tassis-tck`. First task of M2's TCK wiring.
  *Note of 2026-10-10: both points are settled. Every package of `core` and `core20`, the
  `appclient` ones included, runs in `standalone` mode (M6 for `core`, M7 for `core20`). The
  harness dependencies are excluded from `tassis-tck`, and the selection passes without them
  (see `TCK.md`).*

### 3. Mandatory vs optional for a Java SE provider

| Mandatory | Optional |
|---|---|
| Both messaging domains (the sentence allowing a single one was removed from the spec) | XA and the chapter 11 interfaces (`ConnectionConsumer`, `ServerSessionPool`) |
| `JMSXDeliveryCount` (§3.5.11) | `JMSXUserID`, `JMSXAppID`, `byte[]` values for `JMSCorrelationID` |
| Classic, simplified and legacy domain-specific APIs (§2.7 to §2.9) | Jakarta EE support (chapters 12 and 13), including `JMSContext` injection (§12.4) |

## Module layout

```
tassis-api         ← Aggregator re-exposing jakarta.jms-api: a module descriptor, no class
tassis-core        ← The provider: administered objects, client API, in-VM broker, messages, selectors
tassis-cdi-vauban  ← JMSContext injection (§12.4) for applications on Vauban
tassis-cdi-vauban-module-it ← Its proof on the module path; never published
tassis-bench       ← Placeholder, no sources: JMH (M12)
tassis-examples    ← The examples of the documentation, compiled and run by the build
tassis-tck         ← The official TCK runner: in-reactor behind the `tck` profile, class path,
                     test scope only (porting class `TassisJmsObjects`, `PortingSmokeTest`)
```

`tassis-core` exports one package, `io.vidocq.tassis.core`, which holds the administered objects
(`TassisConnectionFactory`, `TassisQueue`, `TassisTopic`) and the administration of a broker
(`TassisBroker`). Its internal packages
(`io.vidocq.tassis.core.internal.*`, not exported) are four:
`message` (the message types and their conversions), `client` (Connection / Session / Producer /
Consumer / QueueBrowser, temporary destinations, and the simplified API), `broker` (the `Broker`
class: queues, subscriptions, client identifiers, delivery), `selector` (lexer, parser, evaluator).

There is no `store` and no `transport` package, although the plan of M0 had them. Messages live
in the broker's memory, in `Broker.Storage`. The client objects reach the broker through one
internal interface, `BrokerConnection` (M11.2), where the network transport of M11.3 will put
its own implementation.

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
  call.
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
  Nothing survives the JVM, by decision (see M10).
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
  (the setting `tassis.listener.maxDeliveries`, since M8c) the message is dropped and an error is logged through
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

### M7 — Simplified API ✅

- [x] `JMSContext` (§2.8): a connection and a session in one object; contexts created from one
      another share the connection, closed with the last of them; autostart
- [x] `JMSProducer`: send options, headers and properties carried by the producer, a `send`
      method per kind of body, asynchronous send through `setAsync`
- [x] `JMSConsumer`, including `receiveBody` and its rules when the body cannot be converted
- [x] Unchecked exceptions (§10.2): each `JMSException` reported as its runtime counterpart
- [x] `ConnectionFactory.createContext` in its four forms

**Deliverable:** `SimplifiedApiTest` (19 tests), 255 unit tests in all, and **901 / 901** on the
TCK selection: the whole of `core` and of `core20`, 61 packages, two tests negated until M11
(see `TCK.md`).

How it works: the three classes are a thin layer over the classic objects. `TassisContext` holds
the connection, a count of the contexts sharing it, and a session it creates on first need, so
that `setClientID` still works on a new context. `Unchecked` maps the exceptions.

The `virtual-threads-reviewer` pass found a deadlock, fixed here with its regression test:
`JMSContext.close()` kept the context's lock while waiting for the listeners, and a listener
using that context needed the same lock. Fixing it exposed a defect dating from M6, also fixed:
a session being closed refused the calls of the very listener the close was waiting for. Three
smaller ones went with it: `createContext` racing with the close of the last context, a
delivery lost when `receiveBody` failed otherwise than on the body type, and a correlation ID
given as bytes sent as a string.

Choices and limits:

- **`receiveBody` on a message it cannot convert**, in an automatically acknowledged context,
  leaves the message exactly where it was: the next receive gets it, not marked as redelivered.
  In the other modes it counts as delivered, as the Javadoc prescribes.
- **A context being closed stays usable by its callbacks in progress**, but creates nothing new.
- **`createQueue`, `createTopic` and the message factory methods need no session**, so they do
  not count as a use of the connection for `setClientID`.
- **Any credentials are accepted** by the `createContext(user, password)` forms, until M11.1.
- **No `XAJMSContext`** yet: XA is M13.

### M8 — TCK complete ✅

- [x] **Signature test** against `jakarta.jms.sig`: `./run-official-tck-messaging-3.1.sh signature`,
      profile `signature` of `tassis-tck`. Passed, and shown to fail on the 3.0.0 API.
- [x] `TCK.md` opens with the result: 901 of 903 functional tests, the signature test, what is
      not passed and what does not apply.

**Deliverable:** the TCK selection grew with each milestone since M2, so nothing functional was
left for this one.

What stays open, and where it is due:

- the two authentication tests of `core20/connectionfactorytests`: passed since M11.1;
- `ee` and `ee20`, out of reach without EJB, MDB or a web container;
- compatibility is **not claimed**: every applicable test passes since M11.1, but a claim is a
  certification request to the Eclipse Foundation, which is the maintainer's to make.

The signature file is under the Eclipse Foundation TCK License and is downloaded by the script,
not stored here; the `sigtest-maven-plugin` is a build-time plugin of the `signature` profile
only.

### M8b — Documentation

Added on 2026-10-10 after an inventory at the end of M7. The milestone is numbered M8b so that
M9 to M12, which the code, `TCK.md` and `CLAUDE.md` refer to by number, keep theirs. It depends
on nothing later than M7 and can be done before or after M8.

What exists and is current: `README.md`, `CLAUDE.md`, `TCK.md`, the milestone sections of this
file, and the Javadoc of the three exported classes.

What is stale, to correct first:

- [x] This file's **Design decisions** table and **Module layout** section still describe the
      plan of M0: a `MessageStore` SPI and a transport SPI that were never written (messages
      live in `Broker.Storage`), internal packages `store` and `transport` that do not exist,
      and `tassis-tck` as a placeholder. Rewrite them from the code.
- [x] The **Spike findings** section lists as "still unverified" two points settled since M6
      (every `core` and `core20` package runs standalone; the harness dependencies are
      excluded), and counts 905 tests where there are 903. Mark the section as a record of
      2026-10-09 and point to `TCK.md` for the current state.
- [x] `AGENTS.md` and `CONTRIBUTING.md` were copied from erasmus at M0 and never reread for
      Tassis.

What is missing:

- [x] **Antora component** under `docs/en`, mirroring Vauban's layout as `AGENTS.md` requires:
      `antora.yml`, `nav.adoc`, and the pages `index` (with the origin of the name and an
      "at a glance" table), `getting-started`, `usage`, `concepts`, `internals`, `tck`,
      `reference`, `migration`. The `performance` page waits for M12 and its `BENCH.md` entries.
  - `usage`: queues, topics and the four kinds of subscription, acknowledgement modes and
    transactions, selectors, listeners, asynchronous send, the simplified API.
  - `concepts` and `internals`: the in-VM broker, storages and subscriptions, deliveries and
    who owns them, the selector index, threads and callbacks, the lock order.
  - `reference`: the administered objects (`TassisConnectionFactory`, `TassisQueue`,
    `TassisTopic`), broker names, `withClientID`, and **the limits a user must know**: messages
    and durable subscriptions do not survive the JVM, any credentials are accepted, a poison
    message is dropped after 10 failed deliveries to a listener, no XA before M13, no JNDI.
  - `tck`: the content of `TCK.md`, kept in step with it.
- [x] **Logo**: `tassis-logo.svg` and its rendering `tassis-logo.png`, at the root for the README
      and in `docs/en/modules/ROOT/images`: a post horn, the emblem of the Tassis post.
- [ ] **Registration in `vidocq-docs`**: the two playbooks, `components-quick-nav.js`, and the
      site's own pages that list the bricks. The published playbook clones from Codefloe, so
      this needs the `Vidocq/tassis` repository to exist there.
- [x] **`tassis-examples`**: `SimplifiedHello`, `ClassicHello` and `ConfiguredHello`, a Java
      module with a test that runs them. `getting-started` quotes them.
- [x] **A usage snippet in `README.md`**.
- [x] **Javadoc review** of the exported package, and a `package-info.java` for it.

Exit criterion: the Antora site builds locally with the Tassis component
(`vidocq-docs/antora-playbook-local.yml`), every page was checked against the code as it is, and
no statement of this file contradicts the code.

**State on 2026-10-10: done, except the two items that wait on something outside this
repository.**

- Every page was read against the code, then updated for M8c, which landed while they were
  being written. The code of `getting-started` is compiled and run by the build
  (`tassis-examples`, 3 tests).
- **The component builds**: Antora 3.1 on the eight pages, warnings treated as failures, no
  warning; every cross-reference and anchor resolves. That build used a playbook holding this
  component alone and Antora's default UI, **not** the `vidocq-docs` playbook: Tassis is not
  registered there yet, so the first half of the exit criterion is not met to the letter.
- Still open: the **registration in `vidocq-docs`**, which is a change in another repository and, for the published playbook,
  needs `Vidocq/tassis` to exist on Codefloe.

### M8c — Configuration ✅

- [x] **A settings model with documented keys** (`internal.settings.Settings`), with no new
      dependency: `TassisConnectionFactory.from(Map)` and `from(Properties)`. An unknown
      `tassis.` key or an invalid value is rejected by an error that names the key.
- [x] **A connection URL**, the one-string form of the same settings:
      `tassis:vm:<broker>[?clientId=...&listener.maxDeliveries=...]`, read by `fromUrl` and
      written by `toUrl`. `vm` is the transport; M11 adds its own after `tassis:`.
- [x] **Named connection factories**: `TassisConnectionFactory.named(name, settings)` reads
      `tassis.factory.<name>.<key>` and inherits the settings that carry no name. This is what
      M9 will resolve `@JMSConnectionFactory("name")` with.
- [x] **The setting that was a constant**: `tassis.listener.maxDeliveries`, also
      `withMaxListenerDeliveries` in code.
- [x] **Documentation**: the table of keys in `README.md` and in the Javadoc of
      `TassisConnectionFactory`. The Antora `reference` and `getting-started` pages take it over
      in M8b.

| Key | In a URL | Default | Meaning |
|---|---|---|---|
| `tassis.url` | | | A connection URL holding the settings below; a setting given by its own key overrides it |
| `tassis.broker` | `tassis:vm:<broker>` | `default` | The name of the in-VM broker |
| `tassis.clientId` | `clientId` | none | The client identifier of the connections (§6.1.2) |
| `tassis.listener.maxDeliveries` | `listener.maxDeliveries` | `10` | Deliveries of a message to a listener that keeps failing before the message is dropped (§8.7) |

**Deliverable:** `ConnectionFactorySettingsTest` (27 tests), 282 unit tests in all. The exit
criterion is met: an application changes its broker and its client identifier without
recompiling, for instance with `TassisConnectionFactory.from(System.getProperties())` and
`-Dtassis.broker=...`.

Choices and limits:

- **External sources are whatever gives a `Map` or `Properties`.** Only the keys under
  `tassis.` are read, so the system properties or a property file an application loads can be
  passed as they are. Tassis itself reads nothing on its own: no file is looked up, no system
  property consulted behind the application's back.
- **Not done, and left to a decision**: environment variables (their names cannot contain dots,
  so they need a naming rule), and MicroProfile Config through `ravel`, which would be a new
  integration outside `tassis-core`.
- **No named destinations.** A queue or a topic is created on first use and is its name, so
  there is nothing to declare; a logical-name indirection can be added if a use for it appears.
- **`fromUrl` rejects what it does not know**, including a transport other than `vm`.

A rule for the milestones that follow: each new setting comes with its key, its URL parameter
and its line in that table, in the same change. Expected: host,
port and credentials (M11), pool size and timeouts (M11b).

### M9 — CDI through Vauban ✅

`tassis-cdi-vauban`: `@Inject JMSContext` (§12.4) for an application on Vauban. **Not covered
by the TCK** (its CDI tests need EJB or a web container): proven by our own tests.

- [x] **`@Inject JMSContext`**, alone or with `@JMSConnectionFactory`, `@JMSSessionMode` and
      `@JMSPasswordCredential` (§12.4.1 to §12.4.3). What is injected is a facade
      (`InjectedJMSContext`) that finds the real context at each call.
- [x] **Request scope** (§12.4.4): one context per request for each distinct combination of
      factory, credentials and session mode, shared by the injection points that ask for it,
      created on first use and closed when the request ends.
- [x] **The restrictions of §12.4.5**: `setClientID`, `setExceptionListener`, `start`, `stop`,
      `setAutoStart`, `close`, `commit`, `rollback`, `recover` and `acknowledge` throw
      `IllegalStateRuntimeException` on an injected context.
- [x] **Connection factories**: a `ConnectionFactory` bean of the application when there is one
      (the bean with `@Named("name")` for `@JMSConnectionFactory("name")`, the bean without
      `@Named` for the default), and otherwise the factory the settings of M8c describe, read
      from the system properties (`tassis.broker`, `tassis.factory.<name>.<key>`).
- [x] **No reflection, nothing opened**: the two beans are wired by the component provider
      Vauban's annotation processor generates at build time, declared by the module descriptor.
      They live in a package that is not exported; `TassisCdi` is the only public class.
      CDI and Vauban are `requires static`: the module adds nothing to an application that does
      not inject.

**Deliverable:** `JmsContextInjectionTest` (9 tests), `RequestContextsTest` (2 tests: a request
on several threads, a context that fails to close) and the module-path proof
`tassis-cdi-vauban-module-it` (`JmsContextInjectionModulePathTest`): an application as a Java
module, its beans processed at build time, injects two contexts while no package is opened to
the container, and the annotations of its injection points reach the producer.

Choices and limits:

- **No `BuildCompatibleExtension`**, which the plan announced: two plain beans are enough, and
  Vauban wires them through the generated `VaubanComponentProvider`.
- **The application registers the two beans** (`TassisCdi.BEANS`, with `addBeanClass`). The
  artifact has no Vauban bean index (`META-INF/vauban-beans.list`, written by Vauban's Maven
  plugin), so discovery by `scanClasspath()` is neither provided nor tested. To do if a use
  for it appears.
- **Outside a request, an injected context fails** (`ContextNotActiveException` from the
  container), as §12.4.4 leaves no other scope without a transaction. The transaction scope
  comes with JTA in M13.
- **The credentials of `@JMSPasswordCredential` are passed to the factory**, which accepts any
  until M11. They are part of what distinguishes two contexts.
- **`SESSION_TRANSACTED` is of no use on an injected context before M13**: §12.4.5 forbids
  `commit` and `rollback`, so nothing can commit what it sends.
- **This is the one place where Tassis reads the system properties by itself**: with injection
  the application does not create the factory, so it cannot hand over its settings. An
  application that wants another source supplies a `ConnectionFactory` bean.
- **Vauban only.** The beans are plain CDI and hold nothing specific to Vauban, but only the
  Vauban wiring is generated and tested.

### M10 — Journal: abandoned

Decided by the maintainer on 2026-10-10: **the broker stays entirely in memory** and writes
nothing to the file system. The append-only journal this milestone planned is not built. The
number is kept so that the other milestones keep theirs.

What follows from it, and is documented as such for users:

- `DeliveryMode.PERSISTENT` is accepted, since it is the default of the API, and changes
  nothing: no message survives the JVM, whatever its delivery mode.
- A durable subscription survives the connections that use it, not the JVM.
- The memory of the JVM bounds what a broker holds.
- Applications that need messages to survive a restart need another provider.

A journal was prototyped before the decision and discarded without being merged. If the
decision is ever revisited, what the prototype taught is worth knowing: the session must tell
the broker what it acknowledges (today acknowledging is only forgetting), a commit must be one
record, a compaction must describe the state after the change it follows, and the file must not
be written through an interruptible channel, since the threads of consumers are interrupted.

### M11 — Network transport, and authentication

Broker server on virtual threads, wire protocol, and authentication (§6.1.1), which only means
something once a client is no longer in the broker's JVM.

Decided with the maintainer on 2026-10-10, after the study that opened the milestone:

| Topic | Decision |
|---|---|
| Wire protocol | **AMQP 1.0** (OASIS, ISO/IEC 19464), preferred to a home-grown binary protocol: it opens a Tassis broker to clients that are not Tassis, and not Java. STOMP and MQTT were ruled out (no typed messages, transactions or queue browsing for one, no queues for the other), as was HTTP/2 through Chappe (a first dependency on another brick, for a protocol ill-suited to pushed delivery). |
| Access | **Open while empty**: a broker with no declared user accepts anyone, as before. Declaring a user secures it: credentials are checked, and a connection without any is refused unless anonymous connections are explicitly allowed. |
| Password on the wire | **Optional TLS**, with the JDK's own. The password travels inside the channel (SASL PLAIN); without TLS it travels in clear, and the documentation says so plainly. |

The cost of AMQP 1.0 is accepted knowingly: with no third-party dependency the type system, the
framing, sessions, links, credit and SASL are all written here, which makes this the largest
milestone of the roadmap. It is cut in three, each merged on its own with the TCK green.

#### M11.1 — Authentication on the in-VM broker ✅

- [x] `TassisBroker`, the first public API of broker administration: `addUser`, `removeUser`,
      `users`, `allowAnonymous`, `isSecured`. It lives in the exported package.
- [x] `internal.broker.Users`: the registry, which guards itself and is never touched under the
      broker lock. A password is kept as a salted SHA-256 digest and compared in constant time;
      an unknown user and a wrong password get the same answer.
- [x] `JMSSecurityException` from every `create...Connection`, `JMSSecurityRuntimeException`
      from every `createContext` (§10.3). The check comes before the client identifier is
      claimed, so a rejected client holds nothing.
- [x] A broker stays secured once it has been: removing its last user does not reopen it.
- [x] The two TCK tests negated since M7 are back in the selection: the porting class declares
      the user of the TCK's configuration and allows anonymous connections, which is what the
      suite expects of a provider.

Not in this slice, and why:

- **Authorization.** The spec asks for none, and nothing distinguishes two users today.
- **Default credentials of a factory** (a `user` and a `password` setting): they come with the
  connection URL of the network transport, in M11.3.
- **A stronger password hash.** Nothing is written out, so the digest only keeps the password
  out of a memory dump; a slow hash would be paid at each connection.

#### M11.2 — The seam between client and broker ✅

The client objects called `Broker` directly, and three things in that contract would not have
survived a network. Each is settled:

- [x] **The broker forgot a message when it delivered it**, and acknowledging was only
      forgetting, on the client's side. The broker now keeps the unsettled deliveries of each
      connection; the session settles them or gives them back with a request, and a commit is
      one request, atomic. (The discarded journal prototype had reached the same conclusion:
      see M10.)
- [x] **The broker read client state under its lock** (two `BooleanSupplier`, and
      `Broker.update(Runnable)`, which ran client code there). It is now told: `start()`,
      `stop()`, `Consumer.listen(boolean)`.
- [x] **Identity was an object reference** passed in each request, for `noLocal`. The broker
      now knows who a connection is, and closes what it owns: consumers, temporary
      destinations, client identifier.
- [x] `BrokerConnection`, the one interface between `internal.client` and a broker;
      `LocalConnection`, its implementation for the broker of the JVM; `Broker.connect`, which
      also checks the credentials.
- [x] **A connection that ends with work left undone** gets all of it taken back by
      `close()`: the one new behaviour of this slice, and the whole treatment a lost connection
      will need. `BrokerConnectionTest` exercises the contract directly.

No behaviour changed for applications: the existing tests pass untouched, and the TCK with them.
The concurrency review of the slice found one old defect, fixed here and recorded as `BUG.md`
T-001: closing a session did not wait for a listener that had removed itself from its consumer.

What it leaves for M11.3:

- **The seam is at the level of the connection**, with no session on the broker's side: the
  client buffers the sends of a transaction. It is the smallest contract both the in-VM broker
  and an AMQP peer can honour, not one modelled on AMQP's sessions and links. The study that
  opens M11.3 may reshape it.
- **A blocking `receive(timeout)`** is what the interface offers. Over AMQP it has to be built
  on credit; see the point on delivery by credit below.
- **`hasTemporaryQueue` / `hasTemporaryTopic` are asked at each use** of a temporary
  destination (`Destinations.resolve`). Free in the JVM, a round trip on a network.
- **One more lock acquisition per message for a listener** of an automatically acknowledged
  session, which now settles after `onMessage`. To be measured in M12.
- **Messages and selectors cross the seam as objects.** The codec is M11.3's.
- **A consumer the broker closes on its own** ends its dispatcher, which is new. But a
  dispatcher that holds a message at that moment may still hand it to its listener while the
  broker gives it back: harmless in the JVM, where a connection closes its sessions first, and
  to be settled for a connection that drops.

#### M11.3 — AMQP 1.0

Opened by a study (2026-10-11) that checked against the texts what this roadmap had so far
recalled from memory.

**What the standard says** (OASIS Standard, 29 October 2012; parts 1 to 5):

- A conformant implementation must implement the types (part 1) and the transport (part 2), and
  the messaging part (3) as soon as it processes messages. Transactions (4) and security (5) are
  recommended, not required.
- There is no client and no broker on the wire: two symmetric peers. One codec serves both.
- Transport: an 8-byte protocol header, frames with a channel, nine performatives (`open`,
  `begin`, `attach`, `flow`, `transfer`, `disposition`, `detach`, `end`, `close`), heartbeats as
  empty frames, a frame size of 512 bytes until negotiated. Link recovery after a failure is
  optional, and left out.
- Local transactions: a control link to a coordinator, `declare` then `discharge`. That covers
  the transacted sessions.
- **Distributed transactions are not specified**: part 4 defers them to a "part 6" that is not
  among the parts of the standard. XA (M13) therefore cannot go through AMQP in a standard way.
- Security: TLS and SASL strongly recommended for a server, with PLAIN and SCRAM-SHA1. PLAIN
  inside optional TLS, as decided, conforms; SCRAM can be added later.

**The JMS mapping is not a standard.** The OASIS "AMQP JMS Mapping" stayed a working draft (the
last one found is WD07, February 2017). The reference in practice is the code of Apache Qpid
JMS, where these conventions were read (2026-10-11, `AmqpSupport`, `AmqpMessageSupport`,
`AmqpDestinationHelper` on its main branch):

| JMS need | Convention |
|---|---|
| Message type | message annotation `x-opt-jms-msg-type` |
| Destination type | message annotations `x-opt-jms-dest`, `x-opt-jms-reply-to` (0 queue, 1 topic, 2 temporary queue, 3 temporary topic); terminus capabilities `queue`, `topic`, `temporary-queue`, `temporary-topic` |
| Selector | source filter `jms-selector`, descriptor `apache.org:selector-filter:string` (0x0000468C00000004) |
| `noLocal` | source filter `no-local`, descriptor `apache.org:no-local-filter:list` (0x0000468C00000003) |
| Shared subscriptions | source capabilities `shared`, `global`; connection capability `SHARED-SUBS` |
| Delivery delay | message annotation `x-opt-delivery-time`; connection capability `DELAYED_DELIVERY` |
| One connection per client identifier | `container-id`, connection capability `sole-connection-for-container` |
| Producer without a destination | connection capability `ANONYMOUS-RELAY` |
| Temporary destinations | dynamic nodes, with a `lifetime-policy` |
| Queue browser | distribution mode `copy` |

Still to check in that code: the numeric values of the message types, how the names of JMS
headers are written inside a selector, and how a durable subscription is named on the link.

**Delivery by credit.** AMQP pushes messages against the credit a receiver grants; Qpid JMS
prefetches 1000 messages per consumer by default. The Tassis client will do the opposite at
first: credit on demand, one message per `receive`, and `drain` for `receiveNoWait`, which keeps
today's semantics and the TCK. Prefetch becomes an option, to be measured in M12.

**Decided with the maintainer** (2026-10-11):

| Topic | Decision |
|---|---|
| Scope | **Both directions**: a Tassis broker accepts third-party AMQP clients, and the Tassis client works against a third-party AMQP broker. |
| Modules | **`tassis-amqp`** (the codec and the client transport) and **`tassis-server`** (the broker's listener). `tassis-core` stays without a socket; a remote client does not carry the server. |
| Interoperability tests | A **test module that is never published**, behind a Maven profile, may depend on third-party tools (Qpid JMS as a client; a third-party broker in Docker or locally). Nothing third-party enters the delivered artifacts. |

The seam of M11.2 stands: nothing in the study asks for it to be reshaped before starting.

Slices, each merged on its own with the TCK green, each opened in plan mode since they add
modules:

- [x] **M11.3a — The codec** (`tassis-amqp`): types, frames, performatives, message sections.
      Pure code, with no dependency, not even on `tassis-core`. See below.
- [x] **M11.3b — The server, queues only** (`tassis-server`): SASL PLAIN and ANONYMOUS over the
      registry of M11.1, connection, session, links, credit, settlement. Apache Qpid JMS as the
      client. See below.
- [x] **M11.3c — The rest of the mapping, server side**: topics, subscriptions, selectors,
      `noLocal`, temporary destinations, transactions, browsing, delivery delay. See below.
- [x] **M11.3d-1 — The Tassis client over AMQP**: the client transport behind
      `BrokerConnection`, selected by the connection URL beside `tassis:vm:`; heartbeats; the
      `ExceptionListener` (§6.1.7), stored and never called so far, for a lost connection.
      **Proof: the whole TCK replayed through the network**, besides the in-VM run. See below.
- [ ] **M11.3d-2 — TLS**: `amqps`, for the server and for the client. Until then nothing is
      encrypted, and the server listens on the loopback address unless told otherwise.
- [ ] **M11.3e — The Tassis client against a third-party broker**: what differs from one
      broker to another in the conventions above, found by running against a real one.

#### M11.3a — The codec ✅

- [x] `internal.types`: every primitive type and every encoding of part 1, described values,
      arrays. The encoder writes the shortest encoding; the decoder reads them all.
- [x] `internal.transport`: protocol headers, frames (AMQP, SASL, empty), the nine
      performatives, `error`. `internal.messaging`: the nine sections and `AmqpMessage`, source
      and target, delivery states, lifetime policies. `internal.transactions` and
      `internal.security`. 40 described types, known by numeric and by symbolic descriptor.
- [x] **A decoder that trusts nothing**: sizes and counts checked before allocating, nesting
      bounded, one exception type. `HostileInputTest`, with random and mutated inputs.
- [x] **`tassis-interop`**, behind the `interop` profile, never published: every type crossed
      with Apache Qpid Proton-J in both directions, the order of the fields of each composite
      type checked against Proton-J's accessors by name, and a real exchange, frame by frame,
      with an embedded Apache ActiveMQ Artemis (SASL, open, session, links, a message sent to
      a queue and received back).

What it taught, for the next slices:

- **Hand-computed octets are not a proof.** Four of the test vectors written by hand had a
  wrong size in them; the code was right each time, and it is Proton-J that settled it. The
  cross-check is the proof, the hand-written vectors are documentation.
- **Docker is not needed**, and was not available on the machine this was written on: Artemis
  runs embedded in the test JVM. That is what M11.3e will build on.
- **Artemis grants credit as soon as a sender attaches, and adds no header** to a message that
  has none. Small facts of that kind are what M11.3e is for.
- **The codec gives `null` for a field left out.** The connection, session and link logic of
  M11.3b will need the defaults of the specification in one place (`max-frame-size`,
  `channel-max`, the settle modes, `durable`, `priority`), rather than at each read.
- **Reading a frame from a blocking stream** is three lines over `FrameCodec.size`; it is left
  to M11.3b, with the virtual threads that will do it.
- Not done: `declare.global-id` is carried as any value and was not crossed with Proton-J,
  which types it for distributed transactions, a part AMQP 1.0 leaves unspecified.

#### M11.3b — The server, queues only ✅

- [x] `TassisServer`, the one public class of the new module `tassis-server`:
      `TassisServer.forBroker(name).port(n).start()`. It listens on the loopback address unless
      told otherwise, since nothing is encrypted yet.
- [x] The opening of a connection: protocol headers, SASL `PLAIN` and `ANONYMOUS` over the user
      registry, `open` and `close`, heartbeats, `sole-connection-for-container` for the client
      identifier (§6.1.2).
- [x] Sessions and links: a client sends to a queue (credit granted and renewed, messages in
      several frames, anonymous relay) and receives from one (delivery by credit, `drain`,
      every outcome of a delivery, messages split to the client's frame size).
- [x] **A client that goes away loses nothing**: detach, end, close or a lost socket give back
      what was not settled. This is what M11.2 had prepared, and it took no change to
      `BrokerConnection`.
- [x] `JmsMessageCodec` (`tassis-amqp`, `internal.jms`): a JMS message as an AMQP one and back,
      on the conventions of Apache Qpid JMS; a message of a client that is not a JMS one is
      classed by its body. An object message is never deserialized on the way.
- [x] What is not served yet is **refused per link** (`amqp:not-implemented`) and leaves the
      connection usable: topics, selectors, browsers, temporary destinations, transactions.
- [x] Proof: `TassisServerTest`, frame by frame; and in `tassis-interop`, **Apache Qpid JMS
      against `TassisServer`**: every kind of message, header fields and properties, priority,
      expiration, `receive` / `receiveNoWait` / listener, acknowledgement and `recover`, a
      large message, authentication, client identifiers, and an exchange with a Tassis client
      of the JVM.

What it changed outside the new module, and what it taught:

- **`tassis-core` gained two accessors**, not none as planned: `Broker.name()`, and
  `TassisObjectMessage.serialized()` / `setSerialized(byte[])`, without which an object message
  could only cross a broker by being deserialized there. Both are in internal packages.
- **`tassis-amqp` now depends on `tassis-core`**, for `internal.jms`: the mapping is shared by
  the server and, in M11.3d, by the client transport.
- **The reader never writes.** Were replies written from the reading thread, a client that
  stops reading would block it, and with it everything else of that connection. All writes go
  through one queue and one writer thread, which also gives the frames their order.
- **The last frames must outlive the reader**: closing the input stream closes the socket, and
  a `close` or a refused `sasl-outcome` was lost with it. The writer closes the socket.
- **A client answers the detach that refuses its link**, and may have sent credit for it
  meanwhile: neither is an error. Qpid JMS showed it; the frame-by-frame tests had not.
- **Prefetch and redelivery.** A client that prefetches, as Qpid JMS does by 1000, may be sent
  again a message it gives back while it closes: `JMSXDeliveryCount` can grow by more than one.
- **Message identifiers**: a string that starts with `ID:` travels as it is; the other AMQP
  types of identifier are kept in the text (`ID:AMQP_UUID:...`), which Qpid JMS reads the same
  way. Checked against the client, not against its source.
- **The concurrency review found two defects of delivery**, both reproduced by a test before
  being fixed: two links of one session that had waited for the client's window could be given
  the same delivery identifier, and a message longer than that window was never sent. A
  delivery now takes its identifier once it has room to start, and its frames go as the window
  opens.
- **A message rejected, or declared undeliverable, by a client is dropped**: there is no
  dead-letter queue. To be decided when it matters.
- Not done, and not planned in this slice: the limits of `Limits` are not configurable; a
  `Settings`-like description of a server belongs with the client transport of M11.3d, which
  needs the same URL.

#### M11.3c — The rest of the mapping, server side ✅

A client of the network now has what a client of the JVM has, short of XA.

- [x] **Topics and the four kinds of subscription** (§8.3), `noLocal`, and `unsubscribe` by a
      link that looks the subscription up and deletes it by closing.
- [x] **Message selectors** (§3.8), on queues and on subscriptions; an invalid one refuses the
      link.
- [x] **Temporary destinations** (§6.2.2): created by a sending link with a dynamic target,
      deleted with that link, and with the connection whatever happens.
- [x] **Local transactions** (§6.2.7): a coordinator link, `declare` and `discharge`; the sends
      and the acknowledgements of a transaction reach the broker in one `commit`, or not at
      all. A transaction whose coordinator link or connection goes is rolled back.
- [x] **Queue browsers** (§4.1.3): a snapshot, sent as credit comes, and the client told when
      it has seen it all. **Delivery delay** (§7.9): carried by an annotation, honoured by the
      broker as it already was.
- [x] Proof: Apache Qpid JMS on 21 more scenarios (`QpidJmsMessagingTest`), and the frames of
      transactions, browsers and refusals in `TassisServerTest`.

What it changed in the seam, and what it taught:

- **`BrokerConnection` gained one question and one flag.** `durableSubscriptionTopic(name)`: a
  remote client has no other way to know a durable subscription exists before it unsubscribes.
  And `global`, on `Subscribing`, `unsubscribe` and that lookup: whether a subscription name is
  the broker's rather than a client identifier's.
- **Why `global`.** Qpid JMS asks for `sole-connection-for-container` on every connection,
  whether the application set a client identifier or not, so the server claims the container
  identifier for all of them. A client that set none says so on each subscription, with the
  capability `global`. Without the flag, two such clients did not share a shared subscription:
  found by the test, not foreseen.
- **A rejected commit must not say `amqp:transaction:rollback`.** Qpid JMS turns any rejected
  commit into a `TransactionRolledBackException`, except one whose condition is already that
  one, which it takes for a transaction in doubt. The server answers with the cause
  (`amqp:not-found` for a destination that is gone).
- **A temporary destination that crossed a broker comes back as a plain one of its name.** A
  reply sent to it from the JVM goes to the queue of that name, which is the same storage; the
  kind is told again from the name when the message goes back out to AMQP. A reply sent from
  the JVM after the temporary queue was deleted creates a plain queue of that name: harmless,
  and untidy. Over the network it is refused: there, a name the broker gives to temporary
  destinations is one of them or nothing.
- **Only the creating connection may consume from a temporary destination** (§6.2.2): the
  client of the JVM checks it; over the network it is the client library that does, not the
  server.
- **The concurrency review found no race in the transactions**, which only the reader thread
  touches, and five defects elsewhere, each reproduced by a test before its fix: a browser
  sent a long message without regard for the session window, and forgot a drain it could not
  answer at once; an outcome given in a transaction that was over consumed the message; a
  deleted temporary destination named without its capability came back as a queue; the scope
  of a looked-up subscription was read from the link name by a looser rule than the one that
  created it. It also asked for bounds, now in `Limits`: the transactions a connection may
  hold undischarged, and the octets one may hold.
- Still not there: a `Settings`-like description of a server and configurable limits (with
  M11.3d), a dead-letter queue, and anything of XA.

#### M11.3d-1 — The Tassis client over AMQP ✅

`TassisConnectionFactory.fromUrl("tassis:amqp://host:5672")` reaches a broker in another
process. M11.3d was cut in two on 2026-10-11: this is the transport, TLS is M11.3d-2.

- [x] **A transport is a service.** `tassis-core` declares `uses Transport` and `tassis-amqp`
      `provides` it: the provider does not depend on its transport, and a URL that names one
      which is not there fails by saying which artifact is missing. An application on the
      module path that only connects must name the module (`requires io.vidocq.tassis.amqp`):
      a module that only provides a service is not resolved by itself.
- [x] **Settings**: `tassis.server`, and the URL `tassis:amqp://<host>[:<port>]`.
- [x] **`AmqpBrokerConnection`**: the seam of M11.2 over one AMQP connection and one session.
      Sends by an anonymous relay, one receiving link per consumer, transactions by the
      coordinator, temporary destinations, browsers, durable subscription lookup.
- [x] **Heartbeats**, and a broker silent for twice the time-out is given up.
- [x] **A lost connection**: `BrokerConnection.onLost`, the one addition to the seam; the
      `ExceptionListener` is called, the waits end, every later request fails. No reconnection.
- [x] **Proof**: `NetworkClientTest` (26 scenarios through a socket), an example run by the
      build (`NetworkHello`), and **the official TCK through the network: 903 passed out of
      903**, with nothing excluded (`./run-official-tck-messaging-3.1.sh network`).

What it decided, and what it taught:

- **The open is not put off.** The plan was to open the AMQP connection at the first use, so
  that `setClientID` could still name it. But a server gives a client ten seconds to open, and
  an application may create a connection long before it uses it. The connection is opened at
  once, and `setClientID` closes it and opens another: it must be the first thing done with a
  connection (§6.1.2), so nothing is lost. The identifier of a factory costs nothing.
- **One credit at a time.** A consumer asks the broker for a message when the application
  asks it for one, and takes its credit back (`drain`) when it stops waiting. A message nobody
  asked for stays where another consumer can take it, as with the broker of the JVM: that is
  what let the TCK pass unchanged. Fetching ahead is an optimisation for M12, with its price.
- **A closed consumer may have to stay.** A broker gives back the unsettled deliveries of a
  link that goes, and a session may still acknowledge or commit what a consumer it closed had
  delivered (§8.8). The link is kept, without credit, until then. Apache Qpid JMS does the same.
- **Two questions AMQP does not answer.** "Has the broker done what I told it?" is asked with
  a session flow that wants an echo, answered in the order of reading. "Is the drain over?" is
  answered by the transfer that uses the credit or by a flow that accounts for all of it.
- **No `global` for our own client.** A connection without a client identifier does not ask
  to be the only one with its container identifier, so the server claims nothing for it and
  its subscription names are the broker's. The flag of M11.3c is for clients that always ask.
- **`browse` may now fail**, like any request: its signature in the seam says so.
- **The concurrency review** found no fault in the credit and drain exchanges, and several
  around them, fixed here: two commits attaching the coordinator link at once, a
  `setClientID` refused late that left the connection marked lost, waits that a link ended by
  the broker would never end. The last cannot happen with a Tassis server: it is for M11.3e.
- Still not there: TLS (M11.3d-2), reconnection, fetching ahead, and any broker but ours
  (M11.3e). A message over the 16 MiB a server accepts ends the connection instead of failing
  the send.

### M11b — Connection pool

Added on 2026-10-10. Tassis has no connection pool: every `createConnection()` and
`createContext()` builds a new connection and nothing reuses one. With the in-VM broker that is
harmless, since a connection is a small object and opens no socket. It stops being so with the
network transport, where a connection is a socket, a handshake and an authentication: hence the
place of this milestone, right after M11, and its number, which leaves M12 its own.

The spec does not define pooling: in Jakarta EE it is the application server's job. Vidocq has
no such layer for messaging, so Tassis provides it.

To deliver:

- [ ] A pooling `ConnectionFactory` that wraps a `TassisConnectionFactory`, probably in a module
      of its own (`tassis-pool`) so that `tassis-core` keeps doing one thing. `close()` on what it
      hands out returns the physical connection to the pool instead of closing it.
- [ ] **What a logical close must undo**, so that the next borrower gets a clean connection: its
      sessions, producers, consumers and browsers closed, its transactions rolled back, its
      temporary destinations deleted, the connection stopped, its `ExceptionListener` removed.
- [ ] **Client identifiers.** A client identifier belongs to one open connection at a time
      (§6.1.2), and durable unshared subscriptions depend on it. Decide what a pooled connection
      does with `setClientID` (refuse it, or dedicate the connection) and how a factory made
      with `withClientID` pools, since it can only ever have one connection.
- [ ] **Sizing and lifetime**: maximum size, what a borrower does when the pool is exhausted
      (wait with a timeout, fail), idle eviction, and validation of a connection before it is
      handed out, with the `ExceptionListener` as the signal that one was lost.
- [ ] **The simplified API**: `createContext` served from the pool, with the connection shared
      by the contexts created from one another counted as one borrowed connection.
- [ ] **Virtual threads**: no `synchronized` around a wait, no `ThreadLocal`; a borrower blocked
      on an exhausted pool must not pin its carrier.
- [ ] Whether sessions are pooled too. To be decided by measurement (M12), not by default.

To look at before designing: `mansart-pool`, the workspace's JDBC connection pool, for its
sizing and eviction model. A dependency on it would have to be justified: the rule is that a
brick depends on specs only.

Relation to M9: the CDI injection of `JMSContext` works without a pool on the in-VM broker.
Once this milestone lands, injected contexts should be served from it.

### M12 — Benchmarks

JMH in `tassis-bench`, every figure recorded in `BENCH.md`, and the `performance` page of the
Antora component created in M8b. The benchmarks also settle the question left open since M2:
whether the single broker lock must be split.

### M13 — Transaction management (XA and JTA)

Local transactions are M3. This milestone lets a Tassis session take part in a transaction it
does not coordinate itself, typically a message and a database row committed together.

- [ ] The XA interfaces of chapter 11: `XAConnectionFactory`, `XAConnection`, `XASession`,
      `XAJMSContext`, and their legacy queue and topic variants
- [ ] An `XAResource` over the broker: one transaction branch per `Xid`, `start` / `end` with
      suspend and resume, `prepare`, one-phase and two-phase `commit`, `rollback`, `forget`,
      `isSameRM`, transaction timeout
- [ ] Prepared branches: the sends and the consumed messages of a prepared branch are held by the
      broker, neither visible nor redelivered, until the coordinator decides
- [ ] `XAResource.recover`: the prepared branches the broker holds. They are in memory like
      everything else (see M10), so a branch does not survive the JVM of the broker
- [ ] JTA enlistment with `mansart-transactions`: a session created inside a transaction enlists
      itself, so `@Transactional` and `UserTransaction` cover its sends and receives; §12.3 for the
      session parameters that are then ignored
- [ ] `JMSContext` injection (M9) inside a transaction: the `@TransactionScoped` context of §12.4.4

**Depends on:** M9 for the injected context. With the broker in memory by decision (M10), a
prepared branch is lost with the JVM: two-phase commit then protects against the failure of the
other resources and of the coordinator's decision, not against the loss of the broker's JVM. To
be stated plainly in the documentation of this milestone.

**Not covered by the TCK**: `core` and `core20` have no XA test (spike 2). Proven by our own
tests, including a coordinator that fails between `prepare` and `commit`.

To settle when the milestone opens:

- **Where the JTA part lives.** `tassis-core` only needs `java.transaction.xa`, which is in the
  JDK. Enlistment needs `jakarta.transaction` and `mansart-transactions`, so it goes in a module
  of its own (working name `tassis-transactions-mansart`), which `tassis-core` never requires.
- **What `mansart-transactions` offers by then.** Today it is local-only: one-phase commit, with
  two-phase commit and recovery announced as later work. Until it has them, a transaction that
  spans Tassis and a database is not atomic, and this milestone delivers the `XAResource` and
  one-phase enlistment only.

## Known risks

- **TCK standalone coverage** is proven: every package of `core` and `core20` runs in
  `standalone` mode, and their 903 tests pass (see `TCK.md`); `ee` and `ee20` stay out of
  reach.
- **AMQP 1.0 without a dependency** (M11.3) is a large body of protocol code to write and to
  keep correct, and its JMS mapping rests on conventions rather than on a standard: those of
  Apache Qpid JMS, which each broker follows more or less. Interoperability is tested against
  real third-party peers, in both directions.
- **XA over the network** (M13) has no standard to rest on: AMQP 1.0 leaves distributed
  transactions unspecified.
- **No JNDI in Vidocq**: `@JMSConnectionFactoryDefinition` / `@JMSDestinationDefinition` are
  Jakarta EE resource definitions and stay unimplemented unless a milestone is opened for them.
- **`requires static transitive java.transaction.xa`** in the API module: the XA types are
  visible at compile time although Tassis does not implement them before M13.
