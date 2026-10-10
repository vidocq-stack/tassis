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
| Nature | Home-grown broker, **in-VM** today: the client objects call the `Broker` object of their JVM directly. No transport SPI exists. The network transport (M11) speaks **AMQP 1.0**, decided on 2026-10-10, and introduces the seam it needs. |
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

There is no `store` and no `transport` package, and no SPI for either, although the plan of M0
had them. Messages live in the broker's memory, in `Broker.Storage`, and the client objects call
the `Broker` directly. M11 (network transport) introduces the seam it needs.

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

#### M11.2 — The seam between client and broker

Today the client objects call `Broker` directly, and three things in that contract do not
survive a network:

- **The broker forgets a message when it delivers it.** `Broker.receive` removes it and the
  session keeps it; acknowledging is only forgetting. A client that dies would take its
  unacknowledged messages with it. The broker must hold the unsettled deliveries of each
  connection and requeue them when the connection is lost. (The discarded journal prototype
  had reached the same conclusion: see M10.)
- **The broker reads client state under its lock**: whether the connection is started, whether
  the consumer is closed. Those become things the client tells the broker.
- **Identity is an object reference** (`Outgoing.publisher`, `Subscribing.subscriber`), for
  `noLocal`. It becomes a connection identifier.

To deliver: one interface per connection between `internal.client` and the broker, shaped by
AMQP's model (see below) rather than by today's calls, with the in-VM broker behind it and no
change of behaviour. Proof: the unit tests and the whole TCK, unchanged.

#### M11.3 — AMQP 1.0

To open with a study of its own, since several of the points below are recalled from the
specifications and not yet checked against them:

- the AMQP 1.0 core (types, framing, transport, messaging, transactions, security) and how much
  of it a broker must implement to be a conforming container;
- the **JMS mapping**. AMQP 1.0 defines no queue, topic, selector or durable subscription; the
  conventions that give JMS its semantics (destination and message type annotations, selector
  and no-local filters, shared subscriptions, temporary destinations as dynamic nodes, delivery
  delay) come from the OASIS AMQP JMS Mapping, which to our knowledge never left working-draft
  status, and from what existing brokers and Apache Qpid JMS agreed on in practice;
- **delivery by credit**: AMQP pushes messages against the credit a receiver grants. A
  synchronous `receive` on top of it means either a credit of one per call, or a prefetch whose
  messages another consumer cannot see meanwhile. To be settled with the TCK in hand, then
  measured in M12;
- local transactions through the transaction coordinator, and what M13 (XA) can expect, since
  AMQP 1.0 leaves distributed transactions largely unspecified.

To deliver:

- [ ] The codec, the server on virtual threads, and the client transport, selected by the
      connection URL beside `tassis:vm:`.
- [ ] SASL PLAIN and ANONYMOUS over the registry of M11.1; optional TLS.
- [ ] Where the server lives: a module of its own (`tassis-server`), `tassis-core` keeping the
      client and the in-VM broker. To be confirmed in plan mode, as any new module.
- [ ] The `ExceptionListener` (§6.1.7), stored and never called so far, gets its purpose: a
      lost connection. Heartbeats, to notice one.
- [ ] **Proof: the whole TCK replayed through the network transport**, besides the in-VM run.
- [ ] Interoperability with a client that is not Tassis, as a test outside the reactor: it is
      the reason AMQP was chosen, and it needs a third-party client that must not become a
      dependency.

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
  keep correct, and its JMS mapping rests on conventions rather than on a standard. The slice
  opens with a study, and interoperability is tested against a real third-party client.
- **No JNDI in Vidocq**: `@JMSConnectionFactoryDefinition` / `@JMSDestinationDefinition` are
  Jakarta EE resource definitions and stay unimplemented unless a milestone is opened for them.
- **`requires static transitive java.transaction.xa`** in the API module: the XA types are
  visible at compile time although Tassis does not implement them before M13.
