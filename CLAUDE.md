# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> The Tassis family (later Thurn und Taxis) ran the relay postal service that carried
> messages across Europe from the end of the 15th century: a sender hands a message to the
> network, and the network takes charge of delivering it to its destination. The Tassis
> project implements **Jakarta Messaging 3.1**: it is that network for Java applications.

## Prerequisites

- **Java 25** + **Maven 3.9.16** (`.sdkmanrc` provided — use `sdk env`)
- `jakarta.jms:jakarta.jms-api:3.1.0` is already modular upstream (module
  `jakarta.messaging`, package `jakarta.jms`) — no Vidocq fork needed.
- The build runs `git-commit-id`, so the repository needs at least one commit.

## Essential Commands

```bash
# SDK environment
sdk env

# Full build
./mvnw -ntp clean install

# Unit tests
./mvnw test

# Official TCK: the tests Tassis is expected to pass today (see TCK.md)
./run-official-tck-messaging-3.1.sh

# The signature test of the API (downloads the TCK's JavaTest distribution, 14 MB)
./run-official-tck-messaging-3.1.sh signature

# Another TCK selection
./run-official-tck-messaging-3.1.sh -Dtck.test='**/core/selectorQueue/*IT'

# The same TCK through the network: Tassis client, socket, TassisServer
./run-official-tck-messaging-3.1.sh network

# Interoperability tests (Proton-J, Qpid JMS against TassisServer, the Tassis client against Artemis)
./mvnw -ntp -Pinterop -pl tassis-interop -am verify

# Benchmarks (JMH): build and smoke-test them, then run them; every figure goes to BENCH.md
./mvnw -ntp -Pbench -pl tassis-bench -am verify
java -jar tassis-bench/target/benchmarks.jar -prof gc
```

> `tassis-tck` is **in-reactor behind the `tck` Maven profile**, `tassis-interop` behind
> `interop` and `tassis-bench` behind `bench`. A plain `./mvnw install` builds none of them and
> downloads nothing for them.

## Architecture

Tassis is a **Jakarta Messaging 3.1 provider** with **zero third-party dependency**. Being a
provider, it ships its own broker: in-VM and entirely in memory, opened to the network over
AMQP 1.0 by `tassis-server` (ROADMAP M11.3).

```
tassis-api         ← Aggregator: re-exposes jakarta.jms-api (already modular, no fork)
tassis-core        ← The provider: administered objects (TassisConnectionFactory, TassisQueue,
                     TassisTopic), broker administration (TassisBroker),
                     internal.message, internal.broker, internal.client, internal.selector
tassis-amqp        ← AMQP 1.0 from scratch (M11.3): the codec, JMS messages as AMQP ones, and
                     the client transport (tassis:amqp://host)
tassis-server      ← TassisServer: the listener that opens a broker to AMQP 1.0 clients
tassis-pool        ← TassisConnectionPool: a connection factory that lends its connections (M11b)
tassis-cdi-vauban  ← JMSContext injection (§12.4) on Vauban: three beans and a facade
tassis-cdi-vauban-module-it ← Proof of the above on the module path; never published
tassis-bench       ← JMH benchmarks, beside Artemis and Qpid JMS, behind `bench`; never published
tassis-examples    ← The examples of the documentation, compiled and run by the build
tassis-tck         ← Official TCK runner, in-reactor behind `tck`, class path, test scope only
tassis-interop     ← Tests against third-party AMQP peers, behind `interop`; never published
```

`ROADMAP.md` records the design decisions, the spike findings (API module descriptor, TCK
shape, mandatory vs optional parts of the spec) and the milestones. Read it before starting
a milestone.

## Architecture Constraints Not to Violate

1. **`tassis-core` depends on nothing beyond `jakarta.jms-api`** — no CDI, no JNDI, no
   third-party library. `tassis-amqp`, `tassis-server` and `tassis-pool` depend on Tassis modules only. Third-party code is allowed
   in two places: `tassis-interop`, in test scope behind `interop`, and `tassis-bench` behind
   `bench` (JMH, and the brokers and the client Tassis is measured beside). Neither is ever
   published nor part of the default build, and `tassis-bench` has no module descriptor: its
   benchmarks run from one shaded jar, on the class path.
2. **`io.vidocq.tassis.core` is the only package exported to applications**, and holds only
   what an application must name (the administered objects, and `TassisBroker`). Everything
   else lives under `io.vidocq.tassis.core.internal.*`; some of those packages are exported
   to `tassis-amqp`, `tassis-server` and `tassis-pool` by name, and to nobody else. Likewise
   `tassis-server` exports one package with one class, `TassisServer`, and `tassis-pool` one
   with `TassisConnectionPool`.
3. **Unimplemented API methods throw `UnsupportedOperationException` with a ROADMAP
   pointer** (`"...: see ROADMAP M2"`), never a stub that silently returns wrong data.
   Replace the exception only when implementing the milestone.
4. **No resource file under a package path that another required module exports** (here
   `jakarta/jms/...`): it is a split package and fails module resolution at JVM boot, not at
   compile time.
5. **No `synchronized` around blocking calls, no `ThreadLocal`** — virtual threads first.
6. **Out of scope by decision** (see ROADMAP): a `vidocq` runtime extension, CDI message
   listeners, and **any persistence on the file system**: the broker is entirely in memory
   (M10 is abandoned). Do not start on them without a roadmap change. XA and JTA are ROADMAP
   M13, and stay out of `tassis-core`'s dependencies.
   **Authorization** is out of scope too: a broker authenticates (M11.1) and gives every user
   the same rights.
7. **JUnit 6** (`org.junit:junit-bom`, managed by `vidocq-parent`) for all tests — no Mockito;
   hand-written fixtures only.
8. **Language** — commit messages, Javadoc, comments and all `.md` content in **English**.

## Message model (`internal.message`)

`TassisMessage` (headers, properties, body template) and one subclass per body type.
`ValueConversions` is the single conversion table for properties, `MapMessage` and
`StreamMessage`: change a conversion there, never in a message class. `markReceived()` puts a
message in the state a consumer sees it in (properties and body read-only, body rewound).

## Broker and client (`internal.broker`, `internal.client`)

`Broker` is the in-VM broker, one per name per JVM (`Broker.named`). Messages wait in a
`Storage`: one per queue, one per topic subscription. Publishing to a topic adds the message to
each subscription's storage, after which a subscription behaves exactly like a queue. A send
hands the broker a snapshot (`Messages.snapshot`), never the client's own message.
`Destinations.resolve(...)` is the one place that decides which `Destination` objects are ours.
The subscription rules of §8.3 (reuse, replacement, conflicts) are all in the private
`Broker.named(LocalConnection, Subscribing)`, not to be confused with the registry lookup
`Broker.named(String)`.

**The seam.** `internal.client` never names `Broker`: it knows the broker through the interface
`BrokerConnection`, obtained from `Transports.connect(settings, user, password)`: implemented
for the broker of the JVM by `LocalConnection`, and for a broker of the network by
`AmqpBrokerConnection` in `tassis-amqp`, which `tassis-core` finds as a service (`Transport`)
and never names. The contract is written for a broker that is far away, and **a change to it
is a change to both implementations and to the server**:

- **The broker reads nothing from the client**, and no client code runs under the broker lock.
  What delivery depends on is broker state, set by a request: `start()` / `stop()`, and
  `Consumer.listen(boolean)` for a consumer's listener. It returns a listening period, which
  a dispatcher passes to `take(period)`: a `take` that returns null means the period is over or
  the consumer closed, and ends the dispatcher. Do not reintroduce a callback or a supplier
  from the broker into the client.
- **The broker keeps every unsettled delivery** (`LocalConnection.unsettled`). The session that
  answers for it must `settle` it (alone or in a `commit`), `redeliver` it or
  `returnUndelivered` it; all three ignore a delivery that is no longer the connection's.
  `receive(timeout, settleOnDelivery = true)` is for the one caller that will do none of them:
  the synchronous receive of an automatically acknowledged session.
- **A commit is one request**, `commit(sends, consumed)`: all of it or none of it.
- **`close()` takes back what is left**: consumers, unsettled deliveries (redelivered),
  temporary destinations, client identifier. That is the whole treatment of a lost connection,
  so keep whatever a connection owns reachable from `LocalConnection`.

`Users` is the user registry of a broker (§6.1.1), administered through the public
`TassisBroker`. It guards itself: never read or write it under the broker lock. The check is
made by `Broker.connect`, before anything is kept for the client, so that a rejected client
leaves no trace. A broker is open until a first user is declared and secured for good
afterwards: do not make removing the last user reopen it.

`TassisSession.work` guards the session's unsettled deliveries and uncommitted sends; the only
lock order is `work`, then the broker lock.

## Threads and callbacks

Tassis creates two kinds of thread, both virtual: one **dispatcher per consumer that has a
listener** (`TassisMessageConsumer.dispatch`), and one **completion thread per session** that has
made an asynchronous send. Application callbacks run on them and nowhere else.

- All `onMessage` calls of a session go through `TassisSession.dispatch`, under the session's
  `listeners` lock: that is what makes them serial. Never call a listener from elsewhere.
- Lock order: `listeners`, then `work`, then the broker lock. Nothing running under the broker
  lock may take a session lock or call application code.
- `close()` is the public method, with the restrictions on callbacks where the spec has some
  (session, connection, producer; a consumer or a browser may be closed from anywhere, so their
  `close()` is their `shut()`). `shut()` is what a parent uses to close a child, with no
  restriction. A `shut()` never throws and returns once the child is quiet. A session's
  `shut()` also waits for the listener in progress, which may no longer belong to any
  consumer (BUG.md T-001).
- A dispatcher that cannot deliver the message it took gives it back with
  `returnUndelivered`, not `redeliver`: the application never saw it.

## Settings (`internal.settings`)

`Settings` is the one description of what configures a connection factory. A new setting is
added there once, as a key (`tassis.<name>`) **and** as a URL parameter, with its default and
its validation, and gets its line in the tables of `README.md`, of the `TassisConnectionFactory`
Javadoc and of the Antora `reference` page in the same change. `tassis-core` reads no file and
no system property by itself: the application hands over a `Map` or `Properties`.

## Simplified API

`TassisContext`, `TassisJMSProducer` and `TassisJMSConsumer` are a thin layer over the classic
objects: put behaviour in the classic classes and only delegate from these, through `Unchecked`,
which turns each checked `JMSException` into its unchecked counterpart. A context creates its
session on first need, not in its constructor, so that `setClientID` still works on a new one.
Contexts created from one another share a connection, closed with the last of them.

## CDI injection (`tassis-cdi-vauban`)

`io.vidocq.tassis.cdi` exports `TassisCdi` only; the rest is in `io.vidocq.tassis.cdi.internal`.
`@Inject JMSContext` gives an `InjectedJMSContext`: a facade that asks `RequestContexts`, a
request-scoped bean, for the real context at each call. A `ContextKey` (factory name,
credentials, session mode, read from the annotations of the injection point) says which one;
equal keys share a context within a request. Rules:

- **Behaviour stays in `tassis-core`.** The facade only delegates, or refuses what §12.4.5
  forbids. A new `JMSContext` method is one delegation there.
- **CDI and Vauban are `requires static`**, and the beans are wired by the generated
  `_VaubanComponents`: no reflection, no `opens`. The tests' own beans live in the package
  `io.vidocq.tassis.cdi.app`, the only one surefire opens.
- **A request may run on several threads** (structured subtasks inherit its scope):
  `RequestContexts` guards its map with a lock, and its `@PreDestroy` logs instead of throwing.
- **Factory resolution is in `RequestContexts.factory`**: a `ConnectionFactory` bean of the
  application, else `SettingsFactories`, an application-scoped bean that builds each factory
  once from the system properties. This is the only place where Tassis reads system
  properties by itself.
- **A factory of the settings that reaches a broker of the network is a pool**
  (`TassisConnectionPool.suits`), closed with the application. Whether to pool is decided
  there and nowhere else; a bean the application supplies is used as it is.
- A change here is proven twice: `JmsContextInjectionTest`, and the module-path vehicle
  `tassis-cdi-vauban-module-it`, which must keep passing with nothing opened.

## AMQP 1.0 (`tassis-amqp`)

The codec of the network transport (ROADMAP M11.3): `internal.types` (values, `AmqpEncoder`,
`AmqpDecoder`), `internal.transport` (frames, performatives), `internal.messaging`,
`internal.transactions`, `internal.security`; and `internal.jms`, the one package that knows
JMS: `JmsMessageCodec`, a `TassisMessage` as an AMQP message and back, on the conventions of
Apache Qpid JMS, whose names are in `Conventions`. Exported to `tassis-server` only. Rules:

- **The specification is the source.** A format code, a descriptor, the order of the fields of
  a composite type are written from the OASIS text, and the test that covers them cites the
  section. A new described type is a record that implements `DescribedType`, is registered in
  `AmqpTypes`, and gets its line in `CompositeTypesTest` **and** in
  `tassis-interop`'s `DescribedTypesInteropTest`, whose accessors by name are what proves the
  order of the fields.
- **A field left out is `null`.** The defaults of the specification belong to whoever reads
  the field: the codec must give back what it was given.
- **The decoder trusts nothing.** Check a size against what is left before allocating for it,
  and report whatever is wrong as an `AmqpDecodeException`: no other exception may come out
  because of what a peer sent. Any change there adds its case to `HostileInputTest`.
- **No reflection**: a record lists its own fields.
- **An object message is never deserialized on the way**: it travels as its serialized form
  (`TassisObjectMessage.serialized()`). Deserializing what a client sent would hand the broker
  to whoever can connect.
- A change to the codec is proven twice: `./mvnw install`, and the `interop` profile.

## The AMQP client (`tassis-amqp`, `internal.client`)

`AmqpBrokerConnection` is the seam over a socket: one AMQP connection, one session, a reader
that never writes and a writer that alone writes. Rules:

- **One lock, one condition.** Every wait is on that condition, so it releases the lock; frames
  are queued under the lock. Never read or write the socket under it: `establish` talks to
  the socket itself, before anything else uses the connection, and takes the lock at the end.
- **One credit at a time**, granted when the application asks for a message: a message nobody
  asked for stays with the broker. Whoever stops waiting (`receive` out of time, a listener
  gone, `stop`, `close`) **drains**, then gives back with `released` what came and nobody saw.
  Nothing may stay in `arrived` behind a caller's back.
- **A drain ends** by the transfer that uses the credit or by a `flow` whose delivery count
  accounts for all of it; any other answer is that of an earlier drain.
- **A closed consumer whose session still answers for deliveries keeps its link**
  (`ReceiverLink.close`, `forget`): a broker gives back the unsettled deliveries of a link
  that goes. `detachDeferred` is for what needs the link gone now.
- **`sync()`** is how the client knows the broker has done what it was told: use it after what
  another connection must be able to see at once (`redeliver`, `returnUndelivered`). It only
  waits for a broker that offers `tassis:flow-echo`, so **nothing may depend on it for
  correctness**: with another broker it returns at once.
- **The client is also run against a broker that is not Tassis** (`TassisClientAgainstArtemisTest`
  in `tassis-interop`). What a link is named and which capabilities it carries are what
  Apache Qpid JMS sends (`subscriptionLinkName`): change them against that test, not against
  the Tassis server alone, which reads more forms than other brokers do.
- **An AMQP connection says who it is when it opens**: `claimClientId` reconnects, and a
  connection without a client identifier never asks for `sole-connection-for-container`.
- The names both ends agree on are in `internal.jms.Conventions`, and nowhere else.
- **TLS is the JDK's, and changes the socket only** (`amqps`, TLS from the first octet). The
  application hands over an `SSLContext`, to the server (`TassisServer.Builder.tls`) and, if
  the JVM's trust store is not enough, to the factory (`withSslContext`): Tassis reads no key
  store. The client always checks the host name against the certificate; do not add a way to
  turn that off. **To cut a connection, close the plain socket under the TLS one** (`cut`,
  `abort`): closing a TLS socket waits behind whoever is writing. Whatever a scenario of
  `NetworkClientTest` proves, `TlsNetworkClientTest` proves again over TLS by inheriting it:
  add network scenarios to the first.
- A change here is proven by `NetworkClientTest` (in `tassis-server`) **and by the TCK through
  the network**: `./run-official-tck-messaging-3.1.sh network`.

## The connection pool (`tassis-pool`)

`TassisConnectionPool` is a `ConnectionFactory` over a `TassisConnectionFactory` (ROADMAP M11b).
Rules:

- **What is pooled is the `BrokerConnection`, not the `Connection`.** Each borrower gets a new
  `TassisConnection` over a kept link, so whatever state lives in the client object is new by
  construction. Do not pool or reset a `TassisConnection`.
- **The logical close is `TassisConnection.recycle()`**, and it is the one place that says what
  a link must be rid of before it serves again: sessions shut, temporary destinations deleted,
  delivery stopped. Anything a connection comes to own on its link later (ROADMAP M13) is
  undone there, with a scenario in `TassisConnectionPoolTest` **and** in `NetworkPoolTest`.
- **A link that cannot be made new is closed, not lent**: a client identifier was set or
  refused, it is lost (`BrokerConnection.isLost`, asked at the moment, since listeners are
  told later), or the cleaning failed. `Recycler.done(false)` means closed already, and
  `done` is called once whatever fails: a place of the pool must never be lost.
- **A recycled connection never touches its link again**: a second `close()` returns under
  the `recycling` lock without doing anything. Keep every new path of `close()` behind it.
- **The pool reaches `tassis-core` through `internal.pool` only** (`ConnectionSource`,
  `PhysicalConnection`, `Recycler`), whose signatures name Jakarta Messaging and JDK types
  alone. The pool reads a factory by its URL; the one thing a URL does not say, the TLS
  context, comes through the reader `TassisConnectionFactory` registers when it loads
  (`ConnectionSource.register`): no public method for it, no reflection. **A setting that is
  not in the URL of a factory is lost to its pool**: put new settings in `Settings`.
- **One lock** in the pool, with a condition for the borrowers that wait (`changed`) and one
  for the housekeeper (`tick`). A link is opened and closed outside the lock, its
  place counted first. The pool asks to be told of a loss (`onLost`) under the lock, when a
  link becomes idle: a borrower asks after it, and whoever asks last is told.
- **One housekeeper thread per pool**, virtual, for everything that depends on time: idle
  and lifetime eviction, the report of a connection not given back. It only ever removes
  links that are idle, under the lock, and closes or logs outside it. Whatever changes a
  deadline signals `tick`. The pool takes nothing back from a borrower.
- **The settings of a pool are in `Settings.Pool`**, keys and URL parameters like the others;
  the builder starts from them. No probe before lending, by decision: AMQP has no exchange
  every broker answers.
- The tests of `tassis-pool` run on the class path (they need the server, which the module
  does not require); the pool on the module path is proven by `tassis-examples`.

## The AMQP server (`tassis-server`)

`TassisServer` listens; each client is an `AmqpConnection`, which is a client of
`BrokerConnection` like `internal.client` is. Rules:

- **Three kinds of virtual thread per connection.** The reader handles frames and **never
  writes to the socket**: it queues. The writer alone writes, in queue order. One pump per link
  the client receives by (`OutgoingLink`).
- **One lock per connection** (`AmqpConnection.lock`) guards sessions, links, credit and the
  unsettled deliveries. Order: that lock, then the broker lock. Never hold it while waiting for
  the broker (`take`, `receive`) or for the socket.
- **Credit is the reader's to change, the pump's to use.** The reader changes the link under
  the lock, then calls `Consumer.listen(false)`; the pump calls `listen(true)`, then asks
  `mayDeliver` before `take(period)`. Keep both orders: they are what closes the race.
- **A delivery taken from the broker always ends somewhere**: sent and recorded in
  `session.unsettled`, or `returnUndelivered`. A link, a session or a connection that ends
  gives back what it had not settled.
- **Refuse, do not fail.** A link the server cannot attach is refused (`refuse`), which leaves
  the session usable; only what is out of place ends the connection (`AmqpFailure`). A client
  answers the detach of a refusal: `session.refused` is what makes that answer no error.
- **What a link is, is read from its attach**, on the conventions of Apache Qpid JMS: the name
  of a receiving link is the name of its subscription (up to a `|`), the source says durable,
  shared, `global`, selector and `no-local`; a dynamic target asks for a temporary destination;
  a null source looks a durable subscription up to delete it; `copy` is a browser. Check a new
  convention against the client, in `tassis-interop`, not against memory.
- **A name the broker gives to temporary destinations** (`temporary-queue:`, `temporary-topic:`)
  is, for a client of the network, a temporary destination or nothing: never let it create a
  queue (`temporaryName`).
- **Whatever sends transfers counts the session window frame by frame**: a pump waits
  (`deliver`), a browser keeps its frames for the next flow (`feed`, on the reader thread,
  which must not wait).
- **A transaction holds what it has not done yet** (`Transaction`): the sends, and the
  deliveries accepted in it, which stay unsettled in the broker. Discharge is one
  `commit(sends, consumed)` or one `redeliver(consumed)`. A coordinator link that goes rolls
  its transactions back.
- **`global`**: every AMQP connection has a container identifier, so the server claims it as
  client identifier for all of them; a client that set none says so with the capability
  `global`, and its subscription names are then the broker's, not that identifier's.
- What the server announces and accepts is in `Limits`, and nowhere else.
- A change here is proven three times: `TassisServerTest` (frame by frame), the `interop`
  profile (Apache Qpid JMS as the client), and the TCK, since the broker is shared.

## Selectors (`internal.selector`)

`Selector.parse` compiles a selector, or returns `null` for none. `SelectorParser` holds the
grammar (as a comment) and the checks made when a selector is given; `Expression` is the tree and
its evaluation, where `null` is SQL's NULL / unknown. A subscription filters at publication
(`Broker.commit`). A queue keeps one `View` per selector in use, updated by `Storage.add` and
`Storage.remove`: always go through those two to change what a queue holds.

## Conventions

- **Maven groupId**: `io.vidocq.tassis`. Version `0.4.0-SNAPSHOT`, parent
  `io.vidocq:vidocq-parent:0.4.0-SNAPSHOT`.
- **Java modules**: `io.vidocq.tassis.api`, `io.vidocq.tassis.core`, `io.vidocq.tassis.amqp`,
  `io.vidocq.tassis.server`, `io.vidocq.tassis.pool`, `io.vidocq.tassis.cdi.vauban`.
- **Licence header** on every Java file (`etc/license-header.txt`, checked by the build).

## TDD — Test-Driven Development (mandatory)

1. **Red** — write the test first; it cites the spec section it covers.
2. **Green** — the minimum code to make it pass.
3. **Refactor** — with tests green.

Proof is `./mvnw -ntp clean install` with its output read, never an isolated `test` on a
stale `target/`.

## TCK — Technology Compatibility Kit

Runner, wiring, current score and the list of tests waiting for a later milestone are in
`TCK.md`. The selection run by default is the `tck.test` property of `tassis-tck/pom.xml`:
packages expected to pass, then negated tests grouped by the milestone that will enable them.
Finishing a milestone means deleting its lines there and updating `TCK.md`. When every TCK test
fails at once, run `PortingSmokeTest`: TCK tests swallow the exception of a failed setup.

**Release discipline:** no structural merge on `tassis-core` without TCK
PASS at 100%. Any challenge goes in a `TCK.md` with spec citation and reactivation plan.

## AI Principles — Collaboration on This Repository

- **Plan mode by default** on any structural change (new module, new SPI).
- **Cite the spec, not just the test**, in commit messages and PR descriptions.
- Use agents **`java-modules-guardian`**, **`virtual-threads-reviewer`**,
  **`dependency-gatekeeper`** on any `module-info.java` modification, concurrent code, or
  `pom.xml` change.
- If the rules in this file need updating, keep `AGENTS.md` aligned.

## Documentation (Antora) conventions

The component is `docs/en`, on Vauban's layout and under the rules of `AGENTS.md`: `index`,
`getting-started`, `usage`, `concepts`, `internals`, `tck`, `performance`, `reference`, `migration`. The `performance`
page follows `BENCH.md` and quotes no figure that is not in it: change them together. The logo is `tassis-logo.svg`, at the
root for the README and under `docs/en/modules/ROOT/images` for the documentation; the `.png`
beside each is its rendering, to regenerate when the SVG changes.

- A page states what the code does today. What is planned is named as planned, with its
  milestone, and the `reference` page lists the limits a user must know.
- `tck.adoc` follows `TCK.md`, and the configuration table of `reference.adoc` follows
  `Settings`: change them together.
- The code of `getting-started` is the code of `tassis-examples`, which the build compiles and
  runs: change the example first, then the page.

## Terminology

Use **Java Modules** (or **Java module** for a single module) when referring to
the Java Platform Module System. Do **not** use the abbreviation **JPMS** — in
prose, identifiers, or documentation.
