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
```

> `tassis-tck` is **in-reactor behind the `tck` Maven profile**. A plain `./mvnw install`
> neither builds nor downloads anything TCK-related.

## Architecture

Tassis is a **Jakarta Messaging 3.1 provider** with **zero third-party dependency**. Being a
provider, it ships its own broker: in-VM and entirely in memory, network transport later
(ROADMAP M11).

```
tassis-api         ← Aggregator: re-exposes jakarta.jms-api (already modular, no fork)
tassis-core        ← The provider: administered objects (TassisConnectionFactory, TassisQueue,
                     TassisTopic),
                     internal.message, internal.broker, internal.client, internal.selector
tassis-cdi-vauban  ← JMSContext injection (§12.4) on Vauban: two beans and a facade
tassis-cdi-vauban-module-it ← Proof of the above on the module path; never published
tassis-bench       ← Placeholder — JMH (M12)
tassis-examples    ← The examples of the documentation, compiled and run by the build
tassis-tck         ← Official TCK runner, in-reactor behind `tck`, class path, test scope only
```

`ROADMAP.md` records the design decisions, the spike findings (API module descriptor, TCK
shape, mandatory vs optional parts of the spec) and the milestones. Read it before starting
a milestone.

## Architecture Constraints Not to Violate

1. **`tassis-core` depends on nothing beyond `jakarta.jms-api`** — no CDI, no JNDI, no
   third-party library.
2. **`io.vidocq.tassis.core` is the only exported package**, and holds only what a client
   must name (the administered objects). Everything else lives under
   `io.vidocq.tassis.core.internal.*`.
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
   **Authentication is deferred to M11**: until then any user name and password are accepted.
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
each subscription's storage, after which a subscription behaves exactly like a queue. A single
lock guards the broker **and** the delivery-related state of its clients (connection started,
consumer closed):
change such state only through `Broker.update(...)`, which also wakes the waiting receives. A
send hands the broker a snapshot (`Messages.snapshot`), never the client's own message.
`Destinations.resolve(...)` is the one place that decides which `Destination` objects are ours.
The subscription rules of §8.3 (reuse, replacement, conflicts) are all in the private
`Broker.named(Subscribing)`, not to be confused with the registry lookup `Broker.named(String)`.

`Broker.receive` returns a `Delivery`; from then on the consumer's **session owns it** and must
either settle it (commit, acknowledge, or automatic acknowledgement on delivery) or hand it back
with `Broker.redeliver`. Losing a `Delivery` loses the message. `TassisSession.work` guards the
session's unsettled deliveries and uncommitted sends; the only lock order is `work`, then the
broker lock.

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
  restriction. A `shut()` never throws and returns once the child is quiet.
- A dispatcher that cannot deliver the message it took gives it back with
  `Broker.returnUndelivered`, not `redeliver`: the application never saw it.

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
  application, else the settings read from the system properties. This is the only place where
  Tassis reads system properties by itself.
- A change here is proven twice: `JmsContextInjectionTest`, and the module-path vehicle
  `tassis-cdi-vauban-module-it`, which must keep passing with nothing opened.

## Selectors (`internal.selector`)

`Selector.parse` compiles a selector, or returns `null` for none. `SelectorParser` holds the
grammar (as a comment) and the checks made when a selector is given; `Expression` is the tree and
its evaluation, where `null` is SQL's NULL / unknown. A subscription filters at publication
(`Broker.send`). A queue keeps one `View` per selector in use, updated by `Storage.add` and
`Storage.remove`: always go through those two to change what a queue holds.

## Conventions

- **Maven groupId**: `io.vidocq.tassis`. Version `0.4.0-SNAPSHOT`, parent
  `io.vidocq:vidocq-parent:0.4.0-SNAPSHOT`.
- **Java modules**: `io.vidocq.tassis.api`, `io.vidocq.tassis.core`, `io.vidocq.tassis.cdi.vauban`.
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
`getting-started`, `usage`, `concepts`, `internals`, `tck`, `reference`, `migration`. There is no
`performance` page before M12 and its `BENCH.md` entries. The logo is `tassis-logo.svg`, at the
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
