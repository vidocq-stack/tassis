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

# TCK profile (currently a placeholder module — the harness is wired in ROADMAP M2)
./mvnw -ntp -Ptck clean install
```

> `tassis-tck` is **in-reactor behind the `tck` Maven profile**. A plain `./mvnw install`
> neither builds nor downloads anything TCK-related.

## Architecture

Tassis is a **Jakarta Messaging 3.1 provider** with **zero third-party dependency**. Being a
provider, it ships its own broker: in-VM today, network transport later (ROADMAP M11).

```
tassis-api         ← Aggregator: re-exposes jakarta.jms-api (already modular, no fork)
tassis-core        ← The provider. Only module with code today: TassisConnectionFactory, a
                     skeletal TassisConnection (ROADMAP M0) and the message model in
                     internal.message (ROADMAP M1)
tassis-cdi-vauban  ← Placeholder — JMSContext injection (M9)
tassis-bench       ← Placeholder — JMH (M12)
tassis-examples    ← Placeholder — usage examples
tassis-tck         ← Placeholder — official TCK runner, in-reactor behind `tck` (wired in M2)
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
6. **Out of scope by decision** (see ROADMAP): XA, a `vidocq` runtime extension, CDI message
   listeners. Do not start on them without a roadmap change.
7. **JUnit 6** (`org.junit:junit-bom`, managed by `vidocq-parent`) for all tests — no Mockito;
   hand-written fixtures only.
8. **Language** — commit messages, Javadoc, comments and all `.md` content in **English**.

## Message model (`internal.message`)

`TassisMessage` (headers, properties, body template) and one subclass per body type.
`ValueConversions` is the single conversion table for properties, `MapMessage` and
`StreamMessage`: change a conversion there, never in a message class. `markReceived()` puts a
message in the state a consumer sees it in (properties and body read-only, body rewound).

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

Runner: `jakarta.tck:jms-tck` (JUnit 5, Maven Central), not the JavaTest zip. Administered
objects are supplied through `com.sun.ts.lib.porting.TSJMSObjectsInterface`, selected by the
system property `porting.ts.jmsObjects.class.1` — no JNDI. Scope is `core` + `core20`
(905 tests); `ee` / `ee20` need EJB or a web container and are out of reach. Details and
open questions in ROADMAP "Spike findings".

**Release discipline (once M8 lands):** no structural merge on `tassis-core` without TCK
PASS at 100%. Any challenge goes in a `TCK.md` with spec citation and reactivation plan.

## AI Principles — Collaboration on This Repository

- **Plan mode by default** on any structural change (new module, new SPI).
- **Cite the spec, not just the test**, in commit messages and PR descriptions.
- Use agents **`java-modules-guardian`**, **`virtual-threads-reviewer`**,
  **`dependency-gatekeeper`** on any `module-info.java` modification, concurrent code, or
  `pom.xml` change.
- If the rules in this file need updating, keep `AGENTS.md` aligned.

## Documentation (Antora) conventions

Not created yet (ROADMAP M12). Follow `AGENTS.md`: `docs/en` component, Vauban as the
reference layout, `tck` then `performance` sections.

## Terminology

Use **Java Modules** (or **Java module** for a single module) when referring to
the Java Platform Module System. Do **not** use the abbreviation **JPMS** — in
prose, identifiers, or documentation.
