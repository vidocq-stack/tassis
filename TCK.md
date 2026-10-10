# Tassis — Jakarta Messaging TCK

## Result

| | |
|---|---|
| Specification | Jakarta Messaging 3.1 |
| TCK | `jakarta.tck:jms-tck:11.0.3`, with `jakarta.tck:common:11.0.8` (see Deviations) |
| JDK | Temurin 25 |
| Functional tests | **901 passed, 0 failed** out of 903: all of `core` (642) and `core20` (259 of 261) |
| Signature test | **passed**: `jakarta.jms` of `jakarta.jms-api:3.1.0` against `jakarta.jms.sig` |
| Not passed | 2 tests, negated: authentication, due with M11 (see Status) |
| Not applicable | `ee` and `ee20`, 255 annotations: they need EJB, MDB or a web container |

Tassis does not claim compatibility: two tests of the applicable set are not passed yet, and
this file is a working record, not a certification request.

## Runner

| Item | Value |
|---|---|
| TCK | `jakarta.tck:jms-tck:11.0.3` (JUnit 5, "Jakarta Messaging Outside container"), compiled against `jakarta.jms-api:3.1.0` |
| Module | `tassis-tck`, in-reactor behind the `tck` Maven profile, test scope only, class path |
| Script | `./run-official-tck-messaging-3.1.sh` (report in `tassis-tck/target/tck-report.txt`) |
| Porting class | `io.vidocq.tassis.tck.TassisJmsObjects` (`TSJMSObjectsInterface`): no JNDI |
| Scope | `core` + `core20` (903 tests). `ee` / `ee20` (255 `@Test` annotations) need EJB, MDB or a web container |

How the wiring works:

- The TCK harness (`com.sun.ts.lib.util.TestUtil`) does not read the porting class from the
  system properties but from the file `${java.io.tmpdir}/${user.name}-cts-props.txt`. The build
  copies `tassis-tck/src/test/tck/tassis-cts-props.txt` to `target/tck-tmp` and points both
  properties at it for the test JVM.
- The tests read `jms_timeout`, `user`, `password` and `platform.mode` from the system
  properties; `tassis-tck/pom.xml` sets them. `jms_timeout` is 3000 ms (TCK default: 10000).
- `PortingSmokeTest` exercises the wiring outside any TCK test. Run it first when everything
  fails at once: the TCK tests swallow the exception of a failed setup.

## Status — M7 (unchanged by M8, M8b and M8c)

**901 TCK tests passed / 0 failed** on the selection of `tck.test`: the whole of `core` (642
tests, 46 packages) and of `core20` (259 of its 261 tests, 15 packages). The run reports 902
because it also executes `PortingSmokeTest`, and takes about 13 minutes: many `core20` tests
wait on purpose (2026-10-10, `./mvnw -ntp -Ptck clean install`).

Two tests are negated in the `tck.test` property of `tassis-tck/pom.xml`, both waiting for
authentication, which is part of M11:

| Class | Tests | Needs |
|---|---|---|
| `core20/connectionfactorytests/ClientIT` | `createConnectionExceptionTests`, `createJMSContextExceptionTests` | `JMSSecurityException` for the credentials `("invalid", "invalid")`; Tassis accepts any credentials today |

Out of reach: `ee` and `ee20` (255 annotations), which need EJB, MDB or a web container.

History: M2 199 on 16 packages; M3 219 on 18; M4 458 on 35; M5 598 on 37; M6 642 on 46 (all of
`core`); M7 901 on 61 (`core` + `core20`).

`core` has 644 `@Test` annotations but 642 tests: in `QueueHeaderTestsIT` and
`TopicHeaderTestsIT` the annotation also sits on a private helper taking a parameter
(`chkMessageID(String)`), which JUnit does not run. The 905 of the spike for `core` + `core20`
was a count of annotations; the real total is 903.

The `core20` tests read one more system property, `vehicle`, to tell a container from a Java SE
client: `tassis-tck` sets it to `standalone`.

The porting class hands out `TassisConnectionFactory().withClientID("cts")` for
`DURABLE_SUB_CONNECTION_FACTORY`, which is what the TCK uses for every topic test: the suite
never calls `Connection.setClientID` itself in `core`.

## Signature test

`./run-official-tck-messaging-3.1.sh signature` checks that the API `tassis-api` exposes is
exactly the `jakarta.jms` API of the specification. It runs `jakarta.tck:sigtest-maven-plugin`
2.7 in static mode on the classes of `jakarta.jms-api:3.1.0`, against the TCK's `jakarta.jms.sig`.

- The signature file is published in the JavaTest distribution of the TCK only
  (`jakarta-messaging-tck-3.1.0.zip`), under the Eclipse Foundation TCK License. It is not kept
  in this repository: the script downloads the distribution into
  `tassis-tck/target/tck-distribution` and extracts the file.
- One class is ignored, `java.lang.Object`: the file was recorded on a JDK where
  `Object.finalize()` was not yet deprecated for removal, a difference every class inherits and
  which is the JDK's. Without that option the test reports 52 "added annotations", all of them
  this one; with it, none.
- Control: run against `jakarta.jms-api:3.0.0` the test fails, on exactly what 3.1 added (the
  three-argument exception constructors and the repeatable resource definition annotations).

`tassis-api` adds nothing to the upstream artifact, so the test passes by construction. It is
there because a compatible implementation has to pass it, and it would catch a fork of the API.

## Deviations from the TCK as published

### `jakarta.tck:common` pinned to 11.0.8 (TCK defect)

`jms-tck:11.0.3` declares `jakarta.tck:common:11.0.0-RC4`. In that release,
`com.sun.ts.tests.jms.common.BytesMessageTestImpl` never assigns its `dos` field (no
`putfield dos` in the class), so `new BytesMessageTestImpl().writeBoolean(...)` throws a
`NullPointerException` before the provider is involved. It breaks
`ForeignMsgQueueTestsIT#sendReceiveBytesMsgQueueTest` (and its topic twin) with any provider.
`common` 11.0.8 assigns the field; `tassis-tck` pins that version. To be revisited when a
`jms-tck` release declares a fixed `common`.

### Test methods run in alphabetical order (TCK defect)

Some TCK classes only pass for some orders of their tests. In
`appclient/txtopictests/TxTopicTestsIT`, `transactionRollbackOnPublishTopicTest` creates the
durable subscription `TxTopicTestSubscription` and never unsubscribes; two other tests of the
class reuse that name, and assert on the text of the first message they receive. If a test
publishing another text to the same topic runs in between, they get that stale message, as any
correct provider must deliver it, and fail with "didn't get the right message". JUnit 5's
default order produces exactly that sequence; the alphabetical order does not. `tassis-tck`
sets `junit.jupiter.testmethod.order.default` to `MethodOrderer$MethodName`. No test is
skipped or altered by it.

### A timeout on every test

`tassis-tck` sets `junit.jupiter.execution.timeout.default` to 2 minutes. Several TCK tests wait
on a latch that only a message listener releases; when the provider gets it wrong they wait for
ever and hang the whole run. The timeout turns that into a failure. The slowest class takes 12
seconds.

### Harness dependencies excluded

`jakarta.tck:common` brings Derby, Ant and `commons-httpclient` 3.1, which the standalone
Messaging tests never load. `tassis-tck` excludes them; the selection passes without them.

### In-reactor runner

The workspace keeps the older per-brick TCK runners out of their reactor. `tassis-tck` is
in-reactor behind the `tck` profile, like `erasmus-tck` and the `vidocq-runtime-tck-*` runners:
Tassis was created after the Maven 3.9.16 / Model 4.0.0 migration that removed the reason for
the separation.

## Challenges

None filed.
