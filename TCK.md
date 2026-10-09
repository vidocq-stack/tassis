# Tassis — Jakarta Messaging TCK

## Runner

| Item | Value |
|---|---|
| TCK | `jakarta.tck:jms-tck:11.0.3` (JUnit 5, "Jakarta Messaging Outside container"), compiled against `jakarta.jms-api:3.1.0` |
| Module | `tassis-tck`, in-reactor behind the `tck` Maven profile, test scope only, class path |
| Script | `./run-official-tck-messaging-3.1.sh` (report in `tassis-tck/target/tck-report.txt`) |
| Porting class | `io.vidocq.tassis.tck.TassisJmsObjects` (`TSJMSObjectsInterface`): no JNDI |
| Scope | `core` + `core20` (905 tests). `ee` / `ee20` (255 tests) need EJB, MDB or a web container |

How the wiring works:

- The TCK harness (`com.sun.ts.lib.util.TestUtil`) does not read the porting class from the
  system properties but from the file `${java.io.tmpdir}/${user.name}-cts-props.txt`. The build
  copies `tassis-tck/src/test/tck/tassis-cts-props.txt` to `target/tck-tmp` and points both
  properties at it for the test JVM.
- The tests read `jms_timeout`, `user`, `password` and `platform.mode` from the system
  properties; `tassis-tck/pom.xml` sets them. `jms_timeout` is 3000 ms (TCK default: 10000).
- `PortingSmokeTest` exercises the wiring outside any TCK test. Run it first when everything
  fails at once: the TCK tests swallow the exception of a failed setup.

## Status — M3

**219 TCK tests passed / 0 failed** on the selection of `tck.test` (2026-10-09,
`./mvnw -ntp -Ptck clean install`; the run reports 220 because it also executes `PortingSmokeTest`).

The selection is 18 packages of `core` (234 tests): the 16 queue packages, `sessiontests` and
`appclient/txqueuetests`, minus the 15 tests below, which need a later milestone. The list lives
in the `tck.test` property of `tassis-tck/pom.xml`; a milestone is done when its lines are gone
from it.

| Milestone | Test | Needs |
|---|---|---|
| M4 | `ExceptionQueueTestsIT#xIllegalStateExceptionTestTopicMethodsQ` | topic methods on a `QueueSession` |
| M4 | `MessageProducerTestsIT#sendTopicTest4`, `#sendTopicTest5`, `#sendTopicTest6` | topics |
| M4 | `SessionTestsIT#SimpleSendAndReceiveT`, `#SubscriberTests`, `#ackTests`, `#InvalidDestinationTests` | topics |
| M5 | `ExceptionQueueTestsIT#xInvalidSelectorExceptionQueueTest` | selector parsing |
| M5 | `QueueConnectionTestsIT#connNotStartedQueueTest` | selectors |
| M5 | `QueueTestsIT#queueBrowserMsgsRemainOnQueueTest`, `#messageSelectorMsgRemainsOnQueueTest`, `#msgSelectorMsgHeaderQueueTest` | selectors |
| M5 | `SessionTestsIT#selectorAndBrowserTests`, `#InvalidSelectorTests` | selectors (the second also needs topics) |

`appclient/txqueuetests` passing settles one of spike 2's open points for this package: an
`appclient` package runs as-is in `standalone` mode. The other `appclient` packages are not tried yet.

Not selected yet: the other 24 packages of `core` and all of `core20`.

History: M2 199 / 199 on 16 packages; M3 re-enabled the 7 tests waiting for it and added 2 packages.

## Deviations from the TCK as published

### `jakarta.tck:common` pinned to 11.0.8 (TCK defect)

`jms-tck:11.0.3` declares `jakarta.tck:common:11.0.0-RC4`. In that release,
`com.sun.ts.tests.jms.common.BytesMessageTestImpl` never assigns its `dos` field (no
`putfield dos` in the class), so `new BytesMessageTestImpl().writeBoolean(...)` throws a
`NullPointerException` before the provider is involved. It breaks
`ForeignMsgQueueTestsIT#sendReceiveBytesMsgQueueTest` (and its topic twin) with any provider.
`common` 11.0.8 assigns the field; `tassis-tck` pins that version. To be revisited when a
`jms-tck` release declares a fixed `common`.

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
