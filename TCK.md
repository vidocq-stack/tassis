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

## Status — M6

**642 TCK tests passed / 0 failed**: the whole of `core`, its 46 packages including the 11
`appclient` ones, with no test negated (2026-10-09, `./mvnw -ntp -Ptck clean install`; the run
reports 643 because it also executes `PortingSmokeTest`).

`core` has 644 `@Test` annotations but 642 tests: in `QueueHeaderTestsIT` and
`TopicHeaderTestsIT` the annotation also sits on a private helper taking a parameter
(`chkMessageID(String)`), which JUnit does not run. The 905 of the spike for `core` + `core20`
was a count of annotations.

Not selected yet: `core20`, 15 packages and 261 annotations (simplified API, M7).

History: M2 199 on 16 packages; M3 219 on 18; M4 458 on 35; M5 598 on 37; M6 642 on 46.

Every `appclient` package of `core` runs as-is in `standalone` mode, which closes the open point
of spike 2 for `core`.

The porting class hands out `TassisConnectionFactory().withClientID("cts")` for
`DURABLE_SUB_CONNECTION_FACTORY`, which is what the TCK uses for every topic test: the suite
never calls `Connection.setClientID` itself in `core`.

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
