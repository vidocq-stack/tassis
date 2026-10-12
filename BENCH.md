# BENCH.md — Tassis

JMH performance numbers. Entry format: `date · hardware/JVM · exact command ·
raw results · delta vs previous run`. No performance numbers elsewhere (README, commit)
without a corresponding entry here.

The benchmarks are in `tassis-bench`, behind the `bench` profile; the Javadoc of each says
which question it answers, and `Rig` who is behind the name of a provider.

**How to read them.**

- **The machine is a desktop under WSL2**, not a quiet server: an interval smaller than the
  error printed beside it means nothing, and no conclusion below rests on one.
- **The callers are the threads of JMH, platform threads.** What Tassis starts itself
  (listeners, the server, the pool) runs on virtual threads, as in an application.
- **A send does not mean the same thing everywhere.** Messages are non-persistent. Tassis
  returns from `send` when the broker has the message, in the JVM and through the network
  alike. The client of Artemis and Qpid JMS return before the broker answered. This favours
  them where many messages are sent in a row (`ListenerBench`), and matters little where a
  message is received before the next is sent.
- **Artemis keeps nothing on disk and checks no user**, as Tassis does. Everything else is
  what each provider does by default, unless the entry says otherwise.

---

## BENCH-20261012-02 — The server sends its frames without waiting (BUG T-002)

- **Date**: 2026-10-12
- **Commit**: the commit of the fix (branch `feat/m12-benchmarks`)
- **JVM**: Temurin 25+36-LTS, default flags
- **Hardware**: AMD Ryzen 7 5800X (8 cores, 16 threads), 16 GB given to WSL2
- **OS**: Ubuntu 20.04.6 LTS under WSL2, Linux 5.15.167.4
- **Exact command**: the same as BENCH-20261012-01
- **Results**:
  ```
  Benchmark                  (provider)  (size)   Mode  Cnt     Score     Error  Units
  RoundTripBench.roundTrip  tassis-amqp     100  thrpt   10  2746.241 ± 107.912  ops/s
  RoundTripBench.roundTrip  qpid-tassis     100  thrpt   10  4586.959 ± 161.706  ops/s
  Benchmark               (broker)  (source)  Mode  Cnt     Score    Error  Units
  PoolBench.openAndClose   network    direct    ss   20  1071.138 ± 37.377  us/op
  Benchmark              (mode)   (provider)   Mode  Cnt      Score      Error  Units
  ListenerBench.deliver    auto  tassis-amqp  thrpt   10   5729.100 ±  347.949  ops/s
  ListenerBench.deliver    auto  qpid-tassis  thrpt   10  37740.835 ± 1946.785  ops/s
  ```
- **Comparison with the previous run** (BENCH-20261012-01):

  | Benchmark | before | after | |
  |---|---|---|---|
  | `RoundTripBench`, Qpid JMS → Tassis server | 20.6 ops/s | 4 587 ops/s | **222× faster** |
  | `PoolBench`, a connection opened by the Tassis client | 50.8 ms | 1.07 ms | **47× faster** |
  | `ListenerBench`, Qpid JMS → Tassis server | 7 520 ops/s | 37 741 ops/s | **5.0× faster** |
  | `ListenerBench`, Tassis client → Tassis server | 2 451 ops/s | 5 729 ops/s | **2.3× faster** |
  | `RoundTripBench`, Tassis client → Tassis server | 2 545 ops/s | 2 746 ops/s | within the error |

  The round trip of the Tassis client did not move: its own socket already sent without
  waiting, and it acknowledges at once what the server sends, having something to say back.

## BENCH-20261012-01 — The network before the fix of BUG T-002

The first run of the benchmarks showed a third-party client some 48 ms an exchange against a
`TassisServer`. Kept as the reference the fix is measured against.

- **Date**: 2026-10-12
- **Commit**: 83a0d17, with the benchmarks of the commit that follows it
- **JVM**: Temurin 25+36-LTS, default flags
- **Hardware**: AMD Ryzen 7 5800X (8 cores, 16 threads), 16 GB given to WSL2
- **OS**: Ubuntu 20.04.6 LTS under WSL2, Linux 5.15.167.4
- **Exact command**:
  ```bash
  ./mvnw -ntp -Pbench -pl tassis-bench -am verify        # Maven 3.9.16
  java -jar tassis-bench/target/benchmarks.jar RoundTripBench -p provider=tassis-amqp,qpid-tassis -p size=100
  java -jar tassis-bench/target/benchmarks.jar PoolBench -p broker=network -p source=direct
  java -jar tassis-bench/target/benchmarks.jar ListenerBench -p provider=tassis-amqp,qpid-tassis -p mode=auto
  ```
  Forks, warm-up and measurement are those each benchmark declares.
- **Results**:
  ```
  Benchmark                  (provider)  (size)   Mode  Cnt     Score     Error  Units
  RoundTripBench.roundTrip  tassis-amqp     100  thrpt   10  2545.179 ± 195.140  ops/s
  RoundTripBench.roundTrip  qpid-tassis     100  thrpt   10    20.640 ±   1.362  ops/s
  Benchmark               (broker)  (source)  Mode  Cnt      Score     Error  Units
  PoolBench.openAndClose   network    direct    ss   20  50765.148 ± 404.642  us/op
  Benchmark              (mode)   (provider)   Mode  Cnt     Score      Error  Units
  ListenerBench.deliver    auto  tassis-amqp  thrpt   10  2451.336 ±  344.398  ops/s
  ListenerBench.deliver    auto  qpid-tassis  thrpt   10  7519.694 ± 2908.742  ops/s
  ```
- **Comparison with the previous run**: none, this is the first.
