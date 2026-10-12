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

## BENCH-20261012-03 — The first campaign: every benchmark, every provider

- **Date**: 2026-10-12
- **Commit**: the commit of the fix of BUG T-002 (branch `feat/m12-benchmarks`)
- **JVM**: Temurin 25+36-LTS, default flags
- **Hardware**: AMD Ryzen 7 5800X (8 cores, 16 threads), 16 GB given to WSL2
- **OS**: Ubuntu 20.04.6 LTS under WSL2, Linux 5.15.167.4
- **Versions compared with**: Apache ActiveMQ Artemis 2.57.0, Apache Qpid JMS 2.11.0
- **Exact command** (1 h 12 min):
  ```bash
  ./mvnw -ntp -Pbench -pl tassis-bench -am verify        # Maven 3.9.16
  java -jar tassis-bench/target/benchmarks.jar -prof gc -rf json
  ```
  Forks (2), warm-up (3 × 2 s) and measurement (5 × 3 s) are those each benchmark declares;
  `DrainBench` and `PoolBench` are single shots (5 to warm up, 10 measured, per fork).
- **Results**: the lines of JMH, one table per benchmark; `Alloc` is `gc.alloc.rate.norm`,
  what the whole JVM allocates per operation, client and broker together. The throughput of
  several threads is their sum.
  ```
  Benchmark                     (provider)  (size)   Mode  Cnt        Score        Error  Units   Alloc
  RoundTripBench.roundTrip       tassis-vm     100  thrpt   10  3293994.249  ± 81427.009  ops/s    2096  B/op
  RoundTripBench.roundTrip       tassis-vm   10000  thrpt   10   169754.129   ± 3367.133  ops/s   61472  B/op
  RoundTripBench.roundTrip      artemis-vm     100  thrpt   10    10725.004    ± 188.860  ops/s    6275  B/op
  RoundTripBench.roundTrip      artemis-vm   10000  thrpt   10     9346.359    ± 118.838  ops/s   42826  B/op
  RoundTripBench.roundTrip     tassis-amqp     100  thrpt   10     2771.444    ± 140.459  ops/s   38083  B/op
  RoundTripBench.roundTrip     tassis-amqp   10000  thrpt   10     2389.230     ± 67.107  ops/s  552822  B/op
  RoundTripBench.roundTrip     qpid-tassis     100  thrpt   10     4748.068    ± 128.586  ops/s   22814  B/op
  RoundTripBench.roundTrip     qpid-tassis   10000  thrpt   10     4312.974    ± 112.582  ops/s  322892  B/op
  RoundTripBench.roundTrip  tassis-artemis     100  thrpt   10     2505.017     ± 89.018  ops/s   23565  B/op
  RoundTripBench.roundTrip  tassis-artemis   10000  thrpt   10     2340.283     ± 77.024  ops/s  305427  B/op
  RoundTripBench.roundTrip    qpid-artemis     100  thrpt   10     6027.156    ± 235.653  ops/s    9263  B/op
  RoundTripBench.roundTrip    qpid-artemis   10000  thrpt   10     5591.678    ± 182.205  ops/s   72211  B/op

  Benchmark                  (provider)  (queues)   Mode  Cnt        Score         Error  Units  Alloc
  ContentionBench.threads01   tassis-vm       own  thrpt   10  3372454.683  ± 103147.492  ops/s   2080  B/op
  ContentionBench.threads01   tassis-vm    shared  thrpt   10  3396111.713   ± 56116.147  ops/s   2080  B/op
  ContentionBench.threads01  artemis-vm       own  thrpt   10     6772.028     ± 112.948  ops/s   7254  B/op
  ContentionBench.threads01  artemis-vm    shared  thrpt   10     6485.493     ± 234.254  ops/s   7243  B/op
  ContentionBench.threads02   tassis-vm       own  thrpt   10  1905731.850   ± 55090.781  ops/s   2095  B/op
  ContentionBench.threads02   tassis-vm    shared  thrpt   10  1952378.088   ± 63422.852  ops/s   2085  B/op
  ContentionBench.threads02  artemis-vm       own  thrpt   10    14820.121     ± 209.422  ops/s   7177  B/op
  ContentionBench.threads02  artemis-vm    shared  thrpt   10    14656.170     ± 278.525  ops/s   7098  B/op
  ContentionBench.threads04   tassis-vm       own  thrpt   10  1540217.401   ± 87194.792  ops/s   2081  B/op
  ContentionBench.threads04   tassis-vm    shared  thrpt   10  1498565.160   ± 40631.870  ops/s   2081  B/op
  ContentionBench.threads04  artemis-vm       own  thrpt   10    26753.130     ± 438.357  ops/s   7160  B/op
  ContentionBench.threads04  artemis-vm    shared  thrpt   10    27166.834     ± 283.068  ops/s   7019  B/op
  ContentionBench.threads08   tassis-vm       own  thrpt   10  1528499.308  ± 101369.128  ops/s   2081  B/op
  ContentionBench.threads08   tassis-vm    shared  thrpt   10  1295693.603   ± 76442.624  ops/s   2090  B/op
  ContentionBench.threads08  artemis-vm       own  thrpt   10    53506.416     ± 777.187  ops/s   7076  B/op
  ContentionBench.threads08  artemis-vm    shared  thrpt   10    52972.853     ± 748.060  ops/s   6874  B/op
  ContentionBench.threads16   tassis-vm       own  thrpt   10  1462773.996   ± 40522.520  ops/s   2089  B/op
  ContentionBench.threads16   tassis-vm    shared  thrpt   10  1241525.333   ± 90448.944  ops/s   2081  B/op
  ContentionBench.threads16  artemis-vm       own  thrpt   10    94071.747    ± 5366.451  ops/s   7019  B/op
  ContentionBench.threads16  artemis-vm    shared  thrpt   10    92416.669    ± 2694.522  ops/s   6846  B/op

  Benchmark                   (provider)  (waiters)   Mode  Cnt        Score         Error  Units  Alloc
  IdleWaitersBench.roundTrip   tassis-vm          0  thrpt   10  3282363.114  ± 100542.372  ops/s   2096  B/op
  IdleWaitersBench.roundTrip   tassis-vm        100  thrpt   10   217751.296   ± 17091.619  ops/s   2310  B/op
  IdleWaitersBench.roundTrip   tassis-vm       1000  thrpt   10    18216.090    ± 7726.251  ops/s   5850  B/op
  IdleWaitersBench.roundTrip  artemis-vm          0  thrpt   10    10768.038     ± 157.577  ops/s   6242  B/op
  IdleWaitersBench.roundTrip  artemis-vm        100  thrpt   10    10656.348     ± 158.899  ops/s   6422  B/op
  IdleWaitersBench.roundTrip  artemis-vm       1000  thrpt   10    10419.176     ± 855.881  ops/s   6468  B/op

  Benchmark               (delayed)  (provider)   Mode  Cnt        Score        Error  Units  Alloc
  BacklogBench.roundTrip          0   tassis-vm  thrpt   10  3264171.725  ± 57545.669  ops/s   2096  B/op
  BacklogBench.roundTrip          0  artemis-vm  thrpt   10    10661.016    ± 337.062  ops/s   6266  B/op
  BacklogBench.roundTrip       1000   tassis-vm  thrpt   10   196387.782   ± 4219.343  ops/s   2160  B/op
  BacklogBench.roundTrip       1000  artemis-vm  thrpt   10    10707.812    ± 189.101  ops/s   6394  B/op
  BacklogBench.roundTrip      10000   tassis-vm  thrpt   10    10340.360   ± 2924.135  ops/s   2243  B/op
  BacklogBench.roundTrip      10000  artemis-vm  thrpt   10     9414.157    ± 169.017  ops/s   6274  B/op

  Benchmark                  (mode)   (provider)   Mode  Cnt       Score        Error  Units  Alloc
  ListenerBench.deliver        auto    tassis-vm  thrpt   10  499640.151  ± 36452.391  ops/s   2147  B/op
  ListenerBench.deliver        auto   artemis-vm  thrpt   10  132171.904   ± 4066.499  ops/s   6072  B/op
  ListenerBench.deliver        auto  tassis-amqp  thrpt   10    6106.697    ± 121.744  ops/s  37767  B/op
  ListenerBench.deliver        auto  qpid-tassis  thrpt   10   37886.938   ± 1686.617  ops/s  22721  B/op
  ListenerBench.deliver     dups-ok    tassis-vm  thrpt   10  491127.480  ± 26976.446  ops/s   2146  B/op
  ListenerBench.deliver     dups-ok   artemis-vm  thrpt   10  137313.930   ± 5234.708  ops/s   5354  B/op
  ListenerBench.deliver     dups-ok  tassis-amqp  thrpt   10    5990.262    ± 105.822  ops/s  37738  B/op
  ListenerBench.deliver     dups-ok  qpid-tassis  thrpt   10   37478.311    ± 686.817  ops/s  22734  B/op
  ListenerBench.deliver  transacted    tassis-vm  thrpt   10  518201.117  ± 14456.077  ops/s   2335  B/op
  ListenerBench.deliver  transacted   artemis-vm  thrpt   10   11118.730    ± 105.186  ops/s   6850  B/op
  ListenerBench.deliver  transacted  tassis-amqp  thrpt   10    5943.895    ± 195.296  ops/s  37726  B/op
  ListenerBench.deliver  transacted  qpid-tassis  thrpt   10    4545.801     ± 97.640  ops/s  30956  B/op

  Benchmark         (broker)   (client)  Mode  Cnt    Score    Error  Units  Alloc
  DrainBench.drain    tassis     tassis    ss   20  182.365  ± 2.526  us/op  40276  B/op
  DrainBench.drain    tassis     qpid-1    ss   20  190.547  ± 2.134  us/op  24754  B/op
  DrainBench.drain    tassis  qpid-1000    ss   20    6.767  ± 0.461  us/op  23385  B/op
  DrainBench.drain   artemis     tassis    ss   20  190.951  ± 2.025  us/op  26479  B/op
  DrainBench.drain   artemis     qpid-1    ss   20  140.328  ± 2.889  us/op  10109  B/op
  DrainBench.drain   artemis  qpid-1000    ss   20    5.514  ± 0.236  us/op   9679  B/op

  Benchmark                          (provider)   Mode  Cnt        Score        Error  Units  Alloc
  SessionBench.sessionPerRoundTrip    tassis-vm  thrpt   10  1931920.201  ± 86285.497  ops/s   2672  B/op
  SessionBench.sessionPerRoundTrip   artemis-vm  thrpt   10     1434.724     ± 49.840  ops/s  56785  B/op
  SessionBench.sessionPerRoundTrip  tassis-amqp  thrpt   10     1297.366     ± 67.912  ops/s  51780  B/op
  SessionBench.sessionPerRoundTrip  qpid-tassis  thrpt   10      688.177     ± 58.876  ops/s  71977  B/op

  Benchmark               (broker)  (source)  Mode  Cnt     Score     Error  Units  Alloc
  PoolBench.openAndClose        vm    direct    ss   20     0.949   ± 0.368  us/op    916  B/op
  PoolBench.openAndClose        vm    pooled    ss   20     5.367   ± 1.742  us/op    708  B/op
  PoolBench.openAndClose   network    direct    ss   20  1037.750  ± 26.507  us/op  83559  B/op
  PoolBench.openAndClose   network    pooled    ss   20     7.604   ± 2.007  us/op    846  B/op
  ```
- **Comparison with the previous run**: the cases of BENCH-20261012-02 are within its errors
  (`RoundTripBench` 2 771 and 4 748 ops/s for 2 746 and 4 587; `PoolBench` 1 038 µs for
  1 071; `ListenerBench` 6 107 and 37 887 ops/s for 5 729 and 37 741). The rest is measured
  for the first time.

**What the figures say**, each to the question its benchmark was written for.

- **The single broker lock does not scale, and does not need to yet** (`ContentionBench`).
  One thread does 3.4 million round trips a second; two do 1.9 million between them, sixteen
  1.5 million. Queues of their own or a shared one make no difference before eight threads:
  the limit is the lock, not the queue. Artemis scales with its threads, from 6 800 to 94 000,
  and is still fifteen times under at sixteen. A broker of the JVM serves 1.5 million round
  trips a second whatever the number of its clients, which is five hundred times what one
  client of the network asks of it.
- **A receive that waits costs everybody** (`IdleWaitersBench`). 3.3 million round trips a
  second with nobody waiting, 218 000 with a hundred consumers waiting on other queues,
  18 000 with a thousand: every change wakes them all, and each looks at its queue again.
  Artemis does not move. This is the cost of the single condition, and the first thing to fix.
- **Delayed messages are scanned by every receive** (`BacklogBench`). 3.3 million round trips
  a second on an empty queue, 196 000 behind a thousand delayed messages, 10 000 behind ten
  thousand, where Artemis, which keeps them aside, stays at 10 000 throughout.
- **How a listener's session acknowledges does not show** (`ListenerBench`). 500 000,
  491 000 and 518 000 messages a second for automatic, lazy and transacted: within the
  errors. The lock acquisition M11.1 added is not measurable.
- **One credit at a time is what the Tassis client costs on the network** (`DrainBench`).
  182 µs a message, the same as Qpid JMS told to fetch one message at a time (191 µs), and
  twenty-seven times more than Qpid JMS as it comes, fetching a thousand ahead (6.8 µs). The
  figures against Artemis say the same (191, 140, 5.5). The round trip shows it too: 2 771 a
  second for the Tassis client where Qpid JMS does 4 748 against the same server, and
  delivery to a listener, 6 107 for 37 887.
- **The Tassis server is within reach of Artemis.** Qpid JMS does 4 748 round trips a second
  against it and 6 027 against Artemis; it empties a queue at 6.8 µs a message for 5.5.
- **The AMQP code allocates a lot.** A round trip of 100 bytes allocates 38 kB with the
  Tassis client and the Tassis server, 9 kB with Qpid JMS and Artemis; one of 10 kB, 553 kB
  for 72 kB. Both sides take part: 23 kB with only one of the two.
- **A session costs nothing in the JVM and 0.4 ms on the network** (`SessionBench`, beside
  `RoundTripBench` at 100 bytes). In the JVM a session, a producer and a consumer opened for
  each message take the round trip from 0.30 µs to 0.52 µs. Through the network they take it
  from 361 µs to 771 µs; Qpid JMS goes from 211 µs to 1 453 µs.
- **The pool is worth it on the network and only there** (`PoolBench`). A connection of the
  network is opened and closed in 1 038 µs and borrowed and returned in 7.6 µs; one of the
  JVM in 0.9 µs, less than the 5.4 µs of the pool. That is the choice M11b-2 made for
  injected contexts.
- **In the JVM, Tassis is three hundred times faster than Artemis in the JVM** on a round trip
  (3.3 million a second for 10 700), and eighteen times on 10 kB. Not a fair fight and not meant
  as one: Artemis reaches its broker of the JVM through its protocol and its thread pools,
  Tassis through a method call. It is the figure of what an in-VM broker is for.

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
