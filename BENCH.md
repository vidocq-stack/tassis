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

## BENCH-20261012-04 — A receive that waits is woken for its own queue only (M12-2)

- **Date**: 2026-10-12
- **Commit**: the commit of M12-2 (branch `feat/m12-2-waiting-receive`)
- **JVM**: Temurin 25+36-LTS, default flags
- **Hardware**: AMD Ryzen 7 5800X (8 cores, 16 threads), 16 GB given to WSL2
- **OS**: Ubuntu 20.04.6 LTS under WSL2, Linux 5.15.167.4
- **Exact command** (52 min):
  ```bash
  ./mvnw -ntp -Pbench -pl tassis-bench -am verify        # Maven 3.9.16
  java -jar tassis-bench/target/benchmarks.jar 'IdleWaitersBench|ContentionBench|RoundTripBench|BacklogBench|ListenerBench|SessionBench' \
      -p provider=tassis-vm,tassis-amqp,qpid-tassis -prof gc -rf json
  ```
  The broker is what changed, so what runs on a Tassis broker is run again, in the JVM and
  behind the server. `-p provider` gives the three providers to every benchmark, including
  three that declare only those of the JVM (`IdleWaitersBench`, `ContentionBench`,
  `BacklogBench`): their lines for the network are new, not a comparison. Four cases are
  missing because they cannot run: Qpid JMS on the shared queue of `ContentionBench` with two
  threads or more, where a consumer that fetches ahead holds the messages another waits for
  (the reason Artemis fetches nothing ahead in that benchmark).
- **Results**:
  ```
  Benchmark                    (provider)  (waiters)   Mode  Cnt        Score         Error  Units  Alloc
  IdleWaitersBench.roundTrip    tassis-vm          0  thrpt   10  3275181.056   ± 61734.462  ops/s   2112  B/op
  IdleWaitersBench.roundTrip    tassis-vm        100  thrpt   10  3185254.641  ± 111555.960  ops/s   2112  B/op
  IdleWaitersBench.roundTrip    tassis-vm       1000  thrpt   10  3187795.533   ± 76151.743  ops/s   2112  B/op
  IdleWaitersBench.roundTrip  tassis-amqp          0  thrpt   10     2784.106     ± 151.198  ops/s  37761  B/op
  IdleWaitersBench.roundTrip  tassis-amqp        100  thrpt   10     2752.541     ± 125.777  ops/s  37758  B/op
  IdleWaitersBench.roundTrip  tassis-amqp       1000  thrpt   10     2695.678      ± 89.272  ops/s  41006  B/op
  IdleWaitersBench.roundTrip  qpid-tassis          0  thrpt   10     4835.625     ± 117.666  ops/s  22656  B/op
  IdleWaitersBench.roundTrip  qpid-tassis        100  thrpt   10     4831.611     ± 115.509  ops/s  22552  B/op
  IdleWaitersBench.roundTrip  qpid-tassis       1000  thrpt   10     4730.168      ± 95.469  ops/s  22684  B/op

  Benchmark                   (provider)  (queues)   Mode  Cnt        Score        Error  Units  Alloc
  ContentionBench.threads01    tassis-vm       own  thrpt   10  3184206.250  ± 90227.285  ops/s   2096  B/op
  ContentionBench.threads01    tassis-vm    shared  thrpt   10  3289585.646  ± 65765.980  ops/s   2096  B/op
  ContentionBench.threads01  tassis-amqp       own  thrpt   10     2705.397    ± 131.830  ops/s  38111  B/op
  ContentionBench.threads01  tassis-amqp    shared  thrpt   10     2706.762    ± 120.463  ops/s  38080  B/op
  ContentionBench.threads01  qpid-tassis       own  thrpt   10     4531.213    ± 178.221  ops/s  22843  B/op
  ContentionBench.threads01  qpid-tassis    shared  thrpt   10     4493.612    ± 258.528  ops/s  22824  B/op
  ContentionBench.threads02    tassis-vm       own  thrpt   10  1962335.750  ± 36430.101  ops/s   2111  B/op
  ContentionBench.threads02    tassis-vm    shared  thrpt   10  1960900.126  ± 77981.198  ops/s   2109  B/op
  ContentionBench.threads02  tassis-amqp       own  thrpt   10     5509.786    ± 189.783  ops/s  38075  B/op
  ContentionBench.threads02  tassis-amqp    shared  thrpt   10     5538.143    ± 254.422  ops/s  38080  B/op
  ContentionBench.threads02  qpid-tassis       own  thrpt   10     9362.783    ± 204.359  ops/s  22902  B/op
  ContentionBench.threads04    tassis-vm       own  thrpt   10  1535364.681  ± 79456.130  ops/s   2097  B/op
  ContentionBench.threads04    tassis-vm    shared  thrpt   10  1364411.656  ± 55432.545  ops/s   2105  B/op
  ContentionBench.threads04  tassis-amqp       own  thrpt   10    11952.139    ± 308.497  ops/s  38122  B/op
  ContentionBench.threads04  tassis-amqp    shared  thrpt   10    11800.498    ± 432.416  ops/s  38138  B/op
  ContentionBench.threads04  qpid-tassis       own  thrpt   10    18085.264    ± 226.948  ops/s  23061  B/op
  ContentionBench.threads08    tassis-vm       own  thrpt   10  1465232.408  ± 38118.459  ops/s   2097  B/op
  ContentionBench.threads08    tassis-vm    shared  thrpt   10  1322928.878  ± 39312.279  ops/s   2097  B/op
  ContentionBench.threads08  tassis-amqp       own  thrpt   10    24166.050    ± 825.973  ops/s  38060  B/op
  ContentionBench.threads08  tassis-amqp    shared  thrpt   10    23903.031    ± 860.236  ops/s  38132  B/op
  ContentionBench.threads08  qpid-tassis       own  thrpt   10    32696.825   ± 2246.295  ops/s  22938  B/op
  ContentionBench.threads16    tassis-vm       own  thrpt   10  1499493.841  ± 32631.820  ops/s   2097  B/op
  ContentionBench.threads16    tassis-vm    shared  thrpt   10  1239066.842  ± 30827.983  ops/s   2098  B/op
  ContentionBench.threads16  tassis-amqp       own  thrpt   10    42787.603   ± 1609.237  ops/s  37984  B/op
  ContentionBench.threads16  tassis-amqp    shared  thrpt   10    41904.525   ± 2209.159  ops/s  38007  B/op
  ContentionBench.threads16  qpid-tassis       own  thrpt   10    54269.071   ± 1907.959  ops/s  22939  B/op

  Benchmark                  (provider)  (size)   Mode  Cnt        Score        Error  Units   Alloc
  RoundTripBench.roundTrip    tassis-vm     100  thrpt   10  3332394.169  ± 62264.908  ops/s    2112  B/op
  RoundTripBench.roundTrip    tassis-vm   10000  thrpt   10   170170.165   ± 1158.038  ops/s   61488  B/op
  RoundTripBench.roundTrip  tassis-amqp     100  thrpt   10     2783.996    ± 146.170  ops/s   38081  B/op
  RoundTripBench.roundTrip  tassis-amqp   10000  thrpt   10     2399.270    ± 100.746  ops/s  552831  B/op
  RoundTripBench.roundTrip  qpid-tassis     100  thrpt   10     4726.682    ± 153.655  ops/s   22845  B/op
  RoundTripBench.roundTrip  qpid-tassis   10000  thrpt   10     4336.141     ± 96.806  ops/s  322918  B/op

  Benchmark               (delayed)   (provider)   Mode  Cnt        Score        Error  Units  Alloc
  BacklogBench.roundTrip          0    tassis-vm  thrpt   10  3166652.867  ± 97027.208  ops/s   2112  B/op
  BacklogBench.roundTrip          0  tassis-amqp  thrpt   10     2604.978    ± 134.471  ops/s  37893  B/op
  BacklogBench.roundTrip          0  qpid-tassis  thrpt   10     4437.045    ± 169.735  ops/s  22727  B/op
  BacklogBench.roundTrip       1000    tassis-vm  thrpt   10   188087.508   ± 4555.549  ops/s   2224  B/op
  BacklogBench.roundTrip       1000  tassis-amqp  thrpt   10     2419.389    ± 138.154  ops/s  38781  B/op
  BacklogBench.roundTrip       1000  qpid-tassis  thrpt   10     4083.736    ± 222.315  ops/s  23400  B/op
  BacklogBench.roundTrip      10000    tassis-vm  thrpt   10    10808.972    ± 220.862  ops/s   2259  B/op
  BacklogBench.roundTrip      10000  tassis-amqp  thrpt   10     1976.274    ± 153.178  ops/s  38795  B/op
  BacklogBench.roundTrip      10000  qpid-tassis  thrpt   10     2807.610    ± 108.130  ops/s  23424  B/op

  Benchmark                  (mode)   (provider)   Mode  Cnt       Score        Error  Units  Alloc
  ListenerBench.deliver        auto    tassis-vm  thrpt   10  506339.246  ± 52763.530  ops/s   2160  B/op
  ListenerBench.deliver        auto  tassis-amqp  thrpt   10    5956.432     ± 69.499  ops/s  37766  B/op
  ListenerBench.deliver        auto  qpid-tassis  thrpt   10   36469.995   ± 1641.211  ops/s  22513  B/op
  ListenerBench.deliver     dups-ok    tassis-vm  thrpt   10  494759.597  ± 65504.412  ops/s   2161  B/op
  ListenerBench.deliver     dups-ok  tassis-amqp  thrpt   10    5929.206    ± 156.336  ops/s  37717  B/op
  ListenerBench.deliver     dups-ok  qpid-tassis  thrpt   10   37403.247   ± 1385.745  ops/s  22709  B/op
  ListenerBench.deliver  transacted    tassis-vm  thrpt   10  441015.599  ± 42571.757  ops/s   2356  B/op
  ListenerBench.deliver  transacted  tassis-amqp  thrpt   10    5956.092    ± 127.245  ops/s  37749  B/op
  ListenerBench.deliver  transacted  qpid-tassis  thrpt   10    4818.702    ± 103.582  ops/s  30951  B/op

  Benchmark                          (provider)   Mode  Cnt        Score        Error  Units  Alloc
  SessionBench.sessionPerRoundTrip    tassis-vm  thrpt   10  1949834.948  ± 63802.247  ops/s   2704  B/op
  SessionBench.sessionPerRoundTrip  tassis-amqp  thrpt   10     1323.058     ± 54.706  ops/s  51779  B/op
  SessionBench.sessionPerRoundTrip  qpid-tassis  thrpt   10      706.336     ± 56.612  ops/s  71619  B/op
  ```
- **Comparison with the previous run** (BENCH-20261012-03), in round trips a second:

  | Benchmark | before | after | |
  |---|---|---|---|
  | `IdleWaitersBench`, in the JVM, nobody waiting | 3 282 000 | 3 275 000 | within the error |
  | `IdleWaitersBench`, in the JVM, 100 waiting | 218 000 | 3 185 000 | **14.6× faster** |
  | `IdleWaitersBench`, in the JVM, 1 000 waiting | 18 200 | 3 188 000 | **175× faster** |
  | `RoundTripBench`, in the JVM, 100 bytes | 3 294 000 | 3 332 000 | within the error |
  | `ContentionBench`, in the JVM, 16 threads, own queues | 1 463 000 | 1 499 000 | within the error |
  | `ContentionBench`, in the JVM, 16 threads, shared queue | 1 242 000 | 1 239 000 | within the error |
  | `RoundTripBench`, Qpid JMS → Tassis server | 4 748 | 4 727 | within the error |
  | `ListenerBench`, Qpid JMS → Tassis server, automatic | 37 887 | 36 470 | within the error |

  Consumers that wait on other queues no longer cost anything: the throughput is the same
  with a thousand of them as with none. Nothing else moved, in the JVM or behind the server,
  `ContentionBench` included: several threads on one lock were not held back by the wake-ups,
  but by the lock.

  One line reads lower than before and is not: `ListenerBench`, in the JVM, transacted, 441 000
  messages a second for 518 000. Run again alone, one version after the other, twice
  (`ListenerBench -p provider=tassis-vm -p mode=transacted`): 477 000 ± 120 000 and
  511 000 ± 63 000 before, 482 000 ± 17 000 and 520 000 ± 69 000 after. That benchmark is
  noisy on this machine, and the two versions do the same.

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
