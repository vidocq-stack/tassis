# BUG.md — Tassis

Reproducible bugs (internal issue, regression, incorrect behavior not yet fixed).
Entry format: `short id · date · symptom · minimal repro · cause hypothesis · status`.

## T-001 · 2026-10-10 · closing a session did not wait for a listener that had removed itself

- **Symptom**: `Session.close()` (and `Connection.close()`) returned while an `onMessage` was
  still running, and gave the message that listener was processing back to the broker: with
  automatic acknowledgement it was delivered a second time, to another consumer, while the
  first delivery was still being processed.
- **Minimal repro**: a listener calls `consumer.setMessageListener(null)` on its own consumer,
  then keeps working; another thread closes the session.
  `AsynchronousDeliveryTest.closingTheSessionWaitsForAListenerThatRemovedItself`.
- **Cause**: a consumer waits for its dispatcher thread when it is closed, but a listener that
  removes itself cannot be made to wait for its own thread, so the consumer forgets the
  dispatcher at once. Nothing else waited for the listener in progress. Present since M6;
  found by the concurrency review of M11.2 (§6.2.15).
- **Status**: **fixed** in M11.2: `TassisSession.shut()` waits for the listener in progress of
  the session before giving back what the session holds.
- **Still open**: `MessageConsumer.close()` called from another thread in the same situation
  returns without waiting for that listener (§8.8). The message is not given back, since the
  session still answers for it, so nothing is lost or duplicated; only the wait is missing.


## T-002 · 2026-10-12 · the server answered a client tens of milliseconds late

- **Symptom**: against a `TassisServer`, each exchange of a third-party client took some 48 ms:
  Apache Qpid JMS sent and received 21 messages a second,
  and the Tassis client took 51 ms to open a connection. Nothing failed, so no test saw it.
- **Minimal repro**: `java -jar tassis-bench/target/benchmarks.jar RoundTripBench -p
  provider=qpid-tassis -p size=100`, and `PoolBench -p broker=network -p source=direct`
  (`BENCH.md`, BENCH-20261012-01).
- **Cause**: the server left the algorithm of Nagle on the sockets it accepts
  (`TassisServer.accept`), where the Tassis client turns it off on its own
  (`AmqpBrokerConnection.establish`). A server answers with small frames, one after the other;
  the second waited for the client to acknowledge the first, which a client delays by up to
  40 ms. Present since M11.3b; found by the first benchmarks of M12.
- **Status**: **fixed** in M12-1: `TassisServer.prepared` sets `TCP_NODELAY` on every accepted
  socket (`TassisServerTest.theSocketOfAClientSendsItsFramesWithoutWaiting`); the figures after
  the fix are BENCH-20261012-02.
