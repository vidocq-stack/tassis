<p align="center">
  <img src="tassis-logo.png" alt="Tassis" width="300">
</p>

<h1 align="center">Tassis</h1>

<p align="center">
  <strong>A Jakarta Messaging 3.1 provider with a broker of its own</strong><br>
  <a href="https://jakarta.ee/specifications/messaging/3.1/">Jakarta Messaging 3.1</a> | JDK 25 | Java Modules | Virtual Threads | Zero third-party dependency
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Messaging_TCK-901%2F903-yellowgreen" alt="TCK">
  <img src="https://img.shields.io/badge/status-in_development-orange" alt="Status">
  <img src="https://img.shields.io/badge/JDK-25-orange" alt="JDK">
  <img src="https://img.shields.io/badge/Maven-3.9.16-purple" alt="Maven">
  <img src="https://img.shields.io/badge/license-EPL--2.0%20OR%20EUPL--1.2%20OR%20GPL--2.0--or--later-blue.svg" alt="License">
</p>

---

## What is Tassis?

Tassis is an implementation of [Jakarta Messaging 3.1](https://jakarta.ee/specifications/messaging/3.1/)
(the specification formerly known as JMS) for the [Vidocq](https://codefloe.com/Vidocq/vidocq)
ecosystem. It is a **provider**, not a client for another product: queues, topics, subscriptions,
acknowledgement, local transactions, selectors and redelivery are implemented here, in a broker
that today runs inside the JVM of the application.

### Why the name Tassis?

Every brick of Vidocq carries the name of someone whose work resembles its own. For messaging,
that is the **Tassis** family.

In 1490 Maximilian I of Habsburg entrusted Francesco de Tassis (Franz von Taxis), from a family
of couriers of Cornello near Bergamo, with a line of mounted couriers between Innsbruck and his
court in the Low Countries. In 1505 a contract with Philip the Handsome turned it into a network:
fixed routes from Brussels to Innsbruck, to the French court and to Spain, and delivery times
written into the contract (five and a half days from Brussels to Innsbruck in summer, six and a
half in winter). The family, known from 1650 as Thurn und Taxis, ran the post of the Holy Roman
Empire and then its own until 1867.

What they invented is what a messaging provider does:

| The Tassis post | Jakarta Messaging |
|---|---|
| The sender hands a letter to the post and goes back to work. It neither travels nor waits for the addressee to be home. | A producer sends a message and carries on. The consumer may be busy, or absent. |
| The letter goes from **relay** to relay. Each relay keeps it until a fresh rider takes it over. | The broker stores the message until a consumer takes it: store and forward. |
| A letter has one addressee, on a known route. | A **queue**: each message goes to one consumer. |
| A slip travels with the bag, and each relay writes on it the hour the bag went through. | A message carries its **headers**: when it was sent, when it expires, whether it is being delivered again. |
| Delivery times are part of the contract. | Delivery mode, priority, expiration and delivery delay are part of the message. |
| The post horn announces the courier before it arrives. | A **message listener** is called when a message arrives. |

The post horn of the logo is the emblem of that post, and is still the one of many European
postal services. The dotted circle around it is the road and its relays.

The spelling is the one the family used in the Low Countries and in Spain, *de Tassis*, between
the Italian *Tasso* and the German *Taxis*.

### Philosophy

- **Zero third-party dependency**: `tassis-core` compiles against `jakarta.jms-api` and nothing
  else. No CDI, no JNDI, no library; the message selector parser is written by hand.
- **A broker of its own**, in the JVM and entirely in memory: it writes nothing to the file
  system, by design. It can be opened to the network with AMQP 1.0, a standard protocol:
  the clients need nothing of Tassis.
- **Java Modules-first**: every artifact is an explicit Java module. `io.vidocq.tassis.core`
  exports one package, which holds the three classes an application names, and opens none.
- **Virtual threads**: the threads Tassis creates, for message listeners and completion
  listeners, are virtual. No `synchronized` around a blocking call, no `ThreadLocal`.
- **Tests first, the TCK as the contract**: each test cites the section of the specification it
  covers, and the official TCK runs from the reactor.

## Status

🚧 **In development, not released.** No artifact is published. Messages live in memory and do not survive the JVM: that
one is a choice, not a gap. [ROADMAP.md](ROADMAP.md) has
the milestones, and the limits a user must know are in the
[reference](docs/en/modules/ROOT/pages/reference.adoc).

## Quick Start

### Prerequisites

- JDK 25 (Temurin)
- Maven 3.9.16
- `io.vidocq:vidocq-parent:0.4.0-SNAPSHOT` in the local Maven repository

```bash
# With SDKMAN!
sdk env
./mvnw -ntp clean install
```

An application then declares one dependency, `io.vidocq.tassis:tassis-core`, and one line in its
module descriptor:

```java
module com.acme.orders {
    requires io.vidocq.tassis.core;
}
```

### Send and receive (simplified API)

```java
ConnectionFactory factory = new TassisConnectionFactory();

try (JMSContext context = factory.createContext()) {
    Queue queue = context.createQueue("orders");

    context.createProducer().send(queue, "hello");

    String body = context.createConsumer(queue).receiveBody(String.class, 5_000);
}
```

The connection factory and the destinations are created with `new`, not looked up in JNDI; the
rest is the standard `jakarta.jms` API. A queue or a topic exists from its first use.

### The same with the classic API

```java
ConnectionFactory factory = new TassisConnectionFactory();

try (Connection connection = factory.createConnection()) {
    Session session = connection.createSession();
    Queue queue = session.createQueue("orders");
    MessageProducer producer = session.createProducer(queue);
    MessageConsumer consumer = session.createConsumer(queue);
    connection.start();

    producer.send(session.createTextMessage("hello"));

    TextMessage received = (TextMessage) consumer.receive(5_000);
}
```

### Publish and subscribe

```java
try (JMSContext context = factory.createContext()) {
    Topic prices = context.createTopic("prices");
    JMSConsumer subscriber = context.createConsumer(prices, "currency = 'EUR'");

    context.createProducer().setProperty("currency", "EUR").send(prices, "1.08");

    String price = subscriber.receiveBody(String.class, 5_000);
}
```

### Injection with CDI

On [Vauban](https://codefloe.com/Vidocq/vauban), a bean has its contexts injected instead of
creating them. Add `io.vidocq.tassis:tassis-cdi-vauban` and
`requires io.vidocq.tassis.cdi.vauban;`, and give the container the beans of `TassisCdi.BEANS`:

```java
@Dependent
public class OrderDesk {

    @Inject
    JMSContext orders;

    @Inject
    @JMSConnectionFactory("audit")
    JMSContext audit;

    public void place(String order) {
        orders.createProducer().send(orders.createQueue("orders"), order);
        audit.createProducer().send(audit.createQueue("trail"), "placed " + order);
    }
}
```

An injected context lives for the request in progress: created on first use, shared by the
injection points that carry the same annotations, closed when the request ends. Its connection
factory is a `ConnectionFactory` bean of the application, or else the settings below read from
the system properties.

The runnable versions of the first three examples are in
[tassis-examples](tassis-examples/src/main/java/io/vidocq/tassis/examples); the build compiles
and runs them.

### Configuration

A connection factory is configured in code, from settings, or from a connection URL:

```java
// in code
ConnectionFactory inCode = new TassisConnectionFactory("orders").withClientID("reader");

// from settings: only the keys under "tassis." are read, so this works with -Dtassis.broker=orders
ConnectionFactory fromSettings = TassisConnectionFactory.from(System.getProperties());

// from a connection URL
ConnectionFactory fromUrl = TassisConnectionFactory.fromUrl("tassis:vm:orders?clientId=reader");
```

| Key | In a URL | Default | Meaning |
|---|---|---|---|
| `tassis.url` | | | A connection URL holding the settings below; a setting given by its own key overrides it |
| `tassis.broker` | `tassis:vm:<broker>` | `default` | The name of the in-VM broker |
| `tassis.server` | `tassis:amqp://<host>[:<port>]` | none | A broker of the network, as `amqp://<host>[:<port>]` (port 5672 by default) or, over TLS, `amqps://<host>[:<port>]` (port 5671), instead of the in-VM one; needs `tassis-amqp` |
| `tassis.clientId` | `clientId` | none | The client identifier of the connections |
| `tassis.listener.maxDeliveries` | `listener.maxDeliveries` | `10` | Deliveries of a message to a listener that keeps failing before the message is dropped |
| `tassis.pool.enabled` | `pool.enabled` | `true` | Whether the CDI integration pools the connections of this factory to a broker of the network |
| `tassis.pool.maxSize` | `pool.maxSize` | `10` | Connections a pool opens at most |
| `tassis.pool.acquireTimeout` | `pool.acquireTimeout` | `PT5S` | How long a borrower waits when all are lent |
| `tassis.pool.idleTimeout` | `pool.idleTimeout` | `PT10M` | How long a connection nobody uses is kept; `PT0S` for ever |
| `tassis.pool.maxLifetime` | `pool.maxLifetime` | `PT30M` | How long a connection is kept at all; `PT0S` for ever |
| `tassis.pool.leakThreshold` | `pool.leakThreshold` | `PT0S` | How long a connection may be lent before it is reported in the log; `PT0S` for never |

The `tassis.pool.*` settings are those a `TassisConnectionPool` built over the factory starts
from; a factory alone makes no use of them. Durations are written as ISO-8601 (`PT30S`, `PT10M`).

Several factories can be defined by name, `tassis.factory.<name>.<key>`, and obtained with
`TassisConnectionFactory.named(name, settings)`; a named factory inherits the settings that carry
no name. An unknown key or an invalid value is an error that names the key.

### Authentication

A broker accepts anyone until a user is declared on it. From then on it is secured: a connection
needs the name and the password of a declared user.

```java
TassisBroker.named("orders").addUser("billing", password);

ConnectionFactory factory = new TassisConnectionFactory("orders");
try (JMSContext context = factory.createContext("billing", password)) {
    // ...
}
factory.createContext("billing", "wrong");   // JMSSecurityRuntimeException
factory.createContext();                      // refused too, unless allowAnonymous(true)
```

Users live in memory and every user has the same rights: see
[Authentication](docs/en/modules/ROOT/pages/usage.adoc) in the documentation.

### Opening the broker to the network

`TassisServer`, in `tassis-server`, opens a broker to other processes over AMQP 1.0:

```java
try (TassisServer server = TassisServer.forBroker("orders").port(5672).start()) {
    // any AMQP 1.0 client reaches the broker "orders" at amqp://this-host:5672
}
```

A Tassis client in another process reaches it by changing its URL, with `tassis-amqp` among its
dependencies:

```java
ConnectionFactory factory = TassisConnectionFactory.fromUrl("tassis:amqp://broker-host:5672");
```

A client needs nothing of Tassis, though. With Apache Qpid JMS, for instance:

```java
ConnectionFactory factory = new org.apache.qpid.jms.JmsConnectionFactory("amqp://broker-host:5672");
```

A client of the network has the whole of Jakarta Messaging, short of XA.

**Nothing is encrypted unless asked.** The server listens on the loopback address unless told
otherwise, and encrypts with TLS when it is given a context; clients then say `amqps`:

```java
TassisServer server = TassisServer.forBroker("orders").host("0.0.0.0").tls(sslContext).start();  // port 5671

ConnectionFactory factory = TassisConnectionFactory.fromUrl("tassis:amqps://broker-host");
```

See [Opening a broker to the network](docs/en/modules/ROOT/pages/usage.adoc).

### Pooling the connections

A connection to a broker of the network is a socket, a handshake and an authentication.
`TassisConnectionPool`, in `tassis-pool`, keeps them open: it is a `ConnectionFactory` whose
connections and contexts go back to the pool when the application closes them.

```java
try (TassisConnectionPool pool = TassisConnectionPool.over(factory).maxSize(8).build()) {
    try (JMSContext context = pool.createContext()) {     // borrowed
        context.createProducer().send(context.createQueue("orders"), "hello");
    }                                                     // given back, not closed
}
```

| Setting | Default | Meaning |
|---|---|---|
| `maxSize(int)` | 10 | connections to the broker open at most, lent or not |
| `acquireTimeout(Duration)` | 5 s | how long a borrower waits when all are lent, before a `ResourceAllocationException` |
| `idleTimeout(Duration)` | 10 min | how long a connection nobody uses is kept |
| `maxLifetime(Duration)` | 30 min | how long a connection is kept at all; never closed under a borrower |
| `leakThreshold(Duration)` | never | how long a connection may be lent before it is reported in the log |

The same settings are keys (`tassis.pool.maxSize`) and URL parameters (`pool.maxSize`) of the
factory the pool is built over. Contexts injected with CDI come from a pool when their factory
reaches a broker of the network.

A borrower finds a connection as a factory gives it: stopped, with nothing of the one before.
A connection given a client identifier (`setClientID`) is not kept: closing it really closes
it. See [Pooling the connections](docs/en/modules/ROOT/pages/usage.adoc).

## Architecture

```mermaid
graph TB
    APP[Application] --> CORE
    subgraph "Runtime"
        CORE[tassis-core<br/><i>client API + in-VM broker</i>] --> API[tassis-api<br/><i>jakarta.messaging</i>]
        CDI[tassis-cdi-vauban<br/><i>JMSContext injection</i>] --> CORE
    end
    subgraph "Testing"
        TCK[tassis-tck<br/><i>903/903</i>]
        EX[tassis-examples]
    end
    TCK --> CORE
    EX --> CORE
    CORE --> JDK[JDK 25<br/><i>virtual threads</i>]
    style CORE fill:#e1f5fe,stroke:#0288d1,stroke-width:2px
```

```
tassis/
├── tassis-api            Aggregator: re-exposes jakarta.jms-api, a Java module upstream
├── tassis-core           The provider: administered objects, client API, in-VM broker,
│                         message model, selectors
├── tassis-amqp           AMQP 1.0, written from scratch: the codec and the mapping of messages
├── tassis-server         TassisServer: opens a broker to AMQP 1.0 clients
├── tassis-pool           TassisConnectionPool: a connection factory that lends its connections
├── tassis-cdi-vauban     JMSContext injection (§12.4) for applications on Vauban
├── tassis-cdi-vauban-module-it   Its proof on the module path; never published
├── tassis-examples       The examples of the documentation, compiled and run by the build
├── tassis-bench          JMH benchmarks, beside Artemis and Qpid JMS, behind the `bench`
│                         profile; never published
├── tassis-tck            Official TCK runner, in-reactor behind the `tck` Maven profile
└── tassis-interop        Tests against third-party AMQP implementations, behind the
                          `interop` profile; never published
```

Inside `tassis-core`, only `io.vidocq.tassis.core` is exported (`TassisConnectionFactory`,
`TassisQueue`, `TassisTopic`, and `TassisBroker` to administer a broker). The rest is internal:

| Package | Role |
|---|---|
| `internal.client` | Connections, sessions, producers, consumers, browsers, and the simplified API as a thin layer over them |
| `internal.broker` | The in-VM broker: one storage per queue and per subscription, delivery, redelivery, expiration |
| `internal.message` | `Message` and the five body types, with one conversion table |
| `internal.selector` | The message selector language: parser, static checks, evaluation |
| `internal.settings` | What configures a connection factory: keys, URL, named factories |

## Jakarta Messaging 3.1 Features

| Feature | Status |
|---|---|
| Classic API (`Connection`, `Session`, `MessageProducer`, `MessageConsumer`) | ✅ |
| Simplified API (`JMSContext`, `JMSProducer`, `JMSConsumer`) | ✅ |
| Legacy domain-specific API (`QueueConnection`, `TopicSession`, ...) | ✅ |
| The five message body types, `getBody`, property conversions | ✅ |
| Queues, topics, temporary destinations, `QueueBrowser` | ✅ |
| Subscriptions: unshared and shared, durable or not, `noLocal` | ✅ |
| Acknowledgement modes, local transactions, `recover`, redelivery | ✅ |
| Message selectors | ✅ |
| `MessageListener`, asynchronous send with `CompletionListener` | ✅ |
| Priority, expiration, delivery delay | ✅ |
| Persistence across a restart | Not planned: the broker is in memory by design |
| Authentication (user registry per broker, `TassisBroker`) | ✅ |
| A broker open to AMQP 1.0 clients: queues, topics and subscriptions, selectors, temporary destinations, local transactions, browsers | ✅ |
| The Tassis client over AMQP (`tassis:amqp://host`), with heartbeats and the `ExceptionListener` for a lost connection | ✅ |
| Over AMQP: TLS (`amqps`), for the server and for the Tassis client | ✅ |
| `JMSContext` injection with CDI, request scope (on Vauban) | ✅ |
| `JMSContext` injection in a JTA transaction | M13 |
| XA and JTA | M13 |

## TCK Validation

Tassis runs the official Jakarta Messaging TCK (`jakarta.tck:jms-tck:11.0.3`) from its reactor.

```
Functional tests :  903/903 of the applicable set (core 642/642, core20 261/261)
Signature test   :  passed
```

The `ee` tests need EJB or a web container and do not apply to a Java SE provider.
**Compatibility is not claimed**, which would take a certification request:
[TCK.md](TCK.md) is the working record.

```bash
./run-official-tck-messaging-3.1.sh             # the functional tests, about 13 minutes
./run-official-tck-messaging-3.1.sh signature   # the signature test of the API
```

## Documentation

| Document | Contents |
|----------|---------|
| [docs/en](docs/en/modules/ROOT/pages) | The Antora component: getting started, usage, concepts, internals, TCK status, reference, migration |
| [ROADMAP.md](ROADMAP.md) | Design decisions and milestones |
| [TCK.md](TCK.md) | TCK runner, score, deviations |
| [BUG.md](BUG.md), [BENCH.md](BENCH.md) | Bug and benchmark records |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to contribute |

## License

[EPL-2.0 OR EUPL-1.2 OR GPL-2.0-or-later](LICENSE), at your option. See also [NOTICE](NOTICE).
