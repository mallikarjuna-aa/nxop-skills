# Amazon MQ Config — Provided by `nxop-flightops-common-mq`

> **Library-provided.** The two-class configuration pattern (`ActiveMqConfig` and
> `ActiveMqInitializerConfig`) is implemented in `nxop-flightops-common-mq`.
> Downstream services must **not** create these classes — they are auto-configured
> when the library is on the classpath.

## What the library provides

### Class 1 — `ActiveMqConfig.java` (pure property POJO)

Located at `com.aa.nxop.flightops.common.mq.activemq.config.ActiveMqConfig`.

- `@ConfigurationProperties(prefix = "activemq")` — binds all `activemq.*` YAML properties
- No constructor, no Spring dependencies, no `@PostConstruct`
- Nested static classes: `ConnectionPool`, `Listener`, `Queue`, `Redelivery`
- Sensible defaults: maxConnections=10, concurrency="1-5", maxRedeliveries=3,
  exponential backoff enabled (multiplier=2.0, maxDelay=30s)

### Class 2 — `ActiveMqInitializerConfig.java` (wiring class)

Located at `com.aa.nxop.flightops.common.mq.activemq.config.ActiveMqInitializerConfig`.

- `@Configuration` + `@EnableJms` + `@EnableConfigurationProperties(ActiveMqConfig.class)`
- Declares all JMS infrastructure beans:
  - `nativeConnectionFactory` — `ActiveMQConnectionFactory` with `RedeliveryPolicy`,
    trusted packages restricted to `com.aa.nxop`, `trustAllPackages(false)`
  - `pooledConnectionFactory` — `JmsPoolConnectionFactory` wrapping the native factory,
    marked `@Primary` to prevent `JmsAutoConfiguration` ambiguity
  - `jmsListenerContainerFactory` — `DefaultJmsListenerContainerFactory` configured with
    `CLIENT_ACKNOWLEDGE`, `sessionTransacted(false)`, configurable concurrency
  - `messageConverter` — `MappingJackson2MessageConverter` (JSON, `TextMessage`)

## What downstream services must do

### 1. Add the Maven dependency

```xml
<dependency>
    <groupId>com.aa.nxop.flightops.common.mq</groupId>
    <artifactId>nxop-flightops-common-mq</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. Supply `activemq.*` properties in `application.yml`

```yaml
activemq:
  # Broker URL — use ssl:// for Amazon MQ (TLS is enforced by AWS)
  # Example: ssl://b-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx-1.mq.us-east-1.amazonaws.com:61617
  broker-url: ${ACTIVEMQ_BROKER_URL}
  username: ${ACTIVEMQ_USERNAME}
  password: ${ACTIVEMQ_PASSWORD}
  connection-pool:
    max-connections: 10
    idle-timeout: 30000
  listener:
    # Concurrency format: min-max (e.g. 1-5 means 1 to 5 concurrent consumers)
    concurrency: ${ACTIVEMQ_CONCURRENCY:1-5}
  queue:
    name: ${ACTIVEMQ_QUEUE_NAME}
  redelivery:
    maximum-redeliveries: 3
    initial-redelivery-delay: 1000
    use-exponential-back-off: true
    back-off-multiplier: 2.0
    maximum-redelivery-delay: 30000
```

Every field declared in `ActiveMqConfig` must have a corresponding YAML key or a default.

Required keys under `activemq`:
```
broker-url, username, password,
connection-pool.max-connections, connection-pool.idle-timeout,
listener.concurrency, queue.name,
redelivery.maximum-redeliveries, redelivery.initial-redelivery-delay,
redelivery.use-exponential-back-off, redelivery.back-off-multiplier,
redelivery.maximum-redelivery-delay
```

## Why the two-class pattern exists

In Spring Boot 3+, when a `@ConfigurationProperties` class has exactly one non-default
constructor, the binder enters **constructor-binding mode**. Every constructor parameter
is resolved from YAML — not from the Spring context. Mixing Spring beans into a
`@ConfigurationProperties` class causes them to resolve as `null`.

The library enforces this separation so downstream services cannot accidentally break it.
