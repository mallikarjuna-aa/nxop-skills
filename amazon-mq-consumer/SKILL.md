---
name: amazon-mq-consumer
description: >
  Implement, configure, and test Amazon MQ (ActiveMQ Classic) consumers in NXOP FlightOps
  Spring Boot services using nxop-flightops-common-mq. Covers the JMS listener -> handler
  pattern, MessageProcessingException vs TransientMessageException handling,
  session.recover() retry semantics, and embedded-broker integration tests. Use when
  building or testing an Amazon MQ / ActiveMQ consumer service.
---

# Skill: Spring Boot Amazon MQ (ActiveMQ) Consumer

This skill defines enterprise standards for implementing, configuring, and testing **Amazon MQ
(ActiveMQ Classic) consumers** in NXOP FlightOps Spring Boot services.
All code must comply with `copilot-instructions.md`.

> **Template notice:** All values shown in this skill (queue names, concurrency settings,
> listener class names, service class names, and test payloads) are **illustrative examples
> only**. Replace every value with the ones that are appropriate for your specific service
> before using any snippet.

---

## Common Library: `nxop-flightops-common-mq`

All Amazon MQ infrastructure — configuration properties (`ActiveMqConfig`), JMS factory
beans (`ActiveMqInitializerConfig`), exception hierarchy (`MessageProcessingException`,
`TransientMessageException`), and observability utilities (`MdcMessageTracker`) — is
provided by the **`nxop-flightops-common-mq`** shared library. Downstream services must
**not** duplicate these classes.

### What the library provides (do NOT create in downstream services)

| Class | Package | Purpose |
|---|---|---|
| `ActiveMqConfig` | `com.aa.nxop.flightops.common.mq.activemq.config` | `@ConfigurationProperties` POJO binding `activemq.*` YAML |
| `ActiveMqInitializerConfig` | `com.aa.nxop.flightops.common.mq.activemq.config` | `nativeConnectionFactory`, `pooledConnectionFactory`, `jmsListenerContainerFactory`, `messageConverter` |
| `MessageProcessingException` | `com.aa.nxop.flightops.common.mq.exception` | Non-retryable exception — listener must acknowledge |
| `TransientMessageException` | `com.aa.nxop.flightops.common.mq.exception` | Retryable exception — listener must call `session.recover()` |
| `MdcMessageTracker` | `com.aa.nxop.flightops.common.mq.observability` | MDC correlation-ID + message-ID propagation |
| `FlightOpsMqAutoConfiguration` | `com.aa.nxop.flightops.common.mq` | Auto-configuration entry point via Spring Boot imports |

---

## Maven Dependencies

```xml
<!-- Common MQ library — provides all ActiveMQ config, exceptions, and observability -->
<dependency>
    <groupId>com.aa.nxop.flightops.common.mq</groupId>
    <artifactId>nxop-flightops-common-mq</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Embedded ActiveMQ broker for integration tests only -->
<dependency>
    <groupId>org.apache.activemq</groupId>
    <artifactId>activemq-broker</artifactId>
    <scope>test</scope>
</dependency>
```

> The common-mq library transitively provides `spring-boot-starter-activemq` and `pooled-jms`.
> Remove Kafka-specific dependencies when creating a pure Amazon MQ service.
> The two sets can coexist if the service consumes from both brokers.

---

## Configuration

All JMS infrastructure is auto-configured by `nxop-flightops-common-mq`. Downstream
services only need to supply `activemq.*` properties in their `application.yml`.

See [config-pattern.md](./references/config-pattern.md) for details on how the library's
two-class pattern works and the full `application.yml` checklist.

### `application.yml` checklist

Required keys under `activemq`:
```
broker-url, username, password,
connection-pool.max-connections, connection-pool.idle-timeout,
listener.concurrency, queue.name,
redelivery.maximum-redeliveries, redelivery.initial-redelivery-delay,
redelivery.use-exponential-back-off, redelivery.back-off-multiplier,
redelivery.maximum-redelivery-delay
```

### Key design decisions enforced by the library

- **Two-class config pattern** — `ActiveMqConfig` (pure POJO) + `ActiveMqInitializerConfig`
  (wiring) to prevent Spring Boot 3+/4 constructor-binding issues.
- **`CLIENT_ACKNOWLEDGE` mandatory** — the listener controls when messages are acknowledged.
- **Listener uses raw `nativeConnectionFactory`** — not the pooled factory. The listener
  container manages its own long-lived session per thread; pooling adds no benefit for
  consumers and risks session-level ack leakage.
- **RedeliveryPolicy mandatory** — configured on the native factory with exponential backoff.
- **Pooled factory is `@Primary`** — exists for `JmsTemplate` outbound sends only; prevents
  `JmsAutoConfiguration` bean ambiguity.
- **Trusted packages restricted** to `com.aa.nxop` — never `trustAllPackages(true)`.

### Infrastructure prerequisite: Broker DLQ strategy

> **`RedeliveryPolicy` is client-side only.** After `maximumRedeliveries` is exhausted,
> the broker routes the message to a Dead-Letter Queue **only if** the broker's
> `deadLetterStrategy` is configured. Without it, exhausted messages go to a shared
> `ActiveMQ.DLQ` (all queues mixed together) or may be silently discarded.

The platform/infra team must provision an **`individualDeadLetterStrategy`** on the
AmazonMQ broker so that each queue gets its own DLQ (e.g., `DLQ.flight-data-queue`).
See `amazon-mq-config-pattern.instructions.md` § "RedeliveryPolicy ≠ Broker DLQ Guarantee"
for the Terraform snippet.

---

## Listener Implementation

See [listener-pattern.md](./references/listener-pattern.md) for the complete listener class
code template.

### Rules

- **Always accept `jakarta.jms.Message` and `jakarta.jms.Session`** as listener parameters.
- **Use `MdcMessageTracker.put(message)`** at the start and **`MdcMessageTracker.clear()`**
  in a `finally` block for every listener method.
- **Use `MdcMessageTracker.resolveMessageId(message)`** to obtain a stable message ID for logging.
- **Log the JMS message ID at `INFO` level** on every received message.
- **Delegate all business logic to a `@Service`.** The listener only extracts, logs, and delegates.
- **Place the listener in a dedicated `@Component`** — never inside a `@Service` or `@RestController`.
- **Acknowledgement contract:**
  - Call `message.acknowledge()` **only after** the handler returns successfully.
  - **Non-retryable** (`MessageProcessingException` from common-mq): **acknowledge** + log.
    TODO: implement custom DLQ handling.
  - **Retryable** (`TransientMessageException`, `RuntimeException`):
    do **not** acknowledge. Call `session.recover()` for broker redelivery per `RedeliveryPolicy`.
  - **Unexpected** (`Exception`): do **not** acknowledge. Call `session.recover()`.
  - Handle `JMSException` from ack/recover separately — log if the ack itself fails.
- **Import `MessageProcessingException` from `com.aa.nxop.flightops.common.mq.exception`** —
  do not create a per-service copy.

---

## Observability

### Correlation ID Propagation

Use `MdcMessageTracker` from the common-mq library for end-to-end traceability:

```java
public void onMessage(Message message, Session session) {
  MdcMessageTracker.put(message);
  String messageId = MdcMessageTracker.resolveMessageId(message);
  log.info("Received JMS message [id={}]", messageId);
  try {
    // ... delegate to handler
  } finally {
    MdcMessageTracker.clear();
  }
}
```

Add `%X{correlationId} %X{jmsMessageId}` to `logback.xml` pattern.

---

## Testing

See [test-patterns.md](./references/test-patterns.md) for complete unit test and integration
test code templates.

### Rules

- **Integration tests are mandatory** — embedded ActiveMQ broker at
  `vm://localhost?broker.persistent=false&create=true`. Class names must end with `IT`.
- **Never use `Thread.sleep()`** — use `Awaitility` with at least `Duration.ofSeconds(30)`.
- **Always annotate integration tests** with `@Tag("integration")` and `@DirtiesContext`.
- Use `@MockitoBean` in `@SpringBootTest` slices; use `@Mock` in plain unit tests.
- Use AssertJ `assertThatThrownBy()` for exception testing.
- Verify **both** happy-path acknowledgement and failure cases (session.recover, no ack).
- Do NOT add `when(...)` stubs in `@BeforeEach` unless **every** test consumes them.

---

## What Copilot Should NOT Do

- Do not create `ActiveMqConfig`, `ActiveMqInitializerConfig`, or `MessageProcessingException`
  in downstream services — they are provided by `nxop-flightops-common-mq`.
- Do not use `@Value` for Amazon MQ configuration — the library uses `@ConfigurationProperties`.
- Do not place `@JmsListener` inside a `@Service` or `@RestController`.
- Do not call `message.acknowledge()` before the handler completes successfully.
- Do not call `message.acknowledge()` for retryable or unexpected exceptions — call
  `session.recover()` instead.
- Do not catch `MessageProcessingException` without acknowledging — non-retryable errors must
  always be acknowledged to prevent infinite redelivery loops.
- Do not swallow exceptions silently — always log at `log.error` level with the full exception.
- Do not omit the `Session` parameter from `onMessage()`.
- Do not use `log.debug(...)` for standard `@JmsListener` processing logs — use `log.info(...)`.
- Do not use `log.warn(...)` for listener exceptions — use `log.error(...)`.
- Do not use `@Autowired` field injection — use constructor injection via `@RequiredArgsConstructor`.
- Do not use wildcard imports.
- Do not use `System.out.println` — use the `log` field from `@Slf4j`.
- Do not use `Thread.sleep()` in tests — use `Awaitility`.
- Do not use `times(1)` in `verify()` — the default is once.
- Do not use `trustAllPackages(true)` on `ActiveMQConnectionFactory`.
- Do not place `when(...)` stubs in `@BeforeEach` unless every test consumes them —
  Mockito `STRICT_STUBS` will flag them as `UnnecessaryStubbing`.
- Do not use `jmsTemplate.convertAndSend(dest, payload)` in IT tests — use
  `jmsTemplate.send(dest, session -> session.createTextMessage(payload))`.
- Do not use `@MockBean` — removed in Spring Boot 4.x; use `@MockitoBean`.
- Do not manually implement MDC propagation — use `MdcMessageTracker` from common-mq.
