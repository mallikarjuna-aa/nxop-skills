---
name: amazon-mq-config-pattern
description: >
  Maintain the NXOP FlightOps Amazon MQ two-class configuration pattern for Spring Boot
  3 and 4. Covers ActiveMqConfig property binding, ActiveMqInitializerConfig wiring,
  CLIENT_ACKNOWLEDGE, native versus pooled connections, acknowledgement recovery,
  redelivery policy, and broker-side individual DLQ configuration. Use when modifying
  ActiveMqConfig or ActiveMqInitializerConfig classes in nxop-flightops-common-mq.
---

# Amazon MQ Consumer Config — Two-Class Pattern (Spring Boot 3+/4)

## Applies To

Apply this skill to the exact file scope:

```text
**/config/**ActiveMqConfig*.java,**/config/**ActiveMqInitializerConfig*.java
```

> **Library-provided.** The `ActiveMqConfig` and `ActiveMqInitializerConfig` classes are
> provided by `nxop-flightops-common-mq`. Downstream services must **not** create these
> classes — they are auto-configured when the library is on the classpath.
>
> This skill exists to enforce the two-class pattern rules if you ever
> modify the library itself or if Copilot encounters these classes during editing.

## Rule: Never combine `@ConfigurationProperties` with Spring-injected dependencies

**Do NOT do this:**
```java
@ConfigurationProperties(prefix = "activemq")
public class ActiveMqConfig {
  private final SomeSpringBean bean; // BROKEN in Spring Boot 3+/4

  public ActiveMqConfig(SomeSpringBean bean) { // constructor-binding mode!
    this.bean = bean;
  }
}
```

**Why it breaks:** In Spring Boot 3+, when a `@ConfigurationProperties` class has exactly
one non-default constructor, the binder enters **constructor-binding mode**. Every
constructor parameter is resolved from YAML — not from the Spring context. The Spring
bean resolves as `null`.

## Required Pattern: Two Classes (implemented in `nxop-flightops-common-mq`)

### Class 1 — Pure POJO (`ActiveMqConfig.java`)
- Located at `com.aa.nxop.flightops.common.mq.activemq.config.ActiveMqConfig`
- Annotated `@ConfigurationProperties` only
- No Spring bean dependencies
- No constructor other than the default (uses Lombok `@Getter`/`@Setter`)
- Nested static classes for grouped properties (ConnectionPool, Listener, Queue, Redelivery)

### Class 2 — Wiring class (`ActiveMqInitializerConfig.java`)
- Located at `com.aa.nxop.flightops.common.mq.activemq.config.ActiveMqInitializerConfig`
- Annotated `@Configuration`, `@EnableJms`, `@EnableConfigurationProperties`
- Receives `ActiveMqConfig` via normal DI (`@RequiredArgsConstructor`)
- Declares all JMS infrastructure beans: `nativeConnectionFactory`,
  `pooledConnectionFactory`, `jmsListenerContainerFactory`, `messageConverter`

## application.yml must include ALL bound fields
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

## RedeliveryPolicy is mandatory

Configured in the library's `ActiveMqInitializerConfig.nativeConnectionFactory()`.
Without it, ActiveMQ uses the default policy (6 retries × 1 s flat delay) which is
too aggressive and not configurable via YAML.

The `ActiveMqConfig` POJO includes a `Redelivery` nested class with sensible defaults:
- `maximumRedeliveries = 3`
- `initialRedeliveryDelay = 1000`
- `useExponentialBackOff = true`
- `backOffMultiplier = 2.0`
- `maximumRedeliveryDelay = 30000`

## CLIENT_ACKNOWLEDGE is mandatory
Set in the library's `jmsListenerContainerFactory` bean.
Never use `SESSION_TRANSACTED` together with `CLIENT_ACKNOWLEDGE`.
Never call `message.acknowledge()` before the handler completes successfully.

## Listener factory uses the raw native connection — not the pool

The `jmsListenerContainerFactory` receives `ActiveMQConnectionFactory nativeConnectionFactory`
directly. The `DefaultJmsListenerContainerFactory` manages its own long-lived session per
consumer thread; connection pooling provides no benefit for listeners and introduces risk:

- **JMS spec foot-gun:** `message.acknowledge()` acknowledges **all prior unacknowledged
  messages on the same session**. If `safeAcknowledge(A)` fails silently and the session
  continues to message B, calling `acknowledge(B)` silently covers A — causing silent
  message loss with no error in logs.
- **Pool isolation:** Pooled sessions may be shared or returned in unexpected states.
  Listeners must own their session exclusively to ensure `session.recover()` redelivers
  exactly the failed message.

The `pooledConnectionFactory` (marked `@Primary`) exists solely for `JmsTemplate` outbound
sends, where connection/session reuse provides measurable throughput gains.

## safeAcknowledge must recover on failure

`safeAcknowledge` must return `boolean`. If the ack fails, the caller **must** call
`session.recover()` immediately to force the broker to redeliver the message. Never
leave a session in a dirty state after a failed acknowledge — the next successful
`acknowledge()` on that session would silently cover the failed message.

## Session injection and recovery

The `@JmsListener` method must accept `Session session` as a second parameter.
For transient or unexpected errors, call `session.recover()` to trigger broker
redelivery per the configured `RedeliveryPolicy`. For non-retryable errors
(`MessageProcessingException` from `com.aa.nxop.flightops.common.mq.exception`),
acknowledge the message to prevent infinite loops.

## RedeliveryPolicy ≠ Broker DLQ Guarantee

**`RedeliveryPolicy` is client-side only.** It controls how many times the client
re-receives a message after `session.recover()`. After `maximumRedeliveries` is
exhausted, the broker moves the message to a Dead-Letter Queue **only if the broker's
`deadLetterStrategy` is configured on the destination**.

### What happens without broker-side DLQ configuration

- AmazonMQ's default is a **shared** `ActiveMQ.DLQ` queue for *all* destinations.
- All poisoned messages from every queue land in one bucket — impossible to triage,
  alert on, or replay per-service.
- If someone disables or removes the default policy, exhausted messages are **silently
  discarded** — no DLQ, no alert, permanent message loss.

### Infrastructure requirement (must be provisioned by platform team)

The broker must have an **individualDeadLetterStrategy** configured for each
service queue. This is a broker-level XML configuration, not application code.

**Terraform (AmazonMQ):**
```hcl
resource "aws_mq_configuration" "activemq" {
  name           = "${var.service_name}-broker-config"
  engine_type    = "ActiveMQ"
  engine_version = "5.18"

  data = <<XML
<?xml version="1.0" encoding="UTF-8"?>
<broker xmlns="http://activemq.apache.org/schema/core">
  <destinationPolicy>
    <policyMap>
      <policyEntries>
        <policyEntry queue=">">
          <deadLetterStrategy>
            <individualDeadLetterStrategy
              queuePrefix="DLQ."
              useQueueForQueueMessages="true"
              processExpired="false" />
          </deadLetterStrategy>
        </policyEntry>
      </policyEntries>
    </policyMap>
  </destinationPolicy>
</broker>
XML
}
```

**Effect:** Messages exhausting redelivery on `flight-data-queue` go to
`DLQ.flight-data-queue` — per-destination, monitorable, replayable.

### Application-side responsibility

- The application (`RedeliveryPolicy`) controls *how many* retries and *how fast*.
- The broker (`deadLetterStrategy`) controls *where* the message goes after exhaustion.
- These are **orthogonal** — both must be configured. If only `RedeliveryPolicy` is set
  and the broker has no individual DLQ strategy, you get a shared catch-all `ActiveMQ.DLQ`
  at best, or silent message loss at worst.
