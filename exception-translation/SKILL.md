---
name: exception-translation
description: >
  Apply NXOP FlightOps exception translation rules when implementing or testing Kafka
  or Amazon MQ listeners and message handlers that translate downstream REST, Kafka,
  or MQ exceptions. Covers all consumer-to-target combinations, acknowledgement,
  retry, session recovery, exception packages, and layer responsibilities.
---

# Exception Translation Reference — Handler & Listener Catch Blocks

## Applies To

Apply this skill to the exact file scope:

```text
src/main/java/**/handler/**/*.java
src/main/java/**/listener/**/*.java
src/test/java/**/handler/**/*.java
src/test/java/**/listener/**/*.java
```

This document defines the exception catch blocks required in the **Handler** and **Listener**
layers for each consumer → target combination in NXOP FlightOps services.

---

## Quick Reference Table

| Consumer | Target | Handler Needs Translate? | Handler Catch Blocks | Listener Catch Blocks |
|----------|--------|--------------------------|----------------------|-----------------------|
| Kafka | REST | Yes | `DownstreamApiException` → `EventProcessingException`<br>`DownstreamTransientException` → `TransientException` | `EventProcessingException` → ack + skip<br>`TransientException` → rethrow (no ack) |
| Kafka | Kafka | No | None — exceptions already `EventProcessingException` / `TransientException` | `EventProcessingException` → ack + skip<br>`TransientException` → rethrow (no ack) |
| Kafka | AMQ | Yes | `MessageProcessingException` → `EventProcessingException`<br>`TransientMessageException` → `TransientException` | `EventProcessingException` → ack + skip<br>`TransientException` → rethrow (no ack) |
| AMQ | REST | Yes | `DownstreamApiException` → `MessageProcessingException`<br>`DownstreamTransientException` → `TransientMessageException` | `MessageProcessingException` → ack + skip<br>`TransientMessageException` → `session.recover()` |
| AMQ | Kafka | Yes | `EventProcessingException` → `MessageProcessingException`<br>`TransientException` → `TransientMessageException` | `MessageProcessingException` → ack + skip<br>`TransientMessageException` → `session.recover()` |

---

## Detailed Breakdown

### 1. Kafka Consumer → REST Target

**Handler:**
```java
try {
  client.postFlightData(dto);
} catch (DownstreamApiException e) {
  log.error("Non-retryable client error: {}", e.getMessage(), e);
  throw new EventProcessingException(e.getMessage(), e);
} catch (DownstreamTransientException e) {
  log.error("Retryable downstream error: {}", e.getMessage(), e);
  throw new TransientException(e.getMessage(), e);
}
```

**Listener:**
```java
try {
  handler.handleMessage(record.value());
  ack.acknowledge();
} catch (EventProcessingException e) {
  log.error("Non-retryable — acknowledging to skip: {}", e.getMessage(), e);
  ack.acknowledge();
} catch (TransientException e) {
  log.error("Retryable — rethrowing for container retry: {}", e.getMessage(), e);
  throw e;
}
```

---

### 2. Kafka Consumer → Kafka Target

**Handler:**
```java
// No catch blocks needed — AsyncPublishGateway.awaitAll() already throws
// EventProcessingException (non-retryable) or TransientException (retryable)
publisherService.publish(dto);
```

**Listener:**
```java
try {
  handler.handleMessage(record.value());
  ack.acknowledge();
} catch (EventProcessingException e) {
  log.error("Non-retryable — acknowledging to skip: {}", e.getMessage(), e);
  ack.acknowledge();
} catch (TransientException e) {
  log.error("Retryable — rethrowing for container retry: {}", e.getMessage(), e);
  throw e;
}
```

---

### 3. Kafka Consumer → AMQ Target

**Handler:**
```java
try {
  mqPublisher.publish(dto);
} catch (MessageProcessingException e) {
  log.error("Non-retryable MQ error: {}", e.getMessage(), e);
  throw new EventProcessingException(e.getMessage(), e);
} catch (TransientMessageException e) {
  log.error("Retryable MQ error: {}", e.getMessage(), e);
  throw new TransientException(e.getMessage(), e);
}
```

**Listener:**
```java
try {
  handler.handleMessage(record.value());
  ack.acknowledge();
} catch (EventProcessingException e) {
  log.error("Non-retryable — acknowledging to skip: {}", e.getMessage(), e);
  ack.acknowledge();
} catch (TransientException e) {
  log.error("Retryable — rethrowing for container retry: {}", e.getMessage(), e);
  throw e;
}
```

---

### 4. AMQ Consumer → REST Target

**Handler:**
```java
try {
  client.postFlightData(dto);
} catch (DownstreamApiException e) {
  log.error("Non-retryable client error: {}", e.getMessage(), e);
  throw new MessageProcessingException(e.getMessage(), e);
} catch (DownstreamTransientException e) {
  log.error("Retryable downstream error: {}", e.getMessage(), e);
  throw new TransientMessageException(e.getMessage(), e);
}
```

**Listener:**
```java
try {
  handler.handleMessage(message.getBody());
  message.acknowledge();
} catch (MessageProcessingException e) {
  log.error("Non-retryable — acknowledging to skip: {}", e.getMessage(), e);
  message.acknowledge();
} catch (TransientMessageException e) {
  log.error("Retryable — triggering session recovery: {}", e.getMessage(), e);
  throw e; // ActiveMQ redelivers via session.recover()
}
```

---

### 5. AMQ Consumer → Kafka Target

**Handler:**
```java
try {
  publisherService.publish(dto);
} catch (EventProcessingException e) {
  log.error("Non-retryable Kafka publish error: {}", e.getMessage(), e);
  throw new MessageProcessingException(e.getMessage(), e);
} catch (TransientException e) {
  log.error("Retryable Kafka publish error: {}", e.getMessage(), e);
  throw new TransientMessageException(e.getMessage(), e);
}
```

**Listener:**
```java
try {
  handler.handleMessage(message.getBody());
  message.acknowledge();
} catch (MessageProcessingException e) {
  log.error("Non-retryable — acknowledging to skip: {}", e.getMessage(), e);
  message.acknowledge();
} catch (TransientMessageException e) {
  log.error("Retryable — triggering session recovery: {}", e.getMessage(), e);
  throw e; // ActiveMQ redelivers via session.recover()
}
```

---

## Key Principles

1. **Handler is the translator** — converts target-side exceptions into consumer-side exceptions
2. **Listener never changes** — always catches the same two consumer-side exception types
3. **No silent loss** — every exception is logged before being rethrown or acknowledged
4. **Non-retryable = ack + skip** — prevents poison-pill infinite loops
5. **Retryable = no ack / session.recover()** — message redelivered by broker
6. **Common libraries remain independent** — `common-rest`, `common-kafka`, `common-mq` never depend on each other

---

## Exception Class Locations

| Exception | Package | Library |
|-----------|---------|---------|
| `EventProcessingException` | `com.aa.nxop.flightops.common.kafka.exception` | nxop-flightops-common-kafka |
| `TransientException` | `com.aa.nxop.flightops.common.kafka.exception` | nxop-flightops-common-kafka |
| `DownstreamApiException` | `com.aa.nxop.flightops.common.rest.exception` | nxop-flightops-common-rest |
| `DownstreamTransientException` | `com.aa.nxop.flightops.common.rest.exception` | nxop-flightops-common-rest |
| `MessageProcessingException` | `com.aa.nxop.flightops.common.mq.exception` | nxop-flightops-common-mq |
| `TransientMessageException` | `com.aa.nxop.flightops.common.mq.exception` | nxop-flightops-common-mq |
