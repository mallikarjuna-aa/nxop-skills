# Combination Services — Cross-Cutting Exception & Timeout Guidance

Rules for services that combine multiple integration types (e.g., Kafka consumer → REST client,
AMQ consumer → Kafka producer). Use when a service has two or more integration patterns.

---

## Common Service Combinations

| Pattern | Consumer | Producer / Client | Common Libraries |
|---|---|---|---|
| Kafka → REST | `nxop-flightops-common-kafka` | `nxop-flightops-common-rest` | Both |
| AMQ → Kafka | `nxop-flightops-common-mq` | `nxop-flightops-common-kafka` | mq + kafka |
| AMQ → REST | `nxop-flightops-common-mq` | `nxop-flightops-common-rest` | mq + rest |
| Kafka → Kafka | `nxop-flightops-common-kafka` | `nxop-flightops-common-kafka` | kafka only |
| REST API → REST | `spring-boot-starter-web` | `nxop-flightops-common-rest` | rest only |

---

## Exception Type Mapping

The handler layer must translate downstream exceptions into the consumer-side exception type:

| Downstream Exception | Meaning | Kafka Listener Action | AMQ Listener Action |
|---|---|---|---|
| `DownstreamApiException` (common-rest) | HTTP 4xx — permanent | → `EventProcessingException` → ack | → `MessageProcessingException` → ack |
| `DownstreamTransientException` (common-rest) | HTTP 5xx / network | Rethrow as `TransientException` → no ack | Rethrow → `session.recover()` |
| `EventProcessingException` (common-kafka) | Non-retryable | Ack — already correct type | N/A |
| `TransientException` (common-kafka) | Retryable | Rethrow — already correct type | Rethrow → `session.recover()` |
| `MessageProcessingException` (common-mq) | AMQ non-retryable | N/A | Ack + log DLQ TODO |
| `TransientMessageException` (common-mq) | AMQ retryable | N/A | `session.recover()` |
| `CallNotPermittedException` (resilience4j) | Circuit breaker open | → `TransientException` | Rethrow → `session.recover()` |

### Handler Exception Translation Pattern

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class FlightDataHandler {

  private final DownstreamApiClientService client;

  public void handleMessage(String payload) {
    try {
      FlightEventDto dto = parsePayload(payload);
      client.postFlight(dto);
    } catch (DownstreamApiException e) {
      log.error("Permanent downstream failure: {}", e.getMessage(), e);
      throw new EventProcessingException(e.getMessage(), e); // or MessageProcessingException for AMQ
    }
    // DownstreamTransientException propagates naturally — both Kafka and AMQ
    // listeners treat uncaught RuntimeExceptions as retryable.
  }
}
```

For complete code examples of all 5 combinations, see `docs/exception-translation-reference.md`.

---

## Timeout Budget Calculation

### Kafka Consumer

```
max.poll.interval.ms ≥ (read-timeout × max-retries) + handler-processing + safety-margin
```

Example: `read-timeout=30s`, `max-retries=3`, handler=2s, margin=30s:
```
max.poll.interval.ms ≥ (30 × 3) + 2 + 30 = 122s → set to at least 150000 (150s)
```

### Amazon MQ Consumer

AMQ does not have a poll interval timeout. Configure listener concurrency for parallel messages:

```yaml
activemq:
  listener:
    concurrency: 1-5
```

---

## Testing Combined Services

| Combination | Test Approach |
|---|---|
| Kafka + REST | `@EmbeddedKafka` + WireMock |
| AMQ + Kafka | Embedded ActiveMQ (`vm://localhost`) + `@EmbeddedKafka` |
| AMQ + REST | Embedded ActiveMQ + WireMock |

Inner `@TestConfiguration` for `KafkaTemplate` is always required (KafkaAutoConfiguration excluded).

---

## Dependency Checklist

| Combination | Additional pom.xml entries |
|---|---|
| + REST Client | `nxop-flightops-common-rest`, `httpclient5` (optional) |
| + Kafka Producer | Producer properties in YAML, `ProducerKafkaInitializerConfig` |
| + Amazon MQ | `nxop-flightops-common-mq`, `activemq.*` properties |
| + Circuit Breaker | `resilience4j-spring-boot3`, resilience4j YAML |
| + Bean Validation | `spring-boot-starter-validation` |
| + WireMock (test) | `wiremock-standalone:3.13.0` (test scope) |
