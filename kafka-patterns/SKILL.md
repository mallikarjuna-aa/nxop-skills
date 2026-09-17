# Kafka Patterns — Consumer, Config & Application Rules

Mandatory scaffolding, config patterns, and application.yml rules for Kafka consumer services
using `nxop-flightops-common-kafka`. Use when building or modifying any Kafka consumer service.

---

## 1. Mandatory Scaffolding Checklist

### 1.1 ObjectMapperConfig — Always required in `config` package

Every Kafka consumer service **must** declare an `ObjectMapperConfig` class with these beans:

```java
@Configuration
public class ObjectMapperConfig {

  @Bean
  @Primary
  public ObjectMapper objectMapper() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule());
  }

  @Bean("convertStringToObject")
  public ObjectMapper convertStringToObject() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
        .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Bean("convertDBObjectToString")
  public ObjectMapper convertDbObjectToString() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }
}
```

Inject via `@Qualifier("convertStringToObject")` — field **must** be named `convertStringToObject`:

```java
@RequiredArgsConstructor
@Service
public class FlightDataHandler {

  @Qualifier("convertStringToObject")
  private final ObjectMapper convertStringToObject;
}
```

### 1.2 Both downstream exception types — always handle as a pair

| Downstream Exception | Maps to | Action |
|---|---|---|
| `DownstreamApiException` | `EventProcessingException` | Non-retryable — ack, swallow |
| `DownstreamTransientException` | `TransientException` | Retryable — rethrow, no ack |

```java
try {
  restClient.postFlightData(payload);
} catch (DownstreamApiException e) {
  log.warn("Non-retryable downstream error: {}", e.getMessage());
  throw new EventProcessingException("Downstream API error", e);
} catch (DownstreamTransientException e) {
  log.warn("Transient downstream error, will retry: {}", e.getMessage());
  throw new TransientException("Downstream transient error", e);
}
```

### 1.3 `@HttpExchange` path vs `base-url`

- API path in `@HttpExchange("/api/v1/...")` on the interface
- `base-url` YAML: **only** `host:port` (no path suffix)

### 1.4 KafkaAutoConfiguration is excluded

`nxop-flightops-common-kafka` excludes `KafkaAutoConfiguration` globally. This means:
- No auto-configured `KafkaTemplate`, `KafkaListenerContainerFactory`, or producer/consumer beans
- Every Kafka bean must be declared explicitly in a `@Configuration` class
- `@EmbeddedKafka` IT tests need inner `@TestConfiguration` for `KafkaTemplate`

---

## 2. Kafka Consumer Config — Two-Class Pattern (Spring Boot 3+/4)

### Rule: Never combine `@ConfigurationProperties` with Spring-injected dependencies

**Class 1 — Pure POJO** (`ConsumerKafkaConfig.java`):
- `@ConfigurationProperties` only — no Spring bean dependencies
- No constructor other than default (Lombok `@Getter`/`@Setter`)
- `buildProperties()` is package-private

**Class 2 — Wiring class** (`ConsumerKafkaInitializerConfig.java`):
- `@Configuration` — receives both beans via `@RequiredArgsConstructor`
- `@PostConstruct` calls `kafkaCommonConfig.applyToCommon(consumerKafkaConfig.buildProperties())`

---

## 3. Guard SASL properties by protocol

```java
Properties buildProperties() {
  Properties properties = new Properties();
  properties.setProperty("bootstrap.servers", bootstrapServers);
  properties.setProperty("security.protocol", securityProtocol);
  if (securityProtocol != null && securityProtocol.toUpperCase().startsWith("SASL")) {
    properties.setProperty("sasl.mechanism", saslMechanism);
    properties.setProperty("sasl.jaas.config", saslJaasConfig);
    properties.setProperty("sasl.client.callback.handler.class", saslCallbackHandlerClass);
  }
}
```

Kafka 4.x validates class-type config values even when SASL is not active.

---

## 4. application.yml — all bound fields must exist

Every field in `ConsumerKafkaConfig` must have a corresponding YAML key or default.
Missing keys bind as `null` → `NullPointerException` in `Properties.setProperty()`.
