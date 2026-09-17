---
name: spring-kafka-producer-msk
description: >
  Implement, configure, and test Apache Kafka producers connecting to AWS MSK in NXOP
  FlightOps Spring Boot services using SyncPublishGateway from nxop-flightops-common-kafka.
  Covers producer config, MSK SASL guard, Avro serialization, synchronous publish
  semantics, and tests. Use when building or testing an MSK Kafka producer service.
---

# Skill: Spring Boot Kafka Producer

This skill defines enterprise standards for implementing, configuring, and testing **Apache Kafka
producers** in NXOP FlightOps Spring Boot services using `SyncPublishGateway` from
`nxop-flightops-common-kafka`. All code must comply with `copilot-instructions.md`.

> **Template notice:** All values shown in this skill (serializer class names, topic names,
> property paths, class names, and test payloads) are **illustrative examples only**. Replace
> every value with the ones appropriate for your specific service.

---

## Architecture Overview

The producer stack has three layers:

| Layer | Class | Responsibility |
|---|---|---|
| Config POJO | `ProducerKafkaConfig` | Bind YAML properties — no Spring deps |
| Wiring | `ProducerKafkaInitializerConfig` | Initialise `ProducerKafkaCommonConfig` via `@PostConstruct` |
| Publisher | `<Domain>KafkaPublisher` | Build `ProducerRecord` and delegate to the gateway |

**Beans from `nxop-flightops-common-kafka` (auto-registered, do not redefine):**

- `SyncPublishGateway` — blocks until broker ACKs or timeout. Translates exceptions to
  `TransientException` (retryable) or `EventProcessingException` (non-retryable).
- `AsyncPublishGateway` — fires `CompletableFuture` with same two-exception contract.
  **Only `SyncPublishGateway` is covered here** — see publisher reference for async swap path.
- `ProducerKafkaConfiguration` — creates `ProducerFactory` and `KafkaTemplate` bean named
  `"ProducerTemplate"`. Activated by `@ConditionalOnProperty(prefix = "kafka.msk.producer.config",
  name = "bootstrap-servers")`. Only usable after `applyToProducer()` has been called.

---

## Reference Files

Full code templates are in `references/` — load only when generating code:

| File | Contents |
|---|---|
| [maven-dependencies.md](references/maven-dependencies.md) | Core dep, AWS MSK deps (with exclusions), `@EmbeddedKafka` test deps |
| [config-pattern.md](references/config-pattern.md) | `ProducerKafkaConfig` POJO (SASL guard), `ProducerKafkaInitializerConfig`, application.yml producer block |
| [publisher-pattern.md](references/publisher-pattern.md) | Publisher class (`@Qualifier("ProducerTemplate")`), handler calling pattern, async swap guide |
| [test-patterns.md](references/test-patterns.md) | Unit test (3 scenarios), IT test (`@EmbeddedKafka` + consumer verification) |

---

## Rules

### Config
- **Two-class pattern required:** pure POJO (`@ConfigurationProperties`) + wiring class (`@Configuration`).
  Never combine them — triggers constructor-binding mode and NPEs in Spring Boot 3+/4.
- Use `properties.setProperty()` — never `properties.put()` (NPE on null value).
- **SASL guard:** Only set `sasl.*` properties when `securityProtocol.toUpperCase().startsWith("SASL")`.
  Kafka 4.x validates class-type configs even when SASL is not active.
- Enable `ProducerKafkaConfig` via `@EnableConfigurationProperties` on main class.
- All common-kafka beans are auto-registered via `AutoConfiguration.imports` — no `@Import` needed.

### Publisher
- Place in a `producer` subpackage — never in `kafka` (listener) or `service` (logic) packages.
- Use `@Service` annotation.
- **Do not use `@RequiredArgsConstructor`** — `@Qualifier("ProducerTemplate")` is not propagated.
  Write a manual constructor.
- Single public method: `publish(topic, trackingId, key, value)`. Accept topic as parameter.
- Declare `throws TransientException, EventProcessingException` in signature.
- Pass `producerKafkaConfig.getDeliveryTimeoutMs()` as `timeoutMs` to `sendSync()`.
- Import exceptions from `nxop-flightops-common-kafka`.

### Handler Integration
- The calling handler/listener must catch `EventProcessingException` (ack + skip) and
  rethrow `TransientException` (retry). Same contract as the consumer listener pattern.

### application.yml Checklist
Every field in `ProducerKafkaConfig` must have a corresponding YAML key:
```
bootstrap-servers, security-protocol, sasl-mechanism, sasl-jaas-config,
sasl-callback-handler-class, region, key-serializer, value-serializer,
max-request-size, request-timeout-ms, delivery-timeout-ms, enable-idempotence,
compression-type, metadata-max-age-ms, connections-max-idle-ms, linger-ms,
batch-size, buffer-memory, max-block-ms
```

### Testing
- Unit tests: `@ExtendWith(MockitoExtension.class)`, mock `SyncPublishGateway` + `KafkaTemplate`.
- IT tests: `@SpringBootTest` + `@EmbeddedKafka`, class names ending in `IT.java`.
- No inner `@TestConfiguration` needed for producer ITs (publisher already has `ProducerTemplate`).
- Override `bootstrap-servers`, `security-protocol=PLAINTEXT`, and serializers in `@TestPropertySource`.
- Do NOT add `sasl-*` properties for PLAINTEXT — Kafka 4.x rejects them.
- Use `KafkaTestUtils.getSingleRecord()` with `Duration.ofSeconds(30)` — no `Thread.sleep()`.
- Always declare `spring-kafka-test` + `scala-library:2.13.16:test` together.
- Use `@MockitoBean` (not `@MockBean` — removed in Spring Boot 4.x).

---

## What Copilot Should NOT Do

- Do not use `@RequiredArgsConstructor` on the publisher — `@Qualifier` is not propagated. Write constructor manually.
- Do not inject `KafkaTemplate` by type alone — always use `@Qualifier("ProducerTemplate")`.
- Do not hardcode topic names in the publisher — accept as parameter or inject from config.
- Do not put `@ConfigurationProperties` and `@PostConstruct` + Spring deps in same class.
- Do not set SASL properties unconditionally — guard with `startsWith("SASL")`.
- Do not set `sasl-*` in `@TestPropertySource` for `@EmbeddedKafka` tests.
- Do not use `@MockBean` — use `@MockitoBean`.
- Do not use `Thread.sleep()` — use `KafkaTestUtils.getSingleRecord()` with 30 s timeout.
- Do not use `RestTemplate` or `System.out.println`.
- Do not use wildcard imports.
- Do not add `spring-kafka-test` without `scala-library:2.13.16:test`.
- Do not define custom `ProducerFactory` or additional `KafkaTemplate` — common-kafka provides them.
- Do not use `AsyncPublishGateway` in AmazonMQ listener context — use `SyncPublishGateway`.
