---
name: spring-kafka-consumer-msk
description: >
  Implement, configure, and test Apache Kafka consumers connecting to AWS MSK in NXOP
  FlightOps Spring Boot services using nxop-flightops-common-kafka. Covers the
  Listener -> Handler -> Mapper pattern, two-class config (POJO + initializer), SASL guard,
  Avro deserialization, and @EmbeddedKafka integration tests. Use when building or testing
  an MSK Kafka consumer service.
---

# Skill: Spring Boot Kafka Consumer

This skill defines enterprise standards for implementing, configuring, and testing **Apache Kafka consumers**
in NXOP FlightOps Spring Boot services. All code must comply with `copilot-instructions.md`.

> **Template notice:** All values shown in this skill (deserializer class names, topic names, group IDs,
> concurrency settings, property paths, listener class names, service class names, and test payloads)
> are **illustrative examples only**. Replace every value with the ones appropriate for your service.

---

## Architecture Overview

A Kafka consumer service consists of four artifacts:

| Artifact | Layer | Purpose |
|---|---|---|
| `ConsumerKafkaConfig` | Config POJO | Binds `kafka.msk.consumer.config.*` YAML properties |
| `ConsumerKafkaInitializerConfig` | Config wiring | Injects POJO + common-kafka bean; calls `applyToCommon()` |
| `FlightEventKafkaListener` | Listener (`@Component`) | Receives `ConsumerRecord`, logs metadata, delegates to handler |
| `FlightDataHandler` | Service (`@Service`) | Business logic — never in listener |
| `flight.avsc` | Avro schema (`com.aa.opshub.avro.flight`) | Defines inbound message contract; generates Java classes via `avro-maven-plugin` |
| `FlightDataEventDto` | Outbound DTO | REST request body — projection of key fields from Avro-generated `FlightEvent` |

`ConsumerKafkaConfiguration` (from `nxop-flightops-common-kafka`) auto-configures `ConsumerFactory`
and `ConsumerKafkaConnectionFactory` (`AckMode.MANUAL`, 30 s auth-exception retry, `DefaultErrorHandler`
with `ContainerPausingRecoverer`). **Do not redefine these beans.**

`KafkaBrokerHealthCheck` (from `nxop-flightops-common-kafka`) polls the broker every 2 min and
resumes paused containers automatically. No additional configuration required.

---

## Reference Files

Full code templates are in `references/` — load only when generating code:

| File | Contents |
|---|---|
| [maven-dependencies.md](references/maven-dependencies.md) | Core dep, AWS MSK deps (with exclusions), `@EmbeddedKafka` test deps (spring-kafka-test + scala-library pair) |
| [config-pattern.md](references/config-pattern.md) | `ConsumerKafkaConfig` POJO (SASL guard), `ConsumerKafkaInitializerConfig` wiring class, `ConsumerKafkaConfiguration` reference |
| [listener-pattern.md](references/listener-pattern.md) | Listener class with `ConsumerRecord`, timestamp formatting, exception handling |
| [test-patterns.md](references/test-patterns.md) | Unit test (4 scenarios), IT test (`@EmbeddedKafka` + `@TestConfiguration`) |
| [avro-schema-pattern.md](references/avro-schema-pattern.md) | Avro `.avsc` schema, handler deserialization to generated class, DTO mapping |
| [flight.avsc](references/flight.avsc) | Canonical Avro schema — copy to new project's `src/main/resources/avroschema/` |

---

## Rules

### Config
- **Two-class pattern required:** pure POJO (`@ConfigurationProperties`) + wiring class (`@Configuration`).
  Never inject `ConsumerKafkaCommonConfig` via constructor into a `@ConfigurationProperties` class — it causes
  constructor-binding `null` injection (`BeanCreationException`).
- Use `properties.setProperty()` — never `properties.put()` (NPE on null value).
- **SASL guard:** Only set `sasl.*` properties when `securityProtocol.toUpperCase().startsWith("SASL")`.
  Kafka 4.x validates class-type configs even when SASL is not active.
- Enable `ConsumerKafkaConfig` via `@EnableConfigurationProperties(ConsumerKafkaConfig.class)` on main class.
- All common-kafka beans are auto-registered via `AutoConfiguration.imports` — no `@Import` needed.
- Do **not** add `@EnableKafka` or `@EnableScheduling` — both are inherited from `nxop-flightops-common-kafka`.

### Listener
- Always use `ConsumerRecord<K, V>` as the parameter — exposes topic, partition, offset, timestamp.
- Log topic, partition, offset, and UTC timestamp at `INFO` level on every record.
- Delegate all business logic to a `@Service` — the listener only extracts, logs, and delegates.
- Handle exceptions explicitly by type — never use empty or comment-only `catch` blocks.
- Catch `EventProcessingException` → log error with full coordinates, acknowledge (non-retryable).
- Catch `TransientException` → log error with full coordinates, rethrow for container retry.
- Place the listener in a dedicated `@Component` class — never inside `@Service` or `@RestController`.

### Avro Schema
- Every Kafka consumer service **must** copy `flight.avsc` to `src/main/resources/avroschema/`.
- The schema namespace is `com.aa.opshub.avro.flight` — shared across all services; **do not change it**.
- The `avro-maven-plugin` generates Java classes from the schema during `generate-sources`.
- The handler's `parsePayload()` uses `instanceof` check first for pre-deserialized Avro objects (from Glue Schema Registry), then falls back to `ObjectMapper`.
- The Avro `FlightEvent` has a single `flight` field → nested `Flight` → `Key` → `fltNum`, `depSta`, `fltOrgDate`, `airlineCode.IATA`.
- Always map from the Avro-generated class to a separate outbound DTO for REST calls.
- Use a `toStr()` helper on Avro `CharSequence` fields — handles null from nullable unions.
- Exclude generated classes from Checkstyle: `com/aa/opshub/avro/flight/**`.
- **Amazon MQ consumers do NOT use Avro** — MQ listeners use hand-written DTOs only.

### application.yml Checklist
Every field in `ConsumerKafkaConfig` must have a corresponding YAML key:
```
bootstrap-servers, security-protocol, sasl-mechanism, sasl-jaas-config,
sasl-callback-handler-class, partition-assignment-strategy, request-timeout-ms,
metadata-max-age-ms, connections-max-idle-ms, max-poll-records, max-poll-interval-ms,
offset-reset, region, key-deserializer, value-deserializer
```

### Testing
- Unit tests: `@ExtendWith(MockitoExtension.class)`, fabricate `ConsumerRecord`, no Spring context.
- IT tests: `@SpringBootTest` + `@EmbeddedKafka`, class names ending in `IT.java`.
- **`@TestConfiguration` inner class is mandatory** for `KafkaTemplate` — excluded by common-kafka.
- Use plain `@SpringBootTest` (no `classes` attribute) — prevents `@TestConfiguration` discovery.
- Set `security-protocol=PLAINTEXT` in `@TestPropertySource`; omit all `sasl-*` properties.
- Awaitility `atMost(Duration.ofSeconds(30))` minimum — KRaft rebalance takes 15–20 s.
- Always declare `spring-kafka-test` + `scala-library:2.13.16:test` together.
- Use `@MockitoBean` (not `@MockBean` — removed in Spring Boot 4.x).

### Observability
- `KafkaBrokerHealthCheck` auto-resumes paused containers — no config needed.
- Propagate correlation ID via MDC (`MDC.put("correlationId", ...)`) in handler; add
  `%X{correlationId}` to `logback.xml`.

---

## What Copilot Should NOT Do

- Do not use plain `String` as `@KafkaListener` parameter — always use `ConsumerRecord<K, V>`.
- Do not use `@Value` for topic names or Kafka config — always use `@ConfigurationProperties`.
- Do not use `Thread.sleep()` in tests — always use `Awaitility`.
- Do not use `enable-auto-commit: true` — always use `ack-mode: MANUAL`.
- Do not use empty or comment-only `catch` blocks — always log and handle.
- Do not use `log.debug(...)` for listener processing — always use `log.info(...)`.
- Do not place `@KafkaListener` inside `@Service` or `@RestController`.
- Do not hardcode bootstrap servers — externalize via `application.yml`.
- Do not use `@Autowired` field injection — use constructor injection.
- Do not use wildcard imports — always explicit.
- Do not use `System.out.println` — always `@Slf4j` `log`.
- Do not add `spring-kafka-test` without `scala-library:2.13.16:test`.
- Do not use `Duration.ofSeconds(10)` for Awaitility — minimum 30 s.
- Do not use `null` check as deep `if` wrapper — use early return with `log.warn`.
- Do not use `log.debug()` with string concatenation — use `log.info()` with `{}` placeholders.
- Do not leave `// TODO: publish to DLT` as only action in `EventProcessingException` catch — always
  log full record coordinates at `log.error`.
- Do not use `@MockBean` — removed in Spring Boot 4.x; use `@MockitoBean`.
- Do not use `@SpringBootTest(classes = ...)` in `@EmbeddedKafka` IT tests.
