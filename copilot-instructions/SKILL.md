---
name: copilot-instructions
description: >
  Apply the universal NXOP FlightOps engineering standards inherited by all services.
  Covers Java and Spring Boot conventions, Checkstyle, maintainability, exception handling,
  testing, Lombok, REST, dependency declarations, and behavior-preserving code changes.
  Use for every task that reads, creates, modifies, reviews, or tests repository code.
---

# Skill: GitHub Copilot Instructions

## Applies To

Apply this skill to the exact universal file scope:

```text
**
```

This repository is the **NXOP FlightOps template**. All downstream service repositories are derived from it.
These instructions apply universally across all projects that inherit from this template.

---

## Technology Stack

- **Language:** Java 25
- **Framework:** Spring Boot 4.1.1
- **Build Tool:** Maven (use `./mvnw` wrapper)
- **Lombok:** Use Lombok annotations to eliminate boilerplate (`@Getter`, `@Setter`, `@Builder`, `@Slf4j`, etc.)
- **Testing:** JUnit 6 (Jupiter) + Mockito + Spring Boot Test
- **Code Quality:** Checkstyle enforced at build time via `maven-checkstyle-plugin`

---

## Coding Style

### Checkstyle Compliance
- **All code must pass Checkstyle validation.** The project enforces **Google Java Style** via
  `google_checks.xml` bundled with `com.puppycrawl.tools:checkstyle`. Do not add, modify, or
  reference any local checkstyle config file.
- The following are enforced by Checkstyle at build time — do not restate them in code reviews:
  naming conventions, import ordering, max line length (100), indentation (2 spaces), brace
  placement (K&R), modifier order, visibility rules, `ParameterAssignment`, `StringLiteralEquality`,
  `MissingSwitchDefault`, `IllegalThrows`, `HideUtilityClassConstructor`, `NeedBraces`.
- **No wildcard imports** — always use explicit imports.
- Remove all unused imports before committing.

---

## Code Maintainability

- **Single Responsibility Principle:** Each class/method does one thing. Split large classes into focused services/helpers.
- **Small methods:** Keep methods concise and well-named.
- **Meaningful naming:** Names must communicate intent. Avoid abbreviations unless domain-standard (e.g., `IATA`, `ETD`).
- **Avoid magic numbers/strings:** Define literals as named constants or configuration values.
- **Layered architecture:** `@RestController` (presentation) → `@Service` (business logic) → `@Repository` (data). Never put business logic in controllers or repositories.
- **Constructor injection only:** Use Lombok `@RequiredArgsConstructor` on `@Service`/`@Component`. Never use `@Autowired` field injection. This keeps dependencies explicit for testing.
- **Immutability:** Prefer `final` fields, Lombok `@Value`, or Java records for DTOs.
- **Configuration via `@ConfigurationProperties`:** Not `@Value` field injection. Externalize to `application.yml`.
- **Limit deep nesting:** Use guard clauses, method extraction, or strategy patterns.
- **Avoid `ApplicationContext` look-ups** at runtime — use standard DI so tests can inject doubles directly.

---

## Exception Handling

- **Never swallow exceptions silently.** An empty or comment-only `catch` block is forbidden.
- **Use specific exception types** — catch the most specific exception type applicable. Avoid catching `Exception` or `Throwable` unless at a top-level boundary handler.
- **Do not declare `throws RuntimeException`, `throws Error`, or `throws Throwable`** in method signatures (enforced by Checkstyle `IllegalThrows` rule).
- **Create domain-specific exceptions** (e.g., `FlightNotFoundException`, `BookingConflictException`) that extend `RuntimeException` for unrecoverable business rule violations.
- **Use `@ControllerAdvice` / `@RestControllerAdvice`** for centralized, consistent HTTP error response handling. Do not handle exceptions individually in each controller.
- **Log exceptions at the appropriate level:**
  - `log.error(...)` with the full exception for unexpected/system errors
  - `log.warn(...)` for recoverable business exceptions
  - Never log and re-throw without adding context — it creates duplicate log noise
- **Preserve exception chains** — when wrapping, always pass the original exception as the `cause`: `throw new ServiceException("message", e)`
- **Validate inputs early** (fail fast) using Spring's `@Valid` / `@Validated` with Bean Validation (`jakarta.validation`) constraints, and handle `MethodArgumentNotValidException` globally.
- **HTTP semantics in REST APIs:**
  - `400 Bad Request` — invalid client input
  - `404 Not Found` — resource does not exist
  - `409 Conflict` — business rule violation
  - `500 Internal Server Error` — unexpected system failures (never expose stack traces to clients)
- **REST error response format:** Return `ProblemDetail` (RFC 7807 / RFC 9457) for all error
  responses from `@RestControllerAdvice` handlers. Extend `ResponseEntityExceptionHandler` to
  integrate with Spring's built-in `ProblemDetail` handling. Never return plain `String` or a
  custom error wrapper — `ProblemDetail` is the Spring Framework 6+/Boot 4 native standard.

---

## Testing

### General Testing Principles
- Tests must be **fast, isolated, repeatable, and self-validating**.
- Test file naming must end with `Test.java` (e.g., `FlightServiceTest.java`) — this is required for both `maven-surefire-plugin` and `maven-failsafe-plugin` to pick them up correctly.
- Maintain tests in `src/test/java` mirroring the package structure of the class under test.
- Every public method of a service or component class must have at least one test.
- Maintain a single `@BeforeEach setUp()` method to initialize all test fixtures
- Group related tests by functionality, not by exception type

## Mock & Field Naming Consistency
- Use full descriptive names for mocks: `flightDataHandler`, `acknowledgment` (not `ack`)
- Import static methods from Mockito: `verify`, `doThrow`, `when`, `never`, `verifyNoInteractions`
- Use AssertJ `assertThatThrownBy()` for exception testing (not JUnit's `assertThrows()`)

## Required Test Scenarios (Mandatory Coverage)
Use the `testing-patterns` skill for required test scenario checklists (Kafka listener,
controller, and message handler tests) when editing `*Test.java` or `*IT.java` files.

### Unit Tests
- Use **JUnit 6** (`@Test`, `@ParameterizedTest`, `@ExtendWith`) for all unit tests.
- Use **Mockito** (`@Mock`, `@InjectMocks`, `@ExtendWith(MockitoExtension.class)`) to isolate the class under test from its dependencies.
- **Do not load a Spring context** (`@SpringBootTest`) in unit tests — it is slow and couples tests to configuration. Use plain JUnit 6 with Mockito instead.
- Follow the **Arrange → Act → Assert** (AAA) pattern with a blank line between each section.
- Use descriptive test method names that express intent: `givenValidFlightId_whenGetFlight_thenReturnsFlightDetails()`.
- Test **both happy paths and failure/edge-case paths** (null inputs, empty collections, boundary values, thrown exceptions).
- Use `@ParameterizedTest` with `@MethodSource` or `@CsvSource` for data-driven tests instead of duplicating test methods.
- Assert on **behavior and outcomes**, not on implementation details (avoid asserting on private fields or internal call counts unless critical).

### Integration Tests
- Use **`@SpringBootTest`** only for integration tests that need the full application context or embedded server.
- Prefer **`@WebMvcTest`** for controller-layer integration tests — it loads only the web layer (faster than `@SpringBootTest`).
- Use **`@DataJpaTest`** for repository-layer integration tests with an embedded database.
- Use **WireMock** or `@MockitoBean` for external HTTP dependencies in integration tests — do not call real external services.
- Integration test class names must end with `IT.java` to align with the `maven-failsafe-plugin` include pattern (`**/*IT.java`).
- Annotate integration tests with a custom tag or category marker (e.g., `@Tag("integration")`) to allow selective test execution.
- **`@EmbeddedKafka` IT tests must always include an inner `@TestConfiguration` class that provides
  `KafkaTemplate`.** `nxop-flightops-common-kafka` excludes `KafkaAutoConfiguration` globally, so
  no `KafkaTemplate` bean is ever auto-configured. An `@Autowired KafkaTemplate` without a provider
  will fail with `UnsatisfiedDependencyException` at context load. Wire the bean to
  `${spring.embedded.kafka.brokers}` using a `DefaultKafkaProducerFactory` with `StringSerializer`.
  Always use plain `@SpringBootTest` (no `classes` attribute) — specifying `classes` prevents
  Spring from discovering the inner `@TestConfiguration` and causes the same failure.
- In `@TestPropertySource` for `@EmbeddedKafka` tests, always set `security-protocol=PLAINTEXT`
  and omit all `sasl-*` properties — they are not required and Kafka 4.x will reject invalid
  class-type values at startup even when SASL is not active.

### Test Coverage
- Aim for **minimum 80% line and branch coverage** for all `src/main/java` code.
- **All new features must include tests** before the code is considered complete — treat untested code as a build failure.
- Prioritize **branch/condition coverage** over line coverage; an untested `else` branch is a hidden defect.
- Do not write tests that test framework behavior (Spring wiring, Lombok-generated methods) — focus on business logic.

### Mocking & Test Doubles
- **Never use `@MockBean`** — it was removed in Spring Boot 4.x. Always use `@MockitoBean`
  (from `org.springframework.test.context.bean.override.mockito`) in Spring integration test slices;
  use `@Mock` + `@ExtendWith(MockitoExtension.class)` in plain unit tests.
- Avoid `Mockito.spy()` on the class under test — it is a code smell indicating the class has too many responsibilities.
- Verify mock interactions (`Mockito.verify(...)`) only when the side effect (e.g., calling an external service) is the primary behavior being tested.

### Avro Generated Classes in Tests
- **Always use Mockito `@Mock` for Avro-generated classes** (e.g., `FlightEvent`, `Flight`, `Key`,
  `AirlineCode`) in unit tests. Never use `newBuilder()` — the `.avsc` schema has many fields
  without defaults, and Avro builders require ALL non-default fields to be set explicitly.
- The schema is the source of truth. Do NOT modify `.avsc` files to add defaults for testing.
- Stub only the fields your test needs: `when(mockKey.getFltNum()).thenReturn("1234")`.

---

## Lombok Usage

- Prefer `@Slf4j` for loggers — the field must be named `log` (required by Checkstyle `ConstantName` rule).
- Use `@RequiredArgsConstructor` on Spring beans for constructor injection.
- Use `@Builder` for complex object construction in tests and production code.
- Use `@Data` only on simple mutable POJOs. Prefer `@Value` for immutable DTOs.
- Do not use `@EqualsAndHashCode` on JPA entities with bidirectional relationships — it can cause infinite loops.

---

## Spring Boot Conventions

- Use `@RestController` (not `@Controller` + `@ResponseBody`) for REST endpoints.
- Keep controllers thin — delegate all logic to `@Service` beans.
- Use `@ConfigurationProperties` classes (not `@Value`) for typed, validated, grouped configuration.
- Enable `spring-boot-actuator` health and info endpoints; do not disable them.
- Use `application.yml` (not `application.properties`) for configuration. Environment-specific overrides go in profile-specific files (e.g., `application-dev.yml`).
- **HTTP client:** Use `RestClient` (Spring Framework 6+) for all new synchronous outbound HTTP calls.
  `RestTemplate` is deprecated since Spring 6.1 — do not introduce it in new code.
- **Declarative HTTP client:** Prefer `@HttpExchange` interfaces backed by `HttpServiceProxyFactory`
  over manual `RestClient` call-sites scattered across services.
- Externalize all client base URLs and timeout values to `application.yml` via
  `@ConfigurationProperties` — never hardcode them.

---

## Code-Truth Policies

These policies govern how Copilot interacts with **existing code**. They exist to protect established behavior and prevent silent dependency assumptions.

- **Never alter the observable behavior of existing code.** Refactoring — renaming, extracting methods, reordering logic, or restructuring for readability and Checkstyle compliance — is permitted. However, the external output, return values, side effects, and exception behavior of any existing method must remain byte-for-byte identical after the change. If a behavioral change is genuinely required, state it explicitly, explain the reason, and seek confirmation before proceeding.
- **Always declare missing dependencies before using them.** Before generating code that introduces a library, framework, or module not already declared in `pom.xml`, explicitly identify each missing dependency with its `groupId`, `artifactId`, and recommended `<scope>`, and present them as required additions. Never write import statements for undeclared dependencies and assume they will resolve.

---

## What Copilot Should NOT Do (unique guardrails)

Rules already stated positively above are omitted here. These are additional guardrails:
- Do not generate `System.out.println` — use the `log` logger from `@Slf4j`.
- Do not use `log.debug(...)` for standard `@KafkaListener` processing logs — use `log.info(...)`.
- Do not use `com.github.tomakehurst:wiremock-jre8` — it is incompatible with Java 25/Spring Boot
  4; use `org.wiremock:wiremock-standalone:3.x` instead.
- Do not import `javax.validation.*` — always use `jakarta.validation.*` in Spring Boot 3+/4.
