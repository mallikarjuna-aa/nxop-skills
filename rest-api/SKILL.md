---
name: rest-api
description: >
  Implement, configure, and test RESTful API endpoints and outbound HTTP clients in NXOP
  FlightOps Spring Boot 4 services. Covers inbound @RestController + DTO validation +
  ProblemDetail error handling, and outbound RestClient / @HttpExchange clients with retry,
  circuit breaker, and the common-rest exception hierarchy. Use when building or testing
  REST endpoints or HTTP client integrations.
---

# Skill: Spring Boot 4 RESTful API & HTTP Client

This skill defines enterprise standards for implementing, configuring, and testing **RESTful API
endpoints and outbound HTTP clients** in NXOP FlightOps Spring Boot 4 services.
All code must comply with `copilot-instructions.md`.

> **Template notice:** All values shown (class names, package paths, property prefixes, endpoint
> paths, base URLs, DTO fields, test payloads) are **illustrative examples only**. Replace every
> value with the ones appropriate for your specific service.

---

## Architecture Overview

### Server Side (Inbound REST)

| Artifact | Layer | Purpose |
|---|---|---|
| Request DTO | DTO | `@Valid` + `jakarta.validation` constraints |
| Response DTO | DTO | Immutable `@Value @Builder` |
| `@RestController` | Presentation | Validate request, delegate, map response |
| `@Service` | Business logic | All domain logic — never in controller |
| `GlobalExceptionHandler` | Exception | `@RestControllerAdvice` extending `ResponseEntityExceptionHandler`, returns `ProblemDetail` |

### Client Side (Outbound HTTP)

```
YourListener / YourController
      ↓
YourHandler                    (business logic)
      ↓
DownstreamApiClientService     (@Service — thin wrapper + optional @CircuitBreaker)
      ↓
RetryableHttpExecutor          (nxop-flightops-common-rest — retry + exception mapping)
      ↓
FlightApiClient                (@HttpExchange — HTTP contract only)
      ↓
RestClient                     (configured in RestClientConfig — baseUrl, timeouts, pool)
```

### Exception Hierarchy (`nxop-flightops-common-rest`)

| Downstream response | Exception | Retried? |
|---|---|---|
| HTTP 2xx | — returns normally | N/A |
| HTTP 4xx | `DownstreamApiException` | **No** — permanent |
| HTTP 5xx / network | `DownstreamTransientException` | **Yes** — 3x exponential |

> `DownstreamApiException` ≈ Kafka's `EventProcessingException`.
> `DownstreamTransientException` ≈ Kafka's `TransientException`.

---

## Reference Files

Full code templates are in `references/` — load only when generating code:

| File | Contents |
|---|---|
| [server-patterns.md](references/server-patterns.md) | Request/response DTOs, controller pattern, `GlobalExceptionHandler` |
| [client-patterns.md](references/client-patterns.md) | `@ConfigurationProperties` + `RestClient` bean, `@HttpExchange` + `HttpServiceProxyFactory`, `DownstreamApiClientService`, `HttpResponseValidator`, direct `RestClient` call |
| [circuit-breaker-pattern.md](references/circuit-breaker-pattern.md) | `@CircuitBreaker` on service, `CallNotPermittedException` handler, Resilience4j YAML config |
| [test-patterns.md](references/test-patterns.md) | `@WebMvcTest` controller test, `DownstreamApiClientService` unit test, WireMock IT test |

---

## Maven Dependencies

### Server-side
- `spring-boot-starter-web` — already in template `pom.xml`
- `spring-boot-starter-validation` — add for `@Valid` on request DTOs

### Client-side
- `nxop-flightops-common-rest:1.0.0` — `RetryableHttpExecutor`, `HttpResponseValidator`, exception hierarchy, `RestClientProperties`
- `httpclient5` (optional) — Apache HttpComponents 5 connection pool for `RestClient`

### Testing
- `org.wiremock:wiremock-standalone:3.13.0` (test scope) — **never** use `wiremock-jre8`

### Circuit Breaker (optional)
- `io.github.resilience4j:resilience4j-spring-boot3:2.2.0` — compatible with Spring Boot 4.x

---

## Rules

### Server Side
- Always use `@RestController` — never `@Controller` + `@ResponseBody`.
- Keep controllers thin — all logic in `@Service`.
- Separate request and response DTOs — never expose JPA entities.
- `@Valid` on every `@RequestBody` parameter — Bean Validation is not applied without it.
- Return `ProblemDetail` (RFC 7807) for all errors — extend `ResponseEntityExceptionHandler`.
- HTTP status semantics: `200 OK`, `201 Created` (with `Location`), `204 No Content`,
  `400 Bad Request`, `404 Not Found`, `409 Conflict`, `500 Internal Server Error`.
- Never expose stack traces in 5xx responses.
- Use `jakarta.validation.*` — never `javax.validation.*`.

### Client Side
- Use `RestClient` for all new synchronous HTTP calls — `RestTemplate` is deprecated since 6.1.
- Extend `RestClientProperties` from `nxop-flightops-common-rest` for typed config.
- Prefer `@HttpExchange` interfaces + `HttpServiceProxyFactory` over scattered `RestClient` calls.
- Wrap `@HttpExchange` in `DownstreamApiClientService` with `RetryableHttpExecutor`.
- Never put `@Retryable` on `@HttpExchange` — two AOP proxies cannot stack.
- Apply `@CircuitBreaker` at `DownstreamApiClientService` level, not on `@HttpExchange` or executor.
- Externalize all base URLs and timeouts to `application.yml` — never hardcode.

### application.yml Checklist

```yaml
clients:
  <prefix>:
    base-url: ${ENV_VAR}          # Required — null causes NPE
    connect-timeout: 5s           # Duration format
    read-timeout: 30s             # Duration format
```

### Testing
- `@WebMvcTest` for controller tests (not `@SpringBootTest`).
- `@MockitoBean` for `@Service` deps (not `@MockBean` — removed in Spring Boot 4.x).
- WireMock for all outbound HTTP stubs — never call real services.
- IT class names must end with `IT` for `maven-failsafe-plugin`.
- `@Tag("integration")` on all IT tests.
- **Required controller scenarios:** happy path (200), validation failure (400), not found (404), server error (500).
- **Required client service scenarios:** happy path, `DownstreamApiException`, `DownstreamTransientException`.
- Use AssertJ for all assertions. Follow AAA pattern.

---

## What Copilot Should NOT Do

- Do not use `RestTemplate` — deprecated since Spring 6.1. Use `RestClient`.
- Do not use `@Autowired` field injection — use `@RequiredArgsConstructor`.
- Do not use `@MockBean` — removed in Spring Boot 4.x. Use `@MockitoBean`.
- Do not expose JPA entities from controllers — map to DTOs.
- Do not place business logic in `@RestController`.
- Do not use `@Value` for client config — use `@ConfigurationProperties`.
- Do not hardcode base URLs or timeouts.
- Do not expose stack traces in error responses.
- Do not call real services from tests — use WireMock.
- Do not use `Thread.sleep()` in tests — use Awaitility.
- Do not import `javax.validation.*` — use `jakarta.validation.*`.
- Do not use wildcard imports.
- Do not use `System.out.println` — use `@Slf4j` `log`.
- Do not use `@Controller` + `@ResponseBody` — use `@RestController`.
- Do not skip `@Valid` on `@RequestBody` parameters.
- Do not implement manual retry loops — use `RetryableHttpExecutor`.
- Do not put `@Retryable` on `@HttpExchange` — use `DownstreamApiClientService` wrapper.
- Do not apply `@CircuitBreaker` on `@HttpExchange` or `RetryableHttpExecutor` — apply at
  `DownstreamApiClientService` level.
- Do not define `baseUrl`, `connectTimeout`, `readTimeout` fields directly — extend `RestClientProperties`.
- Do not use `com.github.tomakehurst:wiremock-jre8` — use `org.wiremock:wiremock-standalone:3.x`.
