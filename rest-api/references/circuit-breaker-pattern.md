# REST API — Circuit Breaker Pattern (Resilience4j)

## When to Add

- Service makes outbound HTTP calls via `DownstreamApiClientService`.
- Downstream has experienced intermittent outages.
- You want to protect upstream callers from cascading timeouts.

> Skip this section if the service only consumes messages without outbound HTTP calls.

## Architecture

```
YourListener / YourController
      ↓
YourHandler                    (business logic)
      ↓
DownstreamApiClientService     (@Service + @CircuitBreaker)
      ↓
RetryableHttpExecutor          (retry + exception mapping)
      ↓
FlightApiClient                (@HttpExchange)
```

## Applying `@CircuitBreaker`

```java
package com.aa.nxop.tps.yourservice.client;

import com.aa.nxop.flightops.common.rest.executor.RetryableHttpExecutor;
import com.aa.nxop.tps.yourservice.dto.FlightEventDto;
import com.aa.nxop.tps.yourservice.dto.DownstreamApiResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DownstreamApiClientService {

  private final FlightApiClient client;
  private final RetryableHttpExecutor executor;

  @CircuitBreaker(name = "downstream-api") // Must match application.yml instance name
  public DownstreamApiResponse postFlight(FlightEventDto dto) {
    return executor.execute(
        "postFlight[" + dto.getFlightNumber() + "]",
        () -> client.postFlight(dto));
  }
}
```

## `CallNotPermittedException` Handler

```java
// Add to @RestControllerAdvice
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ExceptionHandler(CallNotPermittedException.class)
ProblemDetail handleCircuitBreakerOpen(CallNotPermittedException ex) {
  log.warn("Circuit breaker open for downstream call: {}", ex.getMessage());
  ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.SERVICE_UNAVAILABLE);
  problem.setTitle("Service Unavailable");
  problem.setDetail("Downstream service is temporarily unavailable. Please retry later.");
  return problem;
}
```

- **Kafka / AMQ listener handler:** Treat `CallNotPermittedException` as `TransientException` —
  rethrow so the message is retried after the circuit half-opens.
- **REST controller handler:** Return `503 Service Unavailable` via the handler above.

## application.yml Configuration

```yaml
resilience4j:
  circuitbreaker:
    instances:
      downstream-api:                # Must match @CircuitBreaker(name = "downstream-api")
        sliding-window-size: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 30s
        permitted-number-of-calls-in-half-open-state: 3
        minimum-number-of-calls: 5
        register-health-indicator: true
```

## Maven Dependency

```xml
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-spring-boot3</artifactId>
    <version>2.2.0</version>
</dependency>
```

> `resilience4j-spring-boot3` is compatible with Spring Boot 4.x.
> `spring-boot-starter-aop` (already in starter) enables `@CircuitBreaker`.
