# REST API — Client Patterns

## `@ConfigurationProperties` for Client Config

Extend `RestClientProperties` from `nxop-flightops-common-rest` (provides `baseUrl`,
`connectTimeout`, `readTimeout`):

```java
// Replace package, class name, and prefix
package com.aa.nxop.tps.yourservice.config;

import com.aa.nxop.flightops.common.rest.properties.RestClientProperties;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "clients.flight-data-api") // Replace prefix
public class FlightApiClientConfig extends RestClientProperties {
  // baseUrl, connectTimeout (Duration), readTimeout (Duration) are inherited.
  // Add service-specific fields here if needed.
}
```

Register: `@EnableConfigurationProperties(FlightApiClientConfig.class)`

```yaml
# REST Client (Outbound HTTP) — add to application.yml
# The prefix must match the @ConfigurationProperties prefix in your config class.
clients:
  flight-data-api:                                           # Replace key with your prefix
    base-url: ${DOWNSTREAM_API_BASE_URL}                    # Required; from env var
    connect-timeout: ${DOWNSTREAM_API_CONNECT_TIMEOUT:5s}   # Duration format; default 5s
    read-timeout: ${DOWNSTREAM_API_READ_TIMEOUT:30s}        # Duration format; default 30s
```

## RestClient Bean Configuration

```java
// Replace package, class name, property bean type, and bean name
package com.aa.nxop.tps.yourservice.config;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.util.Timeout;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
@RequiredArgsConstructor
public class RestClientConfig {

  private final FlightApiClientConfig clientConfig;

  @Bean
  public RestClient flightApiRestClient() {
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(Timeout.of(clientConfig.getConnectTimeout()))
        .setResponseTimeout(Timeout.of(clientConfig.getReadTimeout()))
        .build();
    HttpComponentsClientHttpRequestFactory factory =
        new HttpComponentsClientHttpRequestFactory(
            HttpClients.custom().setDefaultRequestConfig(requestConfig).build());
    return RestClient.builder()
        .baseUrl(clientConfig.getBaseUrl())
        .requestFactory(factory)
        .build();
  }
}
```

> Without `httpclient5` in `pom.xml`, use `SimpleClientHttpRequestFactory` instead.

## Declarative HTTP Interface — `@HttpExchange`

```java
// Replace package, interface name, endpoint paths, and DTO types
package com.aa.nxop.tps.yourservice.client;

import com.aa.nxop.tps.yourservice.dto.FlightResponseDto;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;

@HttpExchange("/api/v1/flights") // Replace with downstream base path
public interface FlightApiClient {

  @GetExchange("/{flightNumber}")
  FlightResponseDto getFlightByNumber(
      @org.springframework.web.bind.annotation.PathVariable String flightNumber);
}
```

Register as a Spring bean (add to `RestClientConfig`):

```java
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Bean
public FlightApiClient flightApiClient(RestClient flightApiRestClient) {
  RestClientAdapter adapter = RestClientAdapter.create(flightApiRestClient);
  HttpServiceProxyFactory factory = HttpServiceProxyFactory.builderFor(adapter).build();
  return factory.createClient(FlightApiClient.class);
}
```

## `DownstreamApiClientService` — Wrapping with `RetryableHttpExecutor`

Never put `@Retryable` on `@HttpExchange` or `handleMessage()`. Create a thin `@Service` wrapper:

```java
// Replace package, class name, client interface type, and DTO types
package com.aa.nxop.tps.yourservice.client;

import com.aa.nxop.flightops.common.rest.executor.RetryableHttpExecutor;
import com.aa.nxop.tps.yourservice.dto.FlightEventDto;
import com.aa.nxop.tps.yourservice.dto.DownstreamApiResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DownstreamApiClientService {

  private final FlightApiClient client;
  private final RetryableHttpExecutor executor;

  public DownstreamApiResponse postFlight(FlightEventDto dto) {
    return executor.execute(
        "postFlight[" + dto.getFlightNumber() + "]",
        () -> client.postFlight(dto));
  }
}
```

## Using `HttpResponseValidator` (ResponseEntity variant)

When `@HttpExchange` returns `ResponseEntity<T>`, validate explicitly:

```java
import com.aa.nxop.flightops.common.rest.validator.HttpResponseValidator;

// Inject alongside executor
private final HttpResponseValidator validator;

public ResponseEntity<DownstreamApiResponse> postFlight(FlightEventDto dto) {
  return executor.execute(
      "postFlight[" + dto.getFlightNumber() + "]",
      () -> validator.validate("postFlight", client.postFlight(dto)));
}
```

## Direct `RestClient` Call Pattern

For one-off calls when `@HttpExchange` is not suitable:

```java
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

try {
  FlightResponseDto response = restClient.get()
      .uri("/api/v1/flights/{flightNumber}", flightNumber)
      .accept(MediaType.APPLICATION_JSON)
      .retrieve()
      .body(FlightResponseDto.class);
  return response;
} catch (RestClientResponseException ex) {
  log.error("Downstream call failed: status={}, message={}",
      ex.getStatusCode(), ex.getMessage());
  throw new FlightNotFoundException("Flight not found: " + flightNumber, ex);
}
```
