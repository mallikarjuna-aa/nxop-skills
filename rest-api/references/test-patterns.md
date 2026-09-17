# REST API — Test Patterns

## Controller Unit Test — `@WebMvcTest`

```java
// Replace package, controller type, service type, DTO types, endpoint path, and test values
package com.aa.nxop.tps.yourservice.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.aa.nxop.tps.yourservice.dto.FlightResponseDto;
import com.aa.nxop.tps.yourservice.exception.FlightNotFoundException;
import com.aa.nxop.tps.yourservice.service.FlightService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(FlightController.class) // Replace with your controller class
class FlightControllerTest {

  @Autowired
  private MockMvc mockMvc;

  @MockitoBean
  private FlightService flightService; // Replace with your service type

  @Test
  void givenValidFlightNumber_whenGetFlight_thenReturns200WithBody() throws Exception {
    FlightResponseDto response = FlightResponseDto.builder()
        .flightNumber("AA100").origin("DFW").destination("LAX").status("ON_TIME")
        .build();
    when(flightService.getFlightByNumber("AA100")).thenReturn(response);

    mockMvc.perform(get("/api/v1/flights/AA100"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.flightNumber").value("AA100"))
        .andExpect(jsonPath("$.status").value("ON_TIME"));
  }

  @Test
  void givenUnknownFlightNumber_whenGetFlight_thenReturns404() throws Exception {
    when(flightService.getFlightByNumber(anyString()))
        .thenThrow(new FlightNotFoundException("Flight not found"));

    mockMvc.perform(get("/api/v1/flights/UNKNOWN"))
        .andExpect(status().isNotFound());
  }

  @Test
  void givenUnexpectedException_whenGetFlight_thenReturns500() throws Exception {
    when(flightService.getFlightByNumber(anyString()))
        .thenThrow(new RuntimeException("unexpected"));

    mockMvc.perform(get("/api/v1/flights/AA100"))
        .andExpect(status().isInternalServerError());
  }
}
```

## `DownstreamApiClientService` Unit Test

```java
// Replace package, class names, and DTO types
package com.aa.nxop.tps.yourservice.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.aa.nxop.flightops.common.rest.exception.DownstreamApiException;
import com.aa.nxop.flightops.common.rest.exception.DownstreamTransientException;
import com.aa.nxop.flightops.common.rest.executor.RetryableHttpExecutor;
import com.aa.nxop.tps.yourservice.dto.FlightEventDto;
import com.aa.nxop.tps.yourservice.dto.DownstreamApiResponse;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DownstreamApiClientServiceTest {

  @Mock
  private FlightApiClient client;

  @Mock
  private RetryableHttpExecutor executor;

  private DownstreamApiClientService service;

  @BeforeEach
  void setUp() {
    service = new DownstreamApiClientService(client, executor);
  }

  @Test
  void givenValidDto_whenPostFlight_thenReturnsResponse() {
    FlightEventDto dto = new FlightEventDto("AA100");
    DownstreamApiResponse expected = new DownstreamApiResponse();
    when(executor.execute(any(String.class), any(Supplier.class))).thenReturn(expected);

    DownstreamApiResponse result = service.postFlight(dto);

    assertThat(result).isSameAs(expected);
  }

  @Test
  void givenNonRetryableError_whenPostFlight_thenPropagatesDownstreamApiException() {
    FlightEventDto dto = new FlightEventDto("AA100");
    when(executor.execute(any(String.class), any(Supplier.class)))
        .thenThrow(new DownstreamApiException("client error"));

    assertThatThrownBy(() -> service.postFlight(dto))
        .isInstanceOf(DownstreamApiException.class);
  }

  @Test
  void givenTransientError_whenPostFlight_thenPropagatesDownstreamTransientException() {
    FlightEventDto dto = new FlightEventDto("AA100");
    when(executor.execute(any(String.class), any(Supplier.class)))
        .thenThrow(new DownstreamTransientException("server error"));

    assertThatThrownBy(() -> service.postFlight(dto))
        .isInstanceOf(DownstreamTransientException.class);
  }
}
```

## HTTP Client Integration Test — WireMock

```java
// Replace package, class name, service type, DTO types, and WireMock stubs
package com.aa.nxop.tps.yourservice.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.aa.nxop.flightops.common.rest.exception.DownstreamTransientException;
import com.aa.nxop.tps.yourservice.dto.FlightEventDto;
import com.aa.nxop.tps.yourservice.dto.DownstreamApiResponse;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@Tag("integration")
@SpringBootTest
@DirtiesContext
@WireMockTest
@TestPropertySource(properties = {
    "clients.downstream-api.connect-timeout=3s",
    "clients.downstream-api.read-timeout=10s"
})
class FlightApiClientIT {

  @Autowired
  private DownstreamApiClientService clientService;

  @Test
  void givenValidPayload_whenPostFlight_thenReturns200() {
    stubFor(post(urlEqualTo("/api/v1/flights"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("{\"status\":\"ACCEPTED\"}")));

    DownstreamApiResponse result =
        clientService.postFlight(new FlightEventDto("AA100"));

    assertThat(result.getStatus()).isEqualTo("ACCEPTED");
  }

  @Test
  void givenDownstreamReturns503_whenPostFlight_thenThrowsTransientException() {
    stubFor(post(urlEqualTo("/api/v1/flights"))
        .willReturn(aResponse().withStatus(503)));

    assertThatThrownBy(() ->
        clientService.postFlight(new FlightEventDto("AA100")))
        .isInstanceOf(DownstreamTransientException.class);
  }
}
```
