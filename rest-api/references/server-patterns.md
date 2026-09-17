# REST API — Server Patterns

## DTO Design

### Request DTO (mutable, with Bean Validation)

```java
// Replace package, class name, and fields to match your domain
package com.aa.nxop.tps.yourservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Inbound request payload — mutable so Spring MVC can deserialize into it. */
public class FlightRequestDto {

  @NotBlank(message = "Flight number must not be blank")
  @Size(max = 10, message = "Flight number must not exceed 10 characters")
  private String flightNumber;

  @NotNull(message = "Origin is required")
  private String origin;

  @NotNull(message = "Destination is required")
  private String destination;

  public String getFlightNumber() { return flightNumber; }
  public void setFlightNumber(String flightNumber) { this.flightNumber = flightNumber; }
  public String getOrigin() { return origin; }
  public void setOrigin(String origin) { this.origin = origin; }
  public String getDestination() { return destination; }
  public void setDestination(String destination) { this.destination = destination; }
}
```

### Response DTO (immutable, Lombok @Value)

```java
// Replace package, class name, and fields to match your domain
package com.aa.nxop.tps.yourservice.dto;

import lombok.Builder;
import lombok.Value;

/** Outbound response payload — immutable Lombok @Value. */
@Value
@Builder
public class FlightResponseDto {
  String flightNumber;
  String origin;
  String destination;
  String status;
}
```

## Controller Pattern

```java
// Replace package, class name, endpoint paths, DTO types, and service type/name
package com.aa.nxop.tps.yourservice.controller;

import com.aa.nxop.tps.yourservice.dto.FlightRequestDto;
import com.aa.nxop.tps.yourservice.dto.FlightResponseDto;
import com.aa.nxop.tps.yourservice.service.FlightService;
import jakarta.validation.Valid;
import java.net.URI;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/flights") // Replace with your base path
public class FlightController {

  private final FlightService flightService;

  @GetMapping("/{flightNumber}")
  public ResponseEntity<FlightResponseDto> getFlight(
      @PathVariable String flightNumber) {
    log.info("Received GET request for flight: {}", flightNumber);
    FlightResponseDto response = flightService.getFlightByNumber(flightNumber);
    return ResponseEntity.ok(response);
  }

  @PostMapping
  public ResponseEntity<FlightResponseDto> createFlight(
      @Valid @RequestBody FlightRequestDto request) {
    log.info("Received POST request to create flight: {}", request.getFlightNumber());
    FlightResponseDto created = flightService.createFlight(request);
    URI location = ServletUriComponentsBuilder.fromCurrentRequest()
        .path("/{id}")
        .buildAndExpand(created.getFlightNumber())
        .toUri();
    return ResponseEntity.created(location).body(created);
  }
}
```

## Global Exception Handler

Extend `ResponseEntityExceptionHandler` for Spring's built-in `ProblemDetail` handling.

```java
// Replace package and domain exception types
package com.aa.nxop.tps.yourservice.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

  @ExceptionHandler(FlightNotFoundException.class) // Replace with your domain exception
  public ProblemDetail handleFlightNotFound(FlightNotFoundException ex) {
    log.warn("Resource not found: {}", ex.getMessage());
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(
        HttpStatus.NOT_FOUND, ex.getMessage());
    problem.setTitle("Flight Not Found");
    return problem;
  }

  @ExceptionHandler(FlightConflictException.class) // Replace with your domain exception
  public ProblemDetail handleConflict(FlightConflictException ex) {
    log.warn("Business rule conflict: {}", ex.getMessage());
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(
        HttpStatus.CONFLICT, ex.getMessage());
    problem.setTitle("Conflict");
    return problem;
  }

  @ExceptionHandler(Exception.class)
  public ProblemDetail handleUnexpected(Exception ex) {
    log.error("Unexpected error", ex);
    return ProblemDetail.forStatusAndDetail(
        HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
  }
}
```
