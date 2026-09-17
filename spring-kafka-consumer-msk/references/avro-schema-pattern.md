# Kafka Consumer — Avro Schema Pattern

## Schema Location

Place all `.avsc` files under:

```
src/main/resources/avroschema/
```

The `avro-maven-plugin` scans this directory during `generate-sources` and produces Java classes
into `target/generated-sources/java/`. The `namespace` field in the schema determines the
package of the generated class. Name the schema file after the domain (e.g., `flight.avsc`).

## Source Schema

The canonical Avro schema is maintained at [flight.avsc](flight.avsc) in this references directory.
When creating a new Kafka consumer project, **copy this file** to the new project's
`src/main/resources/avroschema/flight.avsc`.

**Do not modify the schema** — it is owned by the upstream producer team. Copy it as-is.
The shared namespace is `com.aa.opshub.avro.flight` and is used consistently across all
services that consume flight events.

## Schema Structure

The production schema (`flight.avsc`, ~5700 lines) defines:

| Record | Key Fields | Notes |
|---|---|---|
| `FlightEvent` (root) | `flight` | Single nullable field → `Flight` record |
| `Flight` | `key`, `alternates`, `cabinCapacity`, `crewData`, `delayCodes`, `event`, `times`, … | 40+ sub-records, all nullable unions |
| `Key` | `airlineCode`, `depSta`, `dupDepStaNum`, `fltNum`, `fltOrgDate` | Primary flight identifier |
| `AirlineCode` | `IATA`, `ICAO` | Airline code pair |

All fields use nullable unions (`["null", "type"]`). Navigate via:
`FlightEvent → getFlight() → getKey() → getFltNum()`.

## Generated Classes

After `mvn generate-sources`, the plugin creates classes in:

```
target/generated-sources/java/com/aa/opshub/avro/flight/
```

Key generated classes: `FlightEvent`, `Flight`, `Key`, `AirlineCode`, `Alternates`,
`CabinCapacity`, `CargoItems`, `Connection`, `CrewData`, `DataTime`, `DelayCodes`,
`Event`, `FosPartition`, `InfoIndicators`, `LdiInfo`, `Times`, and many more.

Each class extends `org.apache.avro.specific.SpecificRecordBase` and provides:
- Standard getters/setters for each field
- A static `SCHEMA$` field
- A `Builder` via `ClassName.newBuilder()`

## Handler Pattern

The handler uses `ObjectMapper` as a fallback to deserialize the Kafka payload into the
Avro-generated class. In production, the Glue Schema Registry deserializer delivers a
`FlightEvent` instance directly. The handler maps key identification fields to a separate
outbound DTO for the REST client.

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class FlightDataHandler {

  private final FlightDataApiClientService client;
  private final ObjectMapper objectMapper;

  public void handleMessage(Object payload) {
    FlightEvent event = parsePayload(payload);
    String flightId = extractFlightIdentifier(event);
    log.info("Processing flight data event for {}", flightId);
    try {
      FlightDataEventDto dto = mapToDto(event);
      client.postFlightData(dto);
      log.info("Successfully posted flight data for {}", flightId);
    } catch (DownstreamApiException e) {
      log.error("Permanent downstream failure for {}: {}",
          flightId, e.getMessage(), e);
      throw new EventProcessingException(e.getMessage(), e);
    }
  }

  private FlightEvent parsePayload(Object payload) {
    if (payload == null) {
      throw new EventProcessingException("Received null payload");
    }
    if (payload instanceof FlightEvent event) {
      return event;  // Already deserialized by Glue Schema Registry
    }
    try {
      return objectMapper.readValue(
          payload.toString(), FlightEvent.class);
    } catch (JsonProcessingException e) {
      throw new EventProcessingException(
          "Failed to parse flight data payload: " + e.getMessage(), e);
    }
  }

  private FlightDataEventDto mapToDto(FlightEvent event) {
    FlightDataEventDto.FlightDataEventDtoBuilder builder =
        FlightDataEventDto.builder();
    if (event.getFlight() != null
        && event.getFlight().getKey() != null) {
      var key = event.getFlight().getKey();
      builder.fltNum(toStr(key.getFltNum()))
          .fltOrgDate(toStr(key.getFltOrgDate()))
          .depSta(toStr(key.getDepSta()));
      if (key.getAirlineCode() != null) {
        builder.airlineCodeIata(
            toStr(key.getAirlineCode().getIATA()));
      }
    }
    return builder.build();
  }

  private String extractFlightIdentifier(FlightEvent event) {
    if (event.getFlight() != null
        && event.getFlight().getKey() != null) {
      var key = event.getFlight().getKey();
      return "flt=" + toStr(key.getFltNum())
          + " org=" + toStr(key.getFltOrgDate())
          + " dep=" + toStr(key.getDepSta());
    }
    return "unknown-flight";
  }

  private String toStr(Object value) {
    return value != null ? value.toString() : null;
  }
}
```

> **Why `.toString()` via `toStr()`?** Avro `string` fields are `CharSequence` by default
> (typically `org.apache.avro.util.Utf8`), not `java.lang.String`. The helper converts safely
> and handles null values from nullable unions.

> **Why a separate DTO?** The Avro-generated class extends `SpecificRecordBase` which adds
> Avro-specific fields (`schema`, `specificData`) to Jackson serialization. Using it directly
> as a REST request body pollutes the JSON. The outbound DTO ensures a clean REST contract.

## Checkstyle Exclusion

Avro-generated code does not conform to Google Java Style. Exclude it in `pom.xml`:

```xml
<excludes>...,com/aa/opshub/avro/flight/**</excludes>
```

## Test Construction Pattern

The production schema has nullable-union fields **without explicit `"default": null`** in some
defining record occurrences. Avro builders (`ClassName.newBuilder()...build()`) validate all
fields and throw `AvroMissingFieldException` when a field lacks a default and is not set.

**In tests, use no-arg constructors + setters** instead of builders:

```java
private FlightEvent buildFlightEvent(String fltNum, String depSta,
    String fltOrgDate, String airlineIata) {
  AirlineCode airlineCode = new AirlineCode();
  airlineCode.setIATA(airlineIata);

  Key key = new Key();
  key.setFltNum(fltNum);
  key.setDepSta(depSta);
  key.setFltOrgDate(fltOrgDate);
  key.setAirlineCode(airlineCode);

  Flight flight = new Flight();
  flight.setKey(key);

  FlightEvent event = new FlightEvent();
  event.setFlight(flight);
  return event;
}
```

Constructors do not validate defaults, so they work regardless of schema default annotations.

## MQ Listeners

Amazon MQ (ActiveMQ) consumers do **not** use Avro schemas or the `avro-maven-plugin`.
MQ messages are plain text/JSON — use hand-written DTOs with `ObjectMapper` directly.
