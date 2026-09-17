# Testing Patterns — Learned Rules for Unit & Integration Tests

Rules and patterns learned from real test failures in NXOP FlightOps services.
Use when writing or reviewing any test file (`*Test.java`, `*IT.java`).

---

## 1. Avro Testing — NEVER use builders in unit tests

### Rule: Always use Mockito `@Mock` for Avro-generated classes — never `newBuilder()`

Avro schemas have many fields **without default values**. Builders require ALL non-nullable,
no-default fields to be set explicitly, making them impractical in tests.

```java
// WRONG — builder fails if any required field is missing
FlightEvent event = FlightEvent.newBuilder()
    .setFlight(...)
    .build(); // throws AvroRuntimeException — missing required fields
```

### Correct Pattern: Mock and stub only needed fields

```java
@ExtendWith(MockitoExtension.class)
class FlightDataHandlerTest {

  @Mock private FlightEvent mockFlightEvent;
  @Mock private Flight mockFlight;
  @Mock private Key mockKey;
  @Mock private AirlineCode mockAirlineCode;

  @Test
  void givenValidFlightEvent_whenHandle_thenProcessesCorrectly() {
    when(mockFlightEvent.getFlight()).thenReturn(mockFlight);
    when(mockFlight.getKey()).thenReturn(mockKey);
    when(mockKey.getFltNum()).thenReturn("1234");
    when(mockKey.getDepSta()).thenReturn("DFW");
    when(mockKey.getFltOrgDate()).thenReturn("2026-05-02");
    when(mockKey.getAirlineCode()).thenReturn(mockAirlineCode);
    when(mockAirlineCode.getIATA()).thenReturn("AA");
    // Act + Assert ...
  }
}
```

- **NEVER** modify `.avsc` files to add defaults for testing convenience.
- Stub only the getters your specific test exercises.

---

## 2. Kafka Listener Integration Tests — @EmbeddedKafka

### Rule: Always provide KafkaTemplate via @TestConfiguration

`nxop-flightops-common-kafka` excludes `KafkaAutoConfiguration` globally. Any test with
`@Autowired KafkaTemplate` without providing a bean fails with `UnsatisfiedDependencyException`.

### Rule: Never use @SpringBootTest(classes = ...) in @EmbeddedKafka tests

Specifying `classes` stops Spring from scanning inner `@TestConfiguration` classes — the
`KafkaTemplate` bean is silently skipped.

### Required Pattern

```java
@Tag("integration")
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {"${kafka.msk.topic.names}"}
)
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "kafka.msk.consumer.config.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "kafka.msk.consumer.config.security-protocol=PLAINTEXT",
    "spring.kafka.consumer.auto-offset-reset=earliest"
})
class FlightDataKafkaListenerIT {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @MockitoBean
  private FlightDataHandler flightDataHandler;

  @TestConfiguration
  static class TestKafkaProducerConfig {

    @Bean
    KafkaTemplate<String, String> kafkaTemplate(
        @Value("${spring.embedded.kafka.brokers}") String brokers) {
      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
  }

  @Test
  void givenValidMessage_whenPublishedToTopic_thenHandlerCalled() {
    String payload = "{\"flightNumber\":\"AA100\"}";
    kafkaTemplate.send("flight-event-aa-departure-arrival-avro", payload);
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() ->
            verify(flightDataHandler).handleMessage(payload));
  }
}
```

### @EmbeddedKafka IT Checklist

1. Inner `@TestConfiguration static class` providing `KafkaTemplate` — **mandatory**
2. `@TestPropertySource` uses `security-protocol=PLAINTEXT` — **no `sasl-*` entries**
3. Plain `@SpringBootTest` — **no `classes` attribute**
4. `@MockitoBean` — **not `@MockBean`** (removed in Boot 4)
5. `@DirtiesContext` — prevent embedded broker reuse across tests
