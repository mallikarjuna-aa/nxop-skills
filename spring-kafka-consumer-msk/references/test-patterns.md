# Kafka Consumer — Test Patterns

## Unit Test — Listener in Isolation

Construct the listener directly and call `listen()` with a fabricated `ConsumerRecord`.
This validates delegation logic and logging without any Kafka infrastructure.

```java
// Replace class names, mock types, topic name, payload, and verify call to match your listener and service
package com.aa.nxop.flightops.kafka;

import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import com.aa.nxop.flightops.exception.EventProcessingException;
import com.aa.nxop.flightops.exception.TransientException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class FlightEventKafkaListenerTest { // Replace with your listener test class name

  @Mock
  private FlightDataHandler flightDataHandler; // Replace with your handler type

  @Mock
  private Acknowledgment ack;

  @InjectMocks
  private FlightEventKafkaListener listener; // Replace with your listener type

  private ConsumerRecord<Object, Object> buildRecord(String payload) {
    return new ConsumerRecord<>(
        "flight-events", // Replace with your topic name
        0, 42L,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME,
        -1, -1,
        "key-1", payload,
        new RecordHeaders(),
        Optional.empty()
    );
  }

  @Test
  void givenValidConsumerRecord_whenListen_thenHandlerCalledAndAcknowledged() {
    String payload = "{\"flightNumber\":\"AA100\"}"; // Replace with representative payload
    ConsumerRecord<Object, Object> record = buildRecord(payload);

    listener.listen(record, ack);

    verify(flightDataHandler).handleMessage(payload); // Replace method name/args
    verify(ack).acknowledge();
  }

  @Test
  void givenNullRecord_whenListen_thenSkippedWithoutSideEffects() {
    listener.listen(null, ack);

    verifyNoInteractions(flightDataHandler);
    verify(ack, never()).acknowledge();
  }

  @Test
  void givenNonRetryableException_whenListen_thenAcknowledgedAndExceptionSwallowed() {
    String payload = "{\"bad\":\"data\"}";
    ConsumerRecord<Object, Object> record = buildRecord(payload);
    doThrow(new EventProcessingException("parse failure"))
        .when(flightDataHandler).handleMessage(payload);

    listener.listen(record, ack);

    verify(ack).acknowledge();
  }

  @Test
  void givenRetryableException_whenListen_thenRethrown() {
    String payload = "{\"flightNumber\":\"AA100\"}";
    ConsumerRecord<Object, Object> record = buildRecord(payload);
    doThrow(new TransientException("broker timeout"))
        .when(flightDataHandler).handleMessage(payload);

    assertThatThrownBy(() -> listener.listen(record, ack))
        .isInstanceOf(TransientException.class);
    verify(ack, never()).acknowledge();
  }
}
```

## Integration Test — Happy Path with `@EmbeddedKafka`

### Critical Rules

- **`@TestConfiguration` inner class is mandatory** — `nxop-flightops-common-kafka` excludes
  `KafkaAutoConfiguration`, so no `KafkaTemplate` bean is auto-configured. Provide one explicitly.
- **Use plain `@SpringBootTest`** (no `classes` attribute) — specifying `classes` prevents Spring
  from scanning the test class for inner `@TestConfiguration`.
- **Set `security-protocol=PLAINTEXT`** in `@TestPropertySource` and omit all `sasl-*` properties.
- **Use `Duration.ofSeconds(30)` minimum** for Awaitility `atMost()` — KRaft broker rebalance
  takes 15–20 s on first startup.
- **Always declare `spring-kafka-test` and `scala-library:2.13.16:test` together** in `pom.xml`.

```java
// Replace class names, topic names, property keys, payload, KafkaTemplate type params, and verify call
package com.aa.nxop.flightops.kafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;

@Tag("integration")
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
  partitions = 1,
  topics = {"flight-events"} // Replace with your topic name(s)
)
@TestPropertySource(properties = {
  "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
  "spring.kafka.consumer.auto-offset-reset=earliest",
  "kafka.topics.flight-events=flight-events" // Replace key/value with your topic property
})
class FlightEventKafkaListenerIT { // Replace with your IT class name, always ending in IT

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate; // Replace type params

  @MockitoBean
  private FlightDataHandler flightDataHandler; // Replace with your handler type

  // KafkaAutoConfiguration is excluded by nxop-flightops-common-kafka, so no KafkaTemplate
  // bean is auto-configured. Provide one explicitly via @TestConfiguration.
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
    String payload = "{\"flightNumber\":\"AA100\"}"; // Replace with representative payload

    kafkaTemplate.send("flight-events", payload); // Replace topic name

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() ->
            verify(flightDataHandler).handleMessage(payload)); // Replace verify call
  }
}
```
