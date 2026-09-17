# Kafka Producer — Test Patterns

## Unit Test — Publisher in Isolation

```java
// Replace class names, topic, trackingId, key, value to match your domain.
package com.aa.nxop.flightops.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.aa.nxop.flightops.common.kafka.exception.EventProcessingException;
import com.aa.nxop.flightops.common.kafka.exception.TransientException;
import com.aa.nxop.flightops.common.kafka.producer.SyncPublishGateway;
import com.aa.nxop.flightops.config.ProducerKafkaConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FlightEventKafkaPublisherTest { // Replace with your publisher test class name

  @Mock
  private KafkaTemplate<Object, Object> kafkaTemplate;

  @Mock
  private SyncPublishGateway syncPublishGateway;

  @Mock
  private ProducerKafkaConfig producerKafkaConfig;

  private FlightEventKafkaPublisher publisher;

  @BeforeEach
  void setUp() {
    when(producerKafkaConfig.getDeliveryTimeoutMs()).thenReturn(180025);
    publisher = new FlightEventKafkaPublisher(
        kafkaTemplate, syncPublishGateway, producerKafkaConfig);
  }

  @Test
  void givenValidPayload_whenPublish_thenSendSyncCalledWithCorrectRecord()
      throws TransientException, EventProcessingException {
    String topic = "flight-events";
    String trackingId = "AA100";
    String key = "key-1";
    String value = "{\"flightNumber\":\"AA100\"}";

    publisher.publish(topic, trackingId, key, value);

    ArgumentCaptor<ProducerRecord<Object, Object>> recordCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(syncPublishGateway).sendSync(
        eq(kafkaTemplate), recordCaptor.capture(), eq(trackingId), eq(180025L));
    assertThat(recordCaptor.getValue().topic()).isEqualTo(topic);
    assertThat(recordCaptor.getValue().key()).isEqualTo(key);
    assertThat(recordCaptor.getValue().value()).isEqualTo(value);
  }

  @Test
  void givenTransientException_whenPublish_thenPropagated()
      throws TransientException, EventProcessingException {
    doThrow(new TransientException("broker timeout", null))
        .when(syncPublishGateway).sendSync(any(), any(), any(), anyLong());

    assertThatThrownBy(() ->
        publisher.publish("flight-events", "AA100", "key-1", "payload"))
        .isInstanceOf(TransientException.class)
        .hasMessageContaining("broker timeout");
  }

  @Test
  void givenEventProcessingException_whenPublish_thenPropagated()
      throws TransientException, EventProcessingException {
    doThrow(new EventProcessingException("record too large", null))
        .when(syncPublishGateway).sendSync(any(), any(), any(), anyLong());

    assertThatThrownBy(() ->
        publisher.publish("flight-events", "AA100", "key-1", "oversized-payload"))
        .isInstanceOf(EventProcessingException.class)
        .hasMessageContaining("record too large");
  }
}
```

## Integration Test — Publisher with `@EmbeddedKafka`

No inner `@TestConfiguration` needed — the publisher already holds the `ProducerTemplate` bean.
Override bootstrap-servers and serializers via `@TestPropertySource`.

```java
// Replace class names, topic, property keys, payload, and consumer group.
package com.aa.nxop.flightops.producer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {"flight-events"} // Replace with your target topic
)
@TestPropertySource(properties = {
    "kafka.msk.producer.config.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "kafka.msk.producer.config.security-protocol=PLAINTEXT",
    // Do NOT add sasl-* properties — PLAINTEXT + Kafka 4.x rejects them
    "kafka.msk.producer.config.key-serializer="
        + "org.apache.kafka.common.serialization.StringSerializer",
    "kafka.msk.producer.config.value-serializer="
        + "org.apache.kafka.common.serialization.StringSerializer",
    "kafka.msk.producer.topic.flight-events=flight-events"
})
class FlightEventKafkaPublisherIT { // Replace with your IT class name

  @Autowired
  private FlightEventKafkaPublisher publisher;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafka;

  private Consumer<String, String> testConsumer;

  @BeforeEach
  void setUp() {
    Map<String, Object> consumerProps =
        KafkaTestUtils.consumerProps("test-group", "true", embeddedKafka);
    consumerProps.put("key.deserializer", StringDeserializer.class);
    consumerProps.put("value.deserializer", StringDeserializer.class);
    testConsumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps)
        .createConsumer();
    testConsumer.subscribe(Collections.singletonList("flight-events"));
  }

  @AfterEach
  void tearDown() {
    testConsumer.close();
  }

  @Test
  void givenValidPayload_whenPublish_thenMessageArrivesOnKafkaTopic() throws Exception {
    String payload = "{\"flightNumber\":\"AA100\"}";
    String trackingId = "AA100";
    String key = "key-1";

    publisher.publish("flight-events", trackingId, key, payload);

    ConsumerRecord<String, String> received =
        KafkaTestUtils.getSingleRecord(testConsumer, "flight-events", Duration.ofSeconds(30));
    assertThat(received.value()).isEqualTo(payload);
    assertThat(received.key()).isEqualTo(key);
  }
}
```
