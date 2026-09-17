# Kafka Producer — Publisher Pattern

## Publisher Class

```java
// Replace package and class name to match your service and domain.
package com.aa.nxop.flightops.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

import com.aa.nxop.flightops.common.kafka.exception.EventProcessingException;
import com.aa.nxop.flightops.common.kafka.exception.TransientException;
import com.aa.nxop.flightops.common.kafka.producer.SyncPublishGateway;
import com.aa.nxop.flightops.config.ProducerKafkaConfig;

/**
 * Synchronous Kafka publisher for domain events.
 *
 * <p>Builds a {@link ProducerRecord} and delegates to {@link SyncPublishGateway#sendSync},
 * blocking until the broker acknowledges receipt or the delivery timeout elapses.
 *
 * <p>To switch to async publishing, replace {@link SyncPublishGateway} with
 * {@link com.aa.nxop.flightops.common.kafka.producer.AsyncPublishGateway} and call
 * {@code asyncPublishGateway.sendAsync(kafkaTemplate, pr, trackingId)}.
 */
@Slf4j
@Service
public class FlightEventKafkaPublisher { // Replace with your publisher class name

  private final KafkaTemplate<Object, Object> kafkaTemplate;
  private final SyncPublishGateway syncPublishGateway;
  private final ProducerKafkaConfig producerKafkaConfig;

  // Manual constructor required — @RequiredArgsConstructor does not propagate @Qualifier
  // from a field annotation to the generated constructor parameter.
  public FlightEventKafkaPublisher(
      @Qualifier("ProducerTemplate") KafkaTemplate<Object, Object> kafkaTemplate,
      SyncPublishGateway syncPublishGateway,
      ProducerKafkaConfig producerKafkaConfig) {
    this.kafkaTemplate = kafkaTemplate;
    this.syncPublishGateway = syncPublishGateway;
    this.producerKafkaConfig = producerKafkaConfig;
  }

  /**
   * Publishes an event to the specified Kafka topic synchronously.
   *
   * @param topic       target Kafka topic name — inject from config, do not hardcode
   * @param trackingId  correlation ID logged on success and failure
   * @param key         Kafka record key (may be null for round-robin partitioning)
   * @param value       Kafka record value (the domain payload)
   * @throws TransientException       on retriable broker/network/timeout failures
   * @throws EventProcessingException on non-retriable failures (record too large, auth error)
   */
  public void publish(
      final String topic,
      final String trackingId,
      final Object key,
      final Object value) throws TransientException, EventProcessingException {
    log.info("Publishing event to topic={} with trackingId={}", topic, trackingId);
    ProducerRecord<Object, Object> pr = new ProducerRecord<>(topic, key, value);
    syncPublishGateway.sendSync(
        kafkaTemplate, pr, trackingId, producerKafkaConfig.getDeliveryTimeoutMs());
  }
}
```

## Calling the Publisher from a Handler

```java
// Illustrative handler pattern for an AmazonMQ → Kafka flow.
package com.aa.nxop.flightops.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.aa.nxop.flightops.common.kafka.exception.EventProcessingException;
import com.aa.nxop.flightops.common.kafka.exception.TransientException;
import com.aa.nxop.flightops.producer.FlightEventKafkaPublisher;

@Slf4j
@Service
@RequiredArgsConstructor
public class FlightDataHandler {

  private final FlightEventKafkaPublisher kafkaPublisher;

  @Value("${kafka.msk.producer.topic.flight-events}") // Replace with your topic property key
  private String targetTopic;

  public void handleMessage(final Object message)
      throws TransientException, EventProcessingException {
    String trackingId = extractTrackingId(message);
    Object key = extractKey(message);
    Object payload = transform(message);
    kafkaPublisher.publish(targetTopic, trackingId, key, payload);
  }

  private String extractTrackingId(Object message) { return "unknown"; }
  private Object extractKey(Object message) { return null; }
  private Object transform(Object message) { return message; }
}
```

## Swapping to `AsyncPublishGateway` (Future Extensibility)

1. Replace `SyncPublishGateway` field/constructor param with `AsyncPublishGateway`.
2. Change internal call:
   ```java
   // Before (sync)
   syncPublishGateway.sendSync(kafkaTemplate, pr, trackingId, producerKafkaConfig.getDeliveryTimeoutMs());
   // After (async)
   asyncPublishGateway.sendAsync(kafkaTemplate, pr, trackingId);
   ```
3. `sendAsync()` returns `CompletableFuture<SendResult<Object, Object>>`.

> **Do not use `AsyncPublishGateway` in AmazonMQ listener context** — JMS pooled threads +
> async futures can exhaust the producer buffer, hide delivery failures, and complicate shutdown.
> Use `SyncPublishGateway` for MQ-to-Kafka bridges; reserve async for Kafka-to-Kafka fan-out.
