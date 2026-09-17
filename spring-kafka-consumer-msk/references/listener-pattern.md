# Kafka Consumer — Listener Pattern

## Timestamp Formatting

```java
private static final DateTimeFormatter TIMESTAMP_FORMATTER =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

// Usage:
TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(record.timestamp()))
```

## Listener Class

```java
// Replace class name, injected service type/name, topic property key, and type parameters
package com.aa.nxop.flightops.kafka;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.aa.nxop.flightops.common.kafka.exception.EventProcessingException;
import com.aa.nxop.flightops.common.kafka.exception.TransientException;

@Slf4j
@Component
@RequiredArgsConstructor
public class FlightEventKafkaListener { // Replace with your listener class name

	private static final DateTimeFormatter TIMESTAMP_FORMATTER =
		DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

	private final FlightDataHandler flightDataHandler; // Replace with your handler type

	@KafkaListener(topics = "#{'${kafka.msk.topic.names}'.split(',')}",
            containerFactory = "ConsumerKafkaConnectionFactory",
            clientIdPrefix = "#{'${kafka.msk.topic.clientId}'}",
            groupId = "#{'${kafka.msk.topic.groupId}'}",
            concurrency = "#{'${kafka.msk.topic.concurrency}'}")
	public void listen(ConsumerRecord<Object, Object> inputEvent, Acknowledgment ack) {
		if (inputEvent == null) {
			log.warn("Received null ConsumerRecord — skipping");
			return;
		}

		String utcTimestamp = TIMESTAMP_FORMATTER.format(
			Instant.ofEpochMilli(inputEvent.timestamp()));
		log.info("Received record from topic={}, partition={}, offset={}, timestamp={}",
			inputEvent.topic(), inputEvent.partition(), inputEvent.offset(), utcTimestamp);

		try {
			flightDataHandler.handleMessage(inputEvent.value()); // Replace with your handler
			ack.acknowledge();
		} catch (final EventProcessingException e) {
			// Non-retryable: acknowledge to prevent poison-pill redelivery loop.
			// TODO: Implement DLT publishing — write the raw inputEvent bytes to a
			// dedicated Dead Letter Topic before acknowledging so the failed record
			// is preserved for investigation and replay.
			log.error("Non-retryable error for record topic={}, partition={}, offset={} "
				+ "— acknowledging to skip. Cause: {}",
				inputEvent.topic(), inputEvent.partition(), inputEvent.offset(),
				e.getMessage(), e);
			ack.acknowledge();
		} catch (final TransientException e) {
			log.error("Retryable failure for record topic={}, partition={}, offset={} "
				+ "— rethrowing for container retry. Cause: {}",
				inputEvent.topic(), inputEvent.partition(), inputEvent.offset(),
				e.getMessage(), e);
			throw e;
		}
	}
}
```
