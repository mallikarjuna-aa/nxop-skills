# Kafka Producer — Config Pattern

## Class 1 — `ProducerKafkaConfig.java` (pure property POJO)

No constructor, no Spring dependencies, no `@PostConstruct`. `buildProperties()` is package-private.
Always use `properties.setProperty()` — never `properties.put()`.

```java
// Replace class name, prefix, and fields to match your service's kafka.* properties.
package com.aa.nxop.flightops.config;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "kafka.msk.producer.config") // Replace prefix if needed
public class ProducerKafkaConfig {

  private String bootstrapServers;
  private String securityProtocol;
  private String saslMechanism;
  private String saslJaasConfig;
  private String saslCallbackHandlerClass;
  private String region;
  private String keySerializer;
  private String valueSerializer;
  private int maxRequestSize;
  private int requestTimeoutMs;
  private int deliveryTimeoutMs;
  private boolean enableIdempotence;
  private String compressionType;
  private int metadataMaxAgeMs;
  private int connectionsMaxIdleMs;
  private int lingerMs;
  private int batchSize;
  private long bufferMemory;
  private int maxBlockMs;

  Properties buildProperties() {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty("security.protocol", securityProtocol);
    // Only set SASL properties when the protocol actually requires them.
    // Kafka 4.x validates class-type config values regardless of the active protocol.
    if (securityProtocol != null && securityProtocol.toUpperCase().startsWith("SASL")) {
      properties.setProperty("sasl.mechanism", saslMechanism);
      properties.setProperty("sasl.jaas.config", saslJaasConfig);
      properties.setProperty("sasl.client.callback.handler.class", saslCallbackHandlerClass);
    }
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
    properties.setProperty(
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(maxRequestSize));
    properties.setProperty(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
    properties.setProperty(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(deliveryTimeoutMs));
    properties.setProperty(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(enableIdempotence));
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
    properties.setProperty(
        ProducerConfig.METADATA_MAX_AGE_CONFIG, String.valueOf(metadataMaxAgeMs));
    properties.setProperty(
        ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(connectionsMaxIdleMs));
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(lingerMs));
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
    properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferMemory));
    properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(maxBlockMs));
    properties.setProperty(AWSSchemaRegistryConstants.AWS_REGION, region);
    return properties;
  }
}
```

## Class 2 — `ProducerKafkaInitializerConfig.java` (wiring class)

```java
// Replace package to match your service
package com.aa.nxop.flightops.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import com.aa.nxop.flightops.common.kafka.config.ProducerKafkaCommonConfig;
import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class ProducerKafkaInitializerConfig {

  private final ProducerKafkaConfig producerKafkaConfig;
  private final ProducerKafkaCommonConfig producerKafkaCommonConfig;

  @PostConstruct
  public void applyKafkaProperties() {
    producerKafkaCommonConfig.applyToProducer(producerKafkaConfig.buildProperties());
  }
}
```

Enable in main application class:

```java
@EnableConfigurationProperties({ConsumerKafkaConfig.class, ProducerKafkaConfig.class})
```

## `ProducerKafkaConfiguration` (from nxop-flightops-common-kafka — reference only)

`ProducerKafkaConfiguration` creates `ProducerFactory` and `KafkaTemplate` bean named
**`"ProducerTemplate"`**. Activated by `@ConditionalOnProperty(prefix = "kafka.msk.producer.config",
name = "bootstrap-servers")`. Only usable after `applyToProducer()` has been called.

## application.yml — Producer Block

```yaml
spring:
  autoconfigure:
    exclude: org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration

kafka:
  msk:
    producer:
      config:
        bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
        security-protocol: SASL_SSL
        sasl-mechanism: AWS_MSK_IAM
        sasl-jaas-config: ${KAFKA_SASL_JAAS_CONFIG}
        sasl-callback-handler-class: software.amazon.msk.auth.iam.IAMClientCallbackHandler
        region: ${AWS_REGION:us-east-1}
        key-serializer: org.apache.kafka.common.serialization.StringSerializer
        value-serializer: com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
        max-request-size: 900000
        request-timeout-ms: 60000
        delivery-timeout-ms: 180025
        enable-idempotence: false
        compression-type: none
        metadata-max-age-ms: 180000
        connections-max-idle-ms: 60000
        linger-ms: 25
        batch-size: 220000
        buffer-memory: 67108864
        max-block-ms: 60000
      topic:
        flight-events: ${KAFKA_PRODUCER_TOPIC_FLIGHT_EVENTS}
```

> **Tuning notes:**
> - `delivery-timeout-ms: 180025` — pass to `SyncPublishGateway.sendSync()` as `timeoutMs`.
> - `enable-idempotence: false` — set `true` + `acks=all` for exactly-once semantics.
> - `linger-ms: 25` + `batch-size: 220000` — moderate-throughput batching. Set `linger-ms: 0`
>   for one-at-a-time publishing in MQ listeners.
