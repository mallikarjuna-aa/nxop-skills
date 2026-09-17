# Kafka Consumer — Config Pattern (Two-Class)

## Class 1 — `ConsumerKafkaConfig.java` (pure property POJO)

No constructor, no Spring dependencies, no `@PostConstruct`. `buildProperties()` is package-private.
Always use `properties.setProperty()` — never `properties.put()`.

```java
// Replace class name, prefix, and inner fields to match your service's kafka.* properties
package com.aa.nxop.flightops.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "kafka.msk.consumer.config")
public class ConsumerKafkaConfig {

  private String bootstrapServers;
  private String securityProtocol;
  private String saslMechanism;
  private String saslJaasConfig;
  private String saslCallbackHandlerClass;
  private String region;
  private String offsetReset;
  private int requestTimeoutMs;
  private int metadataMaxAgeMs;
  private int connectionsMaxIdleMs;
  private int maxPollRecords;
  private int maxPollIntervalMs;
  private String partitionAssignmentStrategy;
  private String keyDeserializer;
  private String valueDeserializer;

  Properties buildProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", bootstrapServers);
    properties.setProperty("security.protocol", securityProtocol);
    // Only set SASL properties when the protocol actually requires them.
    // Kafka 4.x validates class-type config values even when SASL is not active.
    if (securityProtocol != null && securityProtocol.toUpperCase().startsWith("SASL")) {
      properties.setProperty("sasl.mechanism", saslMechanism);
      properties.setProperty("sasl.jaas.config", saslJaasConfig);
      properties.setProperty("sasl.client.callback.handler.class", saslCallbackHandlerClass);
    }
    properties.setProperty(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy);
    properties.setProperty(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
    properties.setProperty(
        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
    properties.setProperty(
        ConsumerConfig.METADATA_MAX_AGE_CONFIG, String.valueOf(metadataMaxAgeMs));
    properties.setProperty(
        ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(connectionsMaxIdleMs));
    properties.setProperty(
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
    properties.setProperty(
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollIntervalMs));
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    properties.setProperty(AWSSchemaRegistryConstants.AWS_REGION, region);
    properties.setProperty(
        AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.name());
    return properties;
  }
}
```

## Class 2 — `ConsumerKafkaInitializerConfig.java` (wiring class)

```java
// Replace package to match your service
package com.aa.nxop.flightops.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import com.aa.nxop.flightops.common.kafka.config.ConsumerKafkaCommonConfig;
import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class ConsumerKafkaInitializerConfig {

  private final ConsumerKafkaConfig consumerKafkaConfig;
  private final ConsumerKafkaCommonConfig kafkaCommonConfig;

  @PostConstruct
  public void applyKafkaProperties() {
    kafkaCommonConfig.applyToCommon(consumerKafkaConfig.buildProperties());
  }
}
```

Enable `ConsumerKafkaConfig` in your main application class:

```java
@EnableConfigurationProperties(ConsumerKafkaConfig.class)
```

> `ConsumerKafkaCommonConfig` and all other beans from `nxop-flightops-common-kafka` are
> registered automatically via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.

## application.yml — Consumer Block

```yaml
spring:
  autoconfigure:
    exclude: org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration

kafka:
  msk:
    consumer:
      config:
        bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
        security-protocol: SASL_SSL
        sasl-mechanism: AWS_MSK_IAM
        sasl-jaas-config: ${KAFKA_SASL_JAAS_CONFIG}
        sasl-callback-handler-class: software.amazon.msk.auth.iam.IAMClientCallbackHandler
        partition-assignment-strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
        request-timeout-ms: 60000
        metadata-max-age-ms: 180000
        connections-max-idle-ms: 60000
        max-poll-records: 50
        max-poll-interval-ms: 660000
        offset-reset: earliest
        region: ${AWS_REGION:us-east-1}
        key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
        value-deserializer: com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
      topic:
        names: ${KAFKA_TOPIC_NAMES}
        concurrency: ${KAFKA_TOPIC_CONCURRENCY:1}
        clientId: ${KAFKA_CLIENT_ID}
        groupId: ${KAFKA_GROUP_ID}
```

> **Tuning notes:**
> - `max-poll-interval-ms: 660000` — allow long processing before rebalance trigger.
> - `max-poll-records: 50` — moderate batch size; tune based on record processing time.
> - `offset-reset: earliest` — replay from start on new consumer group.

## Listener Container Factory (Reference Only)

`nxop-flightops-common-kafka` provides `ConsumerKafkaConfiguration`, which auto-configures both
`ConsumerFactory` and a `KafkaListenerContainerFactory` bean named **`ConsumerKafkaConnectionFactory`**
with `AckMode.MANUAL`, 30 s auth-exception retry interval, and `DefaultErrorHandler` backed by
`ContainerPausingRecoverer`.

**Never** define your own `KafkaListenerContainerFactory` or `ConsumerFactory` bean. Reference
`ConsumerKafkaConnectionFactory` by name in every `@KafkaListener`.
