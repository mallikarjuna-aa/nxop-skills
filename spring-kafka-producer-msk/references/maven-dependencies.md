# Kafka Producer — Maven Dependencies

## Core Dependency

```xml
<dependency>
    <groupId>com.aa.nxop.flightops.common.kafka</groupId>
    <artifactId>nxop-flightops-common-kafka</artifactId>
    <version>1.0.0</version>
</dependency>
```

## AWS MSK Additional Dependencies

If your service connects to **AWS MSK** with IAM authentication and AWS Glue Schema Registry,
add the same dependencies documented in the `spring-kafka-consumer-msk` skill. These exclusions
are identical for both consumer and producer sides:

```xml
<!-- AWS MSK IAM Auth -->
<dependency>
    <groupId>software.amazon.msk</groupId>
    <artifactId>aws-msk-iam-auth</artifactId>
    <version>2.3.6</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
        </exclusion>
        <exclusion>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </exclusion>
    </exclusions>
</dependency>

<!-- AWS Glue Schema Registry -->
<dependency>
    <groupId>software.amazon.glue</groupId>
    <artifactId>schema-registry-serde</artifactId>
    <version>1.1.27</version>
    <exclusions>
        <exclusion>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>sdk-core</artifactId>
        </exclusion>
        <exclusion>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
        </exclusion>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
        </exclusion>
    </exclusions>
</dependency>

<!-- AWS SDK STS (required for IAM role assumption) -->
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>sts</artifactId>
    <version>2.44.9</version>
</dependency>
```

## Integration Test Dependencies (`@EmbeddedKafka`)

Same pair as consumer — always declare together:

```xml
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-library</artifactId>
    <version>2.13.16</version>
    <scope>test</scope>
</dependency>
```
