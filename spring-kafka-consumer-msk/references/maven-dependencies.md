# Kafka Consumer — Maven Dependencies

## Core Dependency

```xml
<dependency>
    <groupId>com.aa.nxop.flightops.common.kafka</groupId>
    <artifactId>nxop-flightops-common-kafka</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Apache Avro (Schema-Driven Code Generation)

Every Kafka consumer service must define its inbound message schema as an Avro `.avsc` file
under `src/main/resources/avroschema/`. The `avro-maven-plugin` generates a Java class from
the schema during `generate-sources`.

### Dependency

```xml
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>1.12.0</version>
</dependency>
```

### Build Plugin

Add to `<build><plugins>`:

```xml
<plugin>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-maven-plugin</artifactId>
    <version>1.12.0</version>
    <executions>
        <execution>
            <phase>generate-sources</phase>
            <goals>
                <goal>schema</goal>
            </goals>
            <configuration>
                <sourceDirectory>${basedir}/src/main/resources/avroschema/</sourceDirectory>
                <outputDirectory>${basedir}/target/generated-sources/java/</outputDirectory>
            </configuration>
        </execution>
    </executions>
</plugin>
```

> **Note:** The generated class extends `org.apache.avro.specific.SpecificRecordBase`.
> Do **not** use the generated class directly as a REST request body — Avro-specific fields
> pollute the JSON. Always map to a separate outbound DTO in the handler.

## AWS MSK Additional Dependencies

If your service connects to **AWS MSK** with IAM authentication and AWS Glue Schema Registry:

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

> **Why the exclusions?** `aws-msk-iam-auth` and `schema-registry-serde` both pull in `slf4j-simple`
> and `logback-classic` transitively, conflicting with NXOP's `spring-boot-starter-log4j2`.

## Integration Test Dependencies (`@EmbeddedKafka`)

**Both** must always be declared as a pair:

```xml
<!-- Embedded Kafka broker for IT tests -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
    <scope>test</scope>
</dependency>

<!--
  Pins scala-library to 2.13.x for the test classpath.
  schema-registry-serde pulls scala-library:2.12.10 at COMPILE scope via
  mbknor-jackson-jsonschema_2.12. Without this pin, EmbeddedKafkaKraftBroker
  fails with: NoClassDefFoundError: scala/$less$colon$less
-->
<dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-library</artifactId>
    <version>2.13.16</version>
    <scope>test</scope>
</dependency>
```
