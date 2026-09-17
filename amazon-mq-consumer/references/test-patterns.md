# Amazon MQ Consumer — Test Patterns

## Unit Test — Listener in Isolation

```java
// Replace class names, mock types, payload, and verify calls to match your listener and handler
package com.aa.nxop.flightops.consumer;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aa.nxop.flightops.common.mq.exception.MessageProcessingException;
import com.aa.nxop.flightops.handler.FlightDataHandler;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FlightDataJmsListenerTest { // Replace with your listener test class name

  private static final String MESSAGE_ID = "ID:test-message-001";
  private static final String PAYLOAD = "{\"flightNumber\":\"AA100\"}"; // Replace

  @Mock
  private FlightDataHandler flightDataHandler; // Replace with your handler type

  @Mock
  private TextMessage textMessage;

  @Mock
  private Session session;

  private FlightDataJmsListener listener; // Replace with your listener type

  // @BeforeEach only constructs the listener — do NOT add when() stubs here.
  // Stubs must be co-located with the test that needs them. Placing them in
  // @BeforeEach when some tests use a different mock (e.g. nonTextMessage) will
  // trigger Mockito STRICT_STUBS UnnecessaryStubbing and fail the build.
  @BeforeEach
  void setUp() {
    listener = new FlightDataJmsListener(flightDataHandler);
  }

  @Test
  void givenValidTextMessage_whenOnMessage_thenHandlerCalledAndMessageAcknowledged()
      throws JMSException {
    when(textMessage.getJMSMessageID()).thenReturn(MESSAGE_ID);
    when(textMessage.getText()).thenReturn(PAYLOAD);

    listener.onMessage(textMessage, session);

    verify(flightDataHandler).handle(PAYLOAD); // Replace method name to match your handler
    verify(textMessage).acknowledge();
  }

  @Test
  void givenMessageProcessingException_whenOnMessage_thenMessageAcknowledged()
      throws JMSException {
    when(textMessage.getJMSMessageID()).thenReturn(MESSAGE_ID);
    when(textMessage.getText()).thenReturn(PAYLOAD);
    doThrow(new MessageProcessingException("downstream failure"))
        .when(flightDataHandler).handle(anyString());

    assertThatCode(() -> listener.onMessage(textMessage, session)).doesNotThrowAnyException();

    // Non-retryable: message IS acknowledged to prevent infinite redelivery
    verify(textMessage).acknowledge();
    verify(session, never()).recover();
  }

  @Test
  void givenRuntimeException_whenOnMessage_thenSessionRecovered()
      throws JMSException {
    when(textMessage.getJMSMessageID()).thenReturn(MESSAGE_ID);
    when(textMessage.getText()).thenReturn(PAYLOAD);
    doThrow(new RuntimeException("unexpected"))
        .when(flightDataHandler).handle(anyString());

    assertThatCode(() -> listener.onMessage(textMessage, session)).doesNotThrowAnyException();

    // Retryable/unexpected: message NOT acknowledged, session recovered for redelivery
    verify(textMessage, never()).acknowledge();
    verify(session).recover();
  }

  @Test
  void givenNonTextMessage_whenOnMessage_thenHandlerNotCalledAndSessionRecovered()
      throws JMSException {
    // Uses a plain Message mock — textMessage stubs must NOT be in @BeforeEach
    // or Mockito STRICT_STUBS will flag them as UnnecessaryStubbing.
    jakarta.jms.Message nonTextMessage = org.mockito.Mockito.mock(jakarta.jms.Message.class);
    when(nonTextMessage.getJMSMessageID()).thenReturn(MESSAGE_ID);

    listener.onMessage(nonTextMessage, session);

    verify(flightDataHandler, never()).handle(anyString());
    verify(nonTextMessage, never()).acknowledge();
    verify(session).recover();
  }
}
```

## Integration Test — End-to-End with Embedded ActiveMQ

```java
// Replace class names, queue name, payload, and verify call to match your listener and handler
package com.aa.nxop.flightops.consumer;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;

import com.aa.nxop.flightops.handler.FlightDataHandler;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@Tag("integration")
@SpringBootTest
@DirtiesContext
@TestPropertySource(properties = {
    // Embedded ActiveMQ VM broker — no external broker required
    "activemq.broker-url=vm://localhost?broker.persistent=false&create=true",
    "activemq.username=",
    "activemq.password=",
    "activemq.queue.name=test.flight.events" // Replace with your test queue name
})
class FlightDataJmsListenerIT { // Replace with your integration test class name, always ending in IT

  @Autowired
  private JmsTemplate jmsTemplate;

  @MockitoBean
  private FlightDataHandler flightDataHandler; // Replace with your handler type

  // If this service also depends on nxop-flightops-common-kafka but does NOT configure a
  // Kafka consumer (producer-only), mock ConsumerFactory to prevent ConsumerKafkaConfiguration
  // from throwing IllegalStateException at startup (it requires applyToCommon() to be called).
  @MockitoBean
  private ConsumerFactory<Object, Object> consumerFactory;

  @Test
  void givenValidMessage_whenSentToQueue_thenHandlerCalled() {
    String payload = "{\"flightNumber\":\"AA100\"}"; // Replace with a representative payload

    // Use send+MessageCreator to bypass MappingJackson2MessageConverter — it is auto-configured
    // on JmsTemplate when an ApplicationContext-scoped MessageConverter bean exists, and it
    // JSON-encodes a Java String a second time (double-encodes). The listener expects a raw
    // TextMessage. jmsTemplate.send(dest, session::createTextMessage) bypasses the converter
    // entirely, exactly as a real Amazon MQ producer would.
    jmsTemplate.send("test.flight.events",
        session -> session.createTextMessage(payload)); // Replace with your test queue name

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> verify(flightDataHandler).handle(payload)); // Replace method/args
  }
}
```

## Integration Test — Cross-Message Ack Isolation

Proves that a failed message is **not** silently acknowledged when the next message
succeeds. With `CLIENT_ACKNOWLEDGE`, `message.acknowledge()` acks all prior unacked
messages on the same session — so if the session isn't recovered after a failure, the
next success would silently cover the poison message. This IT verifies isolation.

```java
package com.aa.nxop.flightops.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.aa.nxop.flightops.handler.FlightDataHandler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Proves cross-message ack isolation under CLIENT_ACKNOWLEDGE.
 *
 * <p>Scenario: Message A triggers a transient failure and is redelivered via
 * session.recover(). Message B arrives and succeeds. This test verifies that
 * message A is NOT silently acknowledged by B's success — it must be redelivered
 * independently per the RedeliveryPolicy.
 */
@Tag("integration")
@SpringBootTest
@DirtiesContext
@TestPropertySource(properties = {
    "activemq.broker-url=vm://localhost?broker.persistent=false&create=true",
    "activemq.username=",
    "activemq.password=",
    "activemq.queue.name=test.ack.isolation",
    // Minimal redelivery for fast test execution
    "activemq.redelivery.maximum-redeliveries=2",
    "activemq.redelivery.initial-redelivery-delay=100",
    "activemq.redelivery.use-exponential-back-off=false",
    "activemq.redelivery.back-off-multiplier=1",
    "activemq.redelivery.maximum-redelivery-delay=200",
    "activemq.listener.concurrency=1"
})
class AckIsolationIT {

  @Autowired
  private JmsTemplate jmsTemplate;

  @MockitoBean
  private FlightDataHandler flightDataHandler;

  @MockitoBean
  private ConsumerFactory<Object, Object> consumerFactory;

  @Test
  void givenTransientFailure_whenNextMessageSucceeds_thenFailedMessageIsRedelivered() {
    String poisonPayload = "{\"flight\":\"POISON\"}";
    String goodPayload = "{\"flight\":\"GOOD\"}";

    // First call (poison): throw transient → session.recover() → redelivered
    // Second call (poison redelivery): succeed
    // Third call (good): succeed
    List<String> received = new ArrayList<>();
    doThrow(new RuntimeException("transient failure"))
        .doAnswer(invocation -> {
          received.add(invocation.getArgument(0));
          return null;
        })
        .doAnswer(invocation -> {
          received.add(invocation.getArgument(0));
          return null;
        })
        .when(flightDataHandler).handle(anyString());

    // Send poison first, then good
    jmsTemplate.send("test.ack.isolation",
        session -> session.createTextMessage(poisonPayload));
    jmsTemplate.send("test.ack.isolation",
        session -> session.createTextMessage(goodPayload));

    // Wait for both messages to be processed (poison redelivered + good)
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> {
          ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
          // At least 3 calls: poison (fail), poison (redeliver succeed), good (succeed)
          verify(flightDataHandler, times(3)).handle(captor.capture());

          List<String> allCalls = captor.getAllValues();
          // First call: poison (failed)
          assertThat(allCalls.get(0)).isEqualTo(poisonPayload);
          // Second call: poison redelivered (session.recover triggered redelivery)
          assertThat(allCalls.get(1)).isEqualTo(poisonPayload);
          // Third call: good message processed independently
          assertThat(allCalls.get(2)).isEqualTo(goodPayload);
        });

    // KEY ASSERTION: The poison message WAS redelivered — proving it was NOT
    // silently acknowledged when the good message succeeded. If ack isolation
    // were broken (pooled session + no recover), the poison would disappear
    // after the good message's acknowledge() covered it.
    assertThat(received).containsExactly(poisonPayload, goodPayload);
  }
}
```
