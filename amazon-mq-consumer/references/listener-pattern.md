# Amazon MQ Listener Pattern

## Exception Classes — Provided by `nxop-flightops-common-mq`

> **Library-provided.** `MessageProcessingException` and `TransientMessageException` are
> provided by `nxop-flightops-common-mq` in the
> `com.aa.nxop.flightops.common.mq.exception` package. Do **not** create per-service
> copies. Import them directly from the library.

| Exception | Package | Listener Action |
|---|---|---|
| `MessageProcessingException` | `com.aa.nxop.flightops.common.mq.exception` | Acknowledge — non-retryable |
| `TransientMessageException` | `com.aa.nxop.flightops.common.mq.exception` | `session.recover()` — retryable |

## Listener Class

```java
// Replace class name, injected handler type/name, and queue property key
package com.aa.nxop.flightops.consumer;

import com.aa.nxop.flightops.common.mq.exception.MessageProcessingException;
import com.aa.nxop.flightops.common.mq.observability.MdcMessageTracker;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FlightDataJmsListener { // Replace with your listener class name

  private final FlightDataHandler flightDataHandler; // Replace with your handler type

  @JmsListener(
      destination = "${activemq.queue.name}", // Replace property key if different
      containerFactory = "jmsListenerContainerFactory")
  public void onMessage(Message message, Session session) {
    MdcMessageTracker.put(message);
    String messageId = MdcMessageTracker.resolveMessageId(message);
    log.info("Received JMS message [id={}]", messageId);

    try {
      String payload = extractPayload(message, messageId);
      if (payload == null) {
        log.error("Message [id={}] has unsupported type {} — calling session.recover() "
            + "for broker redelivery", messageId, message.getClass().getSimpleName());
        recoverSession(session, messageId);
        return;
      }

      flightDataHandler.handle(payload); // Replace with your handler method call
      message.acknowledge();
      log.info("Message [id={}] acknowledged successfully", messageId);
    } catch (MessageProcessingException e) {
      // Non-retryable: acknowledge to prevent infinite redelivery loop.
      // TODO: Implement custom DLQ handling — publish the failed message to a
      // dedicated dead-letter queue/topic for investigation and replay before
      // acknowledging.
      log.error("Non-retryable error for message [id={}]: {} "
          + "— acknowledging to prevent redelivery loop",
          messageId, e.getMessage(), e);
      if (!safeAcknowledge(message, messageId)) {
        recoverSession(session, messageId);
      }
    } catch (RuntimeException e) {
      // Retryable / unexpected: do NOT acknowledge. Call session.recover() to trigger
      // broker redelivery per the configured RedeliveryPolicy. After maximumRedeliveries
      // exhausted, the broker routes the message to its Dead-Letter Queue — but ONLY if
      // the broker has an individualDeadLetterStrategy configured for this queue.
      // Without it, exhausted messages go to a shared ActiveMQ.DLQ (or are lost).
      // See: amazon-mq-config-pattern.instructions.md § "RedeliveryPolicy ≠ Broker DLQ"
      log.error("Retryable/unexpected error for message [id={}]: {} "
          + "— calling session.recover() for broker redelivery",
          messageId, e.getMessage(), e);
      recoverSession(session, messageId);
    } catch (JMSException e) {
      log.error("JMSException while acknowledging message [id={}]", messageId, e);
      recoverSession(session, messageId);
    } finally {
      MdcMessageTracker.clear();
    }
  }

  private boolean safeAcknowledge(Message message, String messageId) {
    try {
      message.acknowledge();
      return true;
    } catch (JMSException ackEx) {
      log.error("Failed to acknowledge message [id={}] after non-retryable error "
          + "— recovering session to prevent silent message loss", messageId, ackEx);
      return false;
    }
  }

  private void recoverSession(Session session, String messageId) {
    try {
      session.recover();
    } catch (JMSException e) {
      log.error("Failed to recover session for message [id={}]", messageId, e);
    }
  }

  private String extractPayload(Message message, String messageId) {
    if (message instanceof TextMessage textMessage) {
      try {
        return textMessage.getText();
      } catch (JMSException e) {
        log.error("Failed to extract text payload from message [id={}]", messageId, e);
        return null;
      }
    }
    return null;
  }
}
```
