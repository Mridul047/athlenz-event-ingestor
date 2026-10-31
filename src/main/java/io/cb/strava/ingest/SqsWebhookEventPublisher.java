package io.cb.strava.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Publishes webhook events to an SQS queue for asynchronous processing by a downstream processor.
 * This is the production implementation of {@link WebhookEventPublisher}.
 *
 * <p>Enqueuing is deliberately the <em>only</em> work the ingest function does for an event
 * delivery. Returning quickly lets us acknowledge Strava well within its timeout, while the heavier
 * work (OAuth, API calls, persistence) happens out of band.
 */
public final class SqsWebhookEventPublisher implements WebhookEventPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(SqsWebhookEventPublisher.class);

  private final SqsClient sqsClient;
  private final ObjectMapper objectMapper;
  private final String queueUrl;

  public SqsWebhookEventPublisher(SqsClient sqsClient, ObjectMapper objectMapper, String queueUrl) {
    if (queueUrl == null || queueUrl.isBlank()) {
      throw new IllegalStateException("EVENT_QUEUE_URL must be configured");
    }
    this.sqsClient = sqsClient;
    this.objectMapper = objectMapper;
    this.queueUrl = queueUrl;
  }

  @Override
  public void publish(StravaWebhookEvent event) {
    final String body;
    try {
      body = objectMapper.writeValueAsString(event);
    } catch (Exception e) {
      // Serialization of a validated event should never fail; treat as a
      // programming error rather than swallowing it.
      throw new IllegalStateException("Failed to serialize webhook event", e);
    }

    SendMessageRequest request =
        SendMessageRequest.builder()
            .queueUrl(queueUrl)
            .messageBody(body)
            // Carry the idempotency key as an attribute so the processor
            // (and anyone inspecting the queue) can dedupe without
            // re-parsing the body.
            .messageAttributes(
                Map.of(
                    "idempotencyKey",
                    MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(event.idempotencyKey())
                        .build()))
            .build();

    sqsClient.sendMessage(request);
    LOG.info(
        "Enqueued event objectId={} ownerId={} aspect={}",
        event.objectId(),
        event.ownerId(),
        event.aspectType());
  }
}
