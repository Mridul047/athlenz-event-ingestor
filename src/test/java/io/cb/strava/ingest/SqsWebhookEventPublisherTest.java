package io.cb.strava.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/** Unit tests for the SQS publisher. The SQS client is mocked, so no AWS calls are made. */
class SqsWebhookEventPublisherTest {

  private static final String QUEUE_URL =
      "https://sqs.eu-west-1.amazonaws.com/123456789012/strava-webhook-events";

  private SqsClient sqsClient;
  private SqsWebhookEventPublisher publisher;

  @BeforeEach
  void setUp() {
    sqsClient = mock(SqsClient.class);
    when(sqsClient.sendMessage(any(SendMessageRequest.class)))
        .thenReturn(SendMessageResponse.builder().messageId("msg-1").build());

    publisher = new SqsWebhookEventPublisher(sqsClient, new ObjectMapper(), QUEUE_URL);
  }

  @Test
  void sendsSerializedEventToConfiguredQueueWithIdempotencyAttribute() {
    StravaWebhookEvent event =
        new StravaWebhookEvent(
            StravaWebhookEvent.ASPECT_CREATE,
            1549560669L,
            1360128428L,
            StravaWebhookEvent.OBJECT_TYPE_ACTIVITY,
            134815L,
            120475L);

    publisher.publish(event);

    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(sqsClient).sendMessage(captor.capture());

    SendMessageRequest sent = captor.getValue();
    assertEquals(QUEUE_URL, sent.queueUrl());
    assertTrue(sent.messageBody().contains("1360128428"));
    assertTrue(
        sent.messageAttributes().containsKey("idempotencyKey"),
        "message must carry an idempotencyKey attribute");
    assertEquals(
        event.idempotencyKey(), sent.messageAttributes().get("idempotencyKey").stringValue());
  }

  @Test
  void rejectsBlankQueueUrlAtConstruction() {
    assertThrows(
        IllegalStateException.class,
        () -> new SqsWebhookEventPublisher(sqsClient, new ObjectMapper(), " "));
  }
}
