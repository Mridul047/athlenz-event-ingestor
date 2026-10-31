package io.cb.strava.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the transport-independent webhook logic (validation handshake and the fast-ack
 * POST path). A recording publisher stands in for SQS, so no AWS services are required and the
 * tests run fast.
 */
class WebhookServiceTest {

  private static final String VERIFY_TOKEN = "s3cr3t-verify-token";

  private final ObjectMapper mapper = new ObjectMapper();
  private RecordingPublisher publisher;
  private WebhookService service;

  @BeforeEach
  void setUp() {
    publisher = new RecordingPublisher();
    service = new WebhookService(VERIFY_TOKEN, publisher, mapper);
  }

  @Test
  void echoesChallengeWhenVerifyTokenMatches() {
    WebhookResult result =
        service.validate(
            Map.of(
                "hub.mode", "subscribe",
                "hub.verify_token", VERIFY_TOKEN,
                "hub.challenge", "challenge-123"));

    assertEquals(200, result.statusCode());
    assertEquals("{\"hub.challenge\":\"challenge-123\"}", result.body());
  }

  @Test
  void rejectsWhenVerifyTokenDoesNotMatch() {
    WebhookResult result =
        service.validate(
            Map.of(
                "hub.mode", "subscribe",
                "hub.verify_token", "wrong-token",
                "hub.challenge", "challenge-123"));

    assertEquals(403, result.statusCode());
    assertNull(result.body());
  }

  @Test
  void rejectsWhenModeIsNotSubscribe() {
    WebhookResult result =
        service.validate(
            Map.of(
                "hub.mode", "unsubscribe",
                "hub.verify_token", VERIFY_TOKEN,
                "hub.challenge", "challenge-123"));

    assertEquals(400, result.statusCode());
  }

  @Test
  void rejectsWhenChallengeMissing() {
    WebhookResult result =
        service.validate(Map.of("hub.mode", "subscribe", "hub.verify_token", VERIFY_TOKEN));

    assertEquals(400, result.statusCode());
  }

  @Test
  void rejectsWhenQueryIsNull() {
    WebhookResult result = service.validate(null);
    assertEquals(400, result.statusCode());
  }

  @Test
  void enqueuesActivityEventAndReturns200() {
    String body =
        """
                {
                  "aspect_type": "create",
                  "event_time": 1549560669,
                  "object_id": 1360128428,
                  "object_type": "activity",
                  "owner_id": 134815,
                  "subscription_id": 120475
                }
                """;

    WebhookResult result = service.receive(body);

    assertEquals(200, result.statusCode());
    assertEquals(1, publisher.published.size());
    assertEquals(1360128428L, publisher.published.get(0).objectId());
  }

  @Test
  void ignoresNonActivityEventButStillReturns200() {
    String body =
        """
                {
                  "aspect_type": "create",
                  "event_time": 1549560669,
                  "object_id": 999,
                  "object_type": "athlete",
                  "owner_id": 134815,
                  "subscription_id": 120475
                }
                """;

    WebhookResult result = service.receive(body);

    assertEquals(200, result.statusCode());
    assertTrue(publisher.published.isEmpty(), "non-activity events must not be enqueued");
  }

  @Test
  void acknowledgesMalformedBodyWithoutEnqueuing() {
    WebhookResult result = service.receive("{ not valid json");

    assertEquals(200, result.statusCode());
    assertTrue(publisher.published.isEmpty());
  }

  @Test
  void acknowledgesEmptyBodyWithoutEnqueuing() {
    assertEquals(200, service.receive(null).statusCode());
    assertEquals(200, service.receive("").statusCode());
    assertTrue(publisher.published.isEmpty());
  }

  @Test
  void acknowledgesEventMissingRequiredFieldsWithoutEnqueuing() {
    // object_type/object_id absent -> ack, no enqueue.
    WebhookResult result = service.receive("{\"aspect_type\":\"create\"}");

    assertEquals(200, result.statusCode());
    assertTrue(publisher.published.isEmpty());
  }

  /** Simple in-memory publisher that records what it was handed. */
  private static final class RecordingPublisher implements WebhookEventPublisher {
    private final List<StravaWebhookEvent> published = new ArrayList<>();

    @Override
    public void publish(StravaWebhookEvent event) {
      published.add(event);
    }
  }
}
