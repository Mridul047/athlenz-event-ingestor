package io.cb.strava.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transport-independent webhook logic for the two Strava callback interactions. Kept free of any
 * API Gateway / Lambda types so it can be unit tested directly, with the {@link WebhookHandler}
 * doing only the thin mapping from the proxy event to these calls.
 *
 * <ul>
 *   <li><b>GET</b> — the one-time subscription <em>validation handshake</em>. Strava sends {@code
 *       hub.mode=subscribe}, a {@code hub.verify_token} we must check, and a {@code hub.challenge}
 *       we must echo back within two seconds.
 *   <li><b>POST</b> — ongoing <em>event deliveries</em>. These must be acknowledged quickly with
 *       HTTP 200; the actual work happens asynchronously (we enqueue to SQS and return
 *       immediately).
 * </ul>
 */
public final class WebhookService {

  private static final Logger LOG = LoggerFactory.getLogger(WebhookService.class);

  private final String verifyToken;
  private final WebhookEventPublisher publisher;
  private final ObjectMapper objectMapper;

  public WebhookService(
      String verifyToken, WebhookEventPublisher publisher, ObjectMapper objectMapper) {
    this.verifyToken = verifyToken == null ? "" : verifyToken;
    this.publisher = publisher;
    this.objectMapper = objectMapper;
  }

  /**
   * Subscription validation handshake.
   *
   * <p>Returns 200 with a JSON body echoing the challenge only when the verify token matches the
   * configured secret. A mismatch returns 403 so a third party cannot complete a subscription on
   * our behalf. A missing or invalid mode/challenge returns 400.
   *
   * @param query the request query parameters (may be {@code null})
   */
  public WebhookResult validate(Map<String, String> query) {
    Map<String, String> q = query == null ? Map.of() : query;
    String mode = q.get("hub.mode");
    String token = q.get("hub.verify_token");
    String challenge = q.get("hub.challenge");

    if (!"subscribe".equals(mode) || challenge == null || challenge.isBlank()) {
      LOG.info("Rejecting validation request: missing or invalid hub.mode/hub.challenge");
      return WebhookResult.badRequest();
    }

    if (!verifyToken.equals(token)) {
      // Do not log the expected or received token values.
      LOG.error("Rejecting validation request: verify token mismatch");
      return WebhookResult.forbidden();
    }

    LOG.info("Subscription validation handshake accepted");
    try {
      // {"hub.challenge":"<challenge>"}
      String body = objectMapper.writeValueAsString(Map.of("hub.challenge", challenge));
      return WebhookResult.ok(body);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize validation response", e);
    }
  }

  /**
   * Event delivery. Acknowledges fast, processes later.
   *
   * <p>We validate the shape, enqueue valid activity events for asynchronous processing, and return
   * 200 immediately. We intentionally return 200 even for events we choose to ignore (for example
   * non-activity objects or a malformed body): Strava only cares that the delivery was accepted,
   * and returning errors would cause it to retry deliveries we do not want.
   *
   * @param body the raw request body (may be {@code null})
   */
  public WebhookResult receive(String body) {
    if (body == null || body.isBlank()) {
      LOG.info("Received empty webhook body; acknowledging without processing");
      return WebhookResult.ok();
    }

    final StravaWebhookEvent event;
    try {
      event = objectMapper.readValue(body, StravaWebhookEvent.class);
    } catch (Exception e) {
      // Malformed body: ack anyway (200) to avoid Strava retry storms,
      // but log so we can investigate. A 4xx here would be retried.
      LOG.info("Received malformed webhook event; acknowledging without processing");
      return WebhookResult.ok();
    }

    if (event == null || event.objectType() == null || event.objectId() == null) {
      LOG.info("Received webhook event missing required fields; acknowledging without processing");
      return WebhookResult.ok();
    }

    if (!event.isActivity()) {
      LOG.info("Ignoring non-activity event: objectType={}", event.objectType());
      return WebhookResult.ok();
    }

    publisher.publish(event);
    LOG.info(
        "Accepted activity event objectId={} ownerId={} aspect={}",
        event.objectId(),
        event.ownerId(),
        event.aspectType());
    return WebhookResult.ok();
  }
}
