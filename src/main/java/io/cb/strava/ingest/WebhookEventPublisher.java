package io.cb.strava.ingest;

/**
 * Publishes validated webhook events for asynchronous processing.
 *
 * <p>Defined as an interface so the handler depends on an abstraction rather than SQS directly.
 * This keeps the handler logic unit-testable (a fake publisher in tests) and lets the transport be
 * swapped (SQS today, something else later) without touching the handler.
 */
public interface WebhookEventPublisher {

  /**
   * Hand the event off for async processing. Implementations must return quickly so the ingest
   * Lambda can ack Strava within its timeout.
   *
   * @param event the validated webhook event
   */
  void publish(StravaWebhookEvent event);
}
