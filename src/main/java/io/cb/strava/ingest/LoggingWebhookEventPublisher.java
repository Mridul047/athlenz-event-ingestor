package io.cb.strava.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fallback publisher used when no SQS queue URL is configured (local development and tests). It
 * simply logs the event instead of enqueuing it.
 */
public final class LoggingWebhookEventPublisher implements WebhookEventPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingWebhookEventPublisher.class);

  @Override
  public void publish(StravaWebhookEvent event) {
    LOG.info(
        "[no SQS configured] would enqueue event objectId={} aspect={}",
        event.objectId(),
        event.aspectType());
  }
}
