package io.cb.strava.ingest;

/**
 * Transport-independent outcome of handling a webhook request. The Lambda handler maps this onto an
 * API Gateway proxy response; tests can assert on it directly without constructing API Gateway
 * types.
 *
 * @param statusCode HTTP status to return
 * @param body response body (may be {@code null} for an empty body)
 */
public record WebhookResult(int statusCode, String body) {

  public static WebhookResult ok() {
    return new WebhookResult(200, null);
  }

  public static WebhookResult ok(String body) {
    return new WebhookResult(200, body);
  }

  public static WebhookResult badRequest() {
    return new WebhookResult(400, null);
  }

  public static WebhookResult forbidden() {
    return new WebhookResult(403, null);
  }

  public static WebhookResult methodNotAllowed() {
    return new WebhookResult(405, null);
  }
}
