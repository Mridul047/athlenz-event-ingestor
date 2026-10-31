package io.cb.strava.ingest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The thin event payload Strava POSTs to the webhook callback.
 *
 * <p>Strava deliberately sends very little: just enough to tell you <em>what</em> changed and for
 * <em>whom</em>. To get the actual activity detail you must call back to the Strava API using the
 * owner's OAuth token (the job of a downstream processor, out of scope for this ingest app).
 *
 * <p>Field names use snake_case to match Strava's JSON. We rely on Jackson's {@code @JsonProperty}
 * mapping. {@code @JsonIgnoreProperties} keeps us forward-compatible if Strava adds fields (for
 * example {@code updates}).
 *
 * <p>Example payload:
 *
 * <pre>{@code
 * {
 *   "aspect_type": "create",
 *   "event_time": 1549560669,
 *   "object_id": 1360128428,
 *   "object_type": "activity",
 *   "owner_id": 134815,
 *   "subscription_id": 120475,
 *   "updates": {}
 * }
 * }</pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StravaWebhookEvent(
    @JsonProperty("aspect_type") String aspectType,
    @JsonProperty("event_time") Long eventTime,
    @JsonProperty("object_id") Long objectId,
    @JsonProperty("object_type") String objectType,
    @JsonProperty("owner_id") Long ownerId,
    @JsonProperty("subscription_id") Long subscriptionId) {

  /** Strava object types we care about. */
  public static final String OBJECT_TYPE_ACTIVITY = "activity";

  /** Aspect types Strava may send. */
  public static final String ASPECT_CREATE = "create";

  public static final String ASPECT_UPDATE = "update";
  public static final String ASPECT_DELETE = "delete";

  public boolean isActivity() {
    return OBJECT_TYPE_ACTIVITY.equals(objectType);
  }

  /**
   * A stable, idempotency-friendly key for this delivery. Combines the object, the aspect, and the
   * event time so a replay of the same event maps to the same key.
   */
  public String idempotencyKey() {
    return objectType + "#" + objectId + "#" + aspectType + "#" + eventTime;
  }
}
