package io.cb.strava.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests the thin API Gateway proxy mapping in {@link WebhookHandler}. The handler is constructed
 * with an in-memory environment (no SQS configured), so it falls back to the logging publisher and
 * makes no AWS calls.
 */
class WebhookHandlerTest {

  private WebhookHandler handlerWith(Map<String, String> env) {
    return new WebhookHandler(env);
  }

  @Test
  void getRoutesToValidationAndEchoesChallenge() {
    WebhookHandler handler = handlerWith(Map.of("STRAVA_VERIFY_TOKEN", "tok"));

    APIGatewayProxyRequestEvent req =
        new APIGatewayProxyRequestEvent()
            .withHttpMethod("GET")
            .withQueryStringParameters(
                Map.of(
                    "hub.mode", "subscribe",
                    "hub.verify_token", "tok",
                    "hub.challenge", "abc123"));

    APIGatewayProxyResponseEvent res = handler.handleRequest(req, null);

    assertEquals(200, res.getStatusCode());
    assertTrue(res.getBody().contains("abc123"));
    assertEquals("application/json", res.getHeaders().get("Content-Type"));
  }

  @Test
  void getWithWrongTokenIsForbidden() {
    WebhookHandler handler = handlerWith(Map.of("STRAVA_VERIFY_TOKEN", "tok"));

    APIGatewayProxyRequestEvent req =
        new APIGatewayProxyRequestEvent()
            .withHttpMethod("GET")
            .withQueryStringParameters(
                Map.of(
                    "hub.mode", "subscribe",
                    "hub.verify_token", "nope",
                    "hub.challenge", "abc123"));

    assertEquals(403, handler.handleRequest(req, null).getStatusCode());
  }

  @Test
  void postActivityEventIsAcknowledged() {
    WebhookHandler handler = handlerWith(Map.of("STRAVA_VERIFY_TOKEN", "tok"));

    APIGatewayProxyRequestEvent req =
        new APIGatewayProxyRequestEvent()
            .withHttpMethod("POST")
            .withBody(
                """
                        {"aspect_type":"create","event_time":1549560669,
                         "object_id":1360128428,"object_type":"activity",
                         "owner_id":134815,"subscription_id":120475}
                        """);

    assertEquals(200, handler.handleRequest(req, null).getStatusCode());
  }

  @Test
  void unsupportedMethodIsRejected() {
    WebhookHandler handler = handlerWith(Map.of("STRAVA_VERIFY_TOKEN", "tok"));

    APIGatewayProxyRequestEvent req = new APIGatewayProxyRequestEvent().withHttpMethod("DELETE");

    assertEquals(405, handler.handleRequest(req, null).getStatusCode());
  }
}
