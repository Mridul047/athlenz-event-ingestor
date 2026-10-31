package io.cb.strava.ingest;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * AWS Lambda entrypoint for the Strava webhook callback, served through the API Gateway proxy
 * integration. No framework: this implements the Lambda {@link RequestHandler} directly against the
 * API Gateway proxy event types.
 *
 * <p>Expensive setup (JSON mapper, SQS client, the webhook service) happens once in the constructor
 * during the Lambda init phase and is reused across warm invocations.
 *
 * <h2>Configuration (environment variables)</h2>
 *
 * <ul>
 *   <li>{@code STRAVA_VERIFY_TOKEN} — the token echoed during the handshake.
 *   <li>{@code EVENT_QUEUE_URL} — SQS queue to publish events to. When unset or blank the function
 *       logs events instead of enqueuing (local dev).
 *   <li>{@code AWS_ENDPOINT_OVERRIDE} — optional local emulator endpoint.
 *   <li>{@code AWS_REGION} — provided by Lambda; used for the emulator case.
 * </ul>
 */
public class WebhookHandler
    implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(WebhookHandler.class);

  private final WebhookService service;

  /** Lambda invokes this no-arg constructor once per container (init phase). */
  public WebhookHandler() {
    this(System.getenv());
  }

  /** Package-private constructor for tests: supply the environment directly. */
  WebhookHandler(Map<String, String> env) {
    ObjectMapper objectMapper = new ObjectMapper();
    String verifyToken = env.getOrDefault("STRAVA_VERIFY_TOKEN", "local-dev-verify-token");
    String queueUrl = env.get("EVENT_QUEUE_URL");

    WebhookEventPublisher publisher;
    if (queueUrl != null && !queueUrl.isBlank()) {
      SqsClient sqsClient =
          SqsClientFactory.create(env.get("AWS_ENDPOINT_OVERRIDE"), env.get("AWS_REGION"));
      publisher = new SqsWebhookEventPublisher(sqsClient, objectMapper, queueUrl);
      LOG.info("Publishing webhook events to SQS");
    } else {
      publisher = new LoggingWebhookEventPublisher();
      LOG.info("No EVENT_QUEUE_URL configured; logging events instead of enqueuing");
    }

    this.service = new WebhookService(verifyToken, publisher, objectMapper);
  }

  @Override
  public APIGatewayProxyResponseEvent handleRequest(
      APIGatewayProxyRequestEvent request, Context context) {
    String method = request.getHttpMethod() == null ? "" : request.getHttpMethod().toUpperCase();
    LOG.info("{} /webhook", method.isEmpty() ? "(none)" : method);

    WebhookResult result =
        switch (method) {
          case "GET" -> service.validate(request.getQueryStringParameters());
          case "POST" -> service.receive(request.getBody());
          default -> {
            LOG.error("Unsupported method: {}", method);
            yield WebhookResult.methodNotAllowed();
          }
        };

    APIGatewayProxyResponseEvent response =
        new APIGatewayProxyResponseEvent().withStatusCode(result.statusCode());
    if (result.body() != null) {
      response.withHeaders(Map.of("Content-Type", "application/json")).withBody(result.body());
    }
    return response;
  }
}
