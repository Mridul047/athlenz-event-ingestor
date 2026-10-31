package io.cb.strava.ingest;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Builds the shared {@link SqsClient}. Without a DI framework we construct the client once in the
 * handler's constructor (the Lambda init phase) and reuse it across warm invocations — the AWS best
 * practice of initializing SDK clients outside the handler method.
 *
 * <p>We use {@link UrlConnectionHttpClient} rather than the default Apache client: a smaller
 * dependency footprint and faster startup, which matters for cold starts.
 *
 * <h2>Local development with an emulator (Floci / LocalStack)</h2>
 *
 * <p>When {@code AWS_ENDPOINT_OVERRIDE} is set (for example {@code http://localhost:4566}), the
 * client is pointed at that endpoint with dummy static credentials and a fixed region. In deployed
 * Lambda the variable is left unset, so the client uses the real AWS endpoints, the Lambda-provided
 * region, and the function's IAM role credentials via the default chains.
 */
final class SqsClientFactory {

  private SqsClientFactory() {}

  static SqsClient create(String endpointOverride, String region) {
    var builder = SqsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder());

    if (endpointOverride != null && !endpointOverride.isBlank()) {
      builder
          .endpointOverride(URI.create(endpointOverride))
          .region(Region.of(region == null || region.isBlank() ? "us-east-1" : region))
          // Emulators accept any credentials; use obvious dummies.
          .credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
    }
    return builder.build();
  }
}
