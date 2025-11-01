import com.amazonaws.HttpMethod;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

public class Handler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    private static final AmazonS3 S3_CLIENT = AmazonS3ClientBuilder.standard().build();
    private static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME");
    private static final long URL_EXPIRATION_TIME = 3600000; // 1 hour in milliseconds

    static {
        org.traccar.Main.run("traccar.xml");
    }

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        String method = Optional.ofNullable(event.getRequestContext())
                .map(APIGatewayV2HTTPEvent.RequestContext::getHttp)
                .map(APIGatewayV2HTTPEvent.RequestContext.Http::getMethod)
                .orElse("GET");

        String path = Optional.ofNullable(event.getRawPath()).orElse("/");
        String query = Optional.ofNullable(event.getRawQueryString())
                .filter(q -> !q.isEmpty()).map(q -> "?" + q).orElse("");
        String fullUrl = "http://localhost:8082" + path + query;

        log(context, fullUrl);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(fullUrl))
                .method(method, bodyPublisher(event));

        if (event.getHeaders() != null) {
            Map<String, String> headers = event.getHeaders();
            String tokens = System.getenv("BLACK_LISTED_TOKENS");
            if (tokens != null && !tokens.isEmpty()) {
                String[] tokenArray = tokens.split(",");
                for (String token : tokenArray) {
                    if (headers.containsKey("authorization") && headers.get("authorization").startsWith("Bearer " + token)) {
                        return errorResponse("Too many requests", 429);
                    }
                }
            }
            context.getLogger().log(headers.toString() + "\n", LogLevel.DEBUG);
            for (String name : List.of("accept", "cookie", "content-type", "authorization")) {
                Optional.ofNullable(headers.get(name))
                        .ifPresent(value -> requestBuilder.header(name, value));
            }
        }

        try {
            return toLambdaResponse(HTTP_CLIENT.send(requestBuilder.build(),
                    HttpResponse.BodyHandlers.ofInputStream()), context);
        } catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            return errorResponse(e.getMessage());
        }
    }

    private static void log(Context context, String message) {
        context.getLogger().log(
                String.format("RequestId: %s %s\n", context.getAwsRequestId(),  message),
                LogLevel.DEBUG);
    }

    private static HttpRequest.BodyPublisher bodyPublisher(APIGatewayV2HTTPEvent event) {
        String method = Optional.ofNullable(event.getRequestContext())
                .map(APIGatewayV2HTTPEvent.RequestContext::getHttp)
                .map(APIGatewayV2HTTPEvent.RequestContext.Http::getMethod)
                .orElse("GET");
        if (method.equalsIgnoreCase("GET")) {
            return HttpRequest.BodyPublishers.noBody();
        }
        String body = Optional.ofNullable(event.getBody()).orElse("");
        if (event.getIsBase64Encoded()) {
            return HttpRequest.BodyPublishers.ofByteArray(Base64.getDecoder().decode(body));
        } else {
            return HttpRequest.BodyPublishers.ofString(body);
        }
    }

    private static APIGatewayV2HTTPResponse toLambdaResponse(HttpResponse<InputStream> response, Context context) throws IOException {
        System.out.printf(" received %d\n", response.statusCode());
        if (response.statusCode() >= 400) {
            String body = new String(response.body().readAllBytes());
            log(context, String.format("returning %d %s", response.statusCode(), body));
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(response.statusCode())
                    .withBody(body)
                    .withIsBase64Encoded(false)
                    .build();
        }

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (InputStream responseStream = response.body();
             GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            responseStream.transferTo(gzipStream);
        }
        byte[] compressedBody = byteStream.toByteArray();

        Map<String, String> headers = new HashMap<>();
        HttpHeaders rawHeaders = response.headers();
        rawHeaders.map().forEach((k, vList) -> headers.put(k, String.join(", ", vList)));

        final int uploadThreshold = 6 * 1024 * 1024; // 6MB
        if (compressedBody.length < uploadThreshold) {
            log(context, String.format("returning %d bytes", compressedBody.length));
            headers.put("Content-Encoding", "gzip");
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(response.statusCode())
                    .withIsBase64Encoded(true)
                    .withHeaders(headers)
                    .withBody(Base64.getEncoder().encodeToString(compressedBody))
                    .build();
        } else {
            System.out.printf(" payload size %d bytes exceeds threshold, uploading to S3\n", compressedBody.length);
            String contentType = headers.getOrDefault("Content-Type", "application/json");
            String fileUrl = uploadToS3AndGetUrl(compressedBody, contentType);
            if (fileUrl == null) {
                return errorResponse("Failed to upload response to S3");
            }
            headers.put("Location", fileUrl);
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(302)
                    .withHeaders(headers)
                    .build();
        }
    }

    private static APIGatewayV2HTTPResponse errorResponse(String message) {
        return errorResponse(message, 503);
    }
    private static APIGatewayV2HTTPResponse errorResponse(String message, int statusCode) {
        System.out.println("returning " + statusCode);
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(statusCode)
                .withBody(message)
                .withIsBase64Encoded(false)
                .build();
    }

    private static String uploadToS3AndGetUrl(byte[] content, String contentType) {
        try {
            String key = "response-" + UUID.randomUUID() + ".json";

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);
            metadata.setContentType(contentType);
            metadata.setContentEncoding("gzip");

            PutObjectRequest putRequest = new PutObjectRequest(
                    BUCKET_NAME,
                    key,
                    new ByteArrayInputStream(content),
                    metadata);

            S3_CLIENT.putObject(putRequest);

            // Generate pre-signed URL
            Date expiration = new Date();
            expiration.setTime(expiration.getTime() + URL_EXPIRATION_TIME);

            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                    new GeneratePresignedUrlRequest(BUCKET_NAME, key)
                    .withMethod(HttpMethod.GET)
                    .withExpiration(expiration);

            URL url = S3_CLIENT.generatePresignedUrl(generatePresignedUrlRequest);
            return url.toString();
        } catch (Exception e) {
            System.err.println("Error uploading to S3: " + e.getMessage());
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            return null;
        }
    }
}
