/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.awaitility.Awaitility;
import org.junit.Assume;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.StringUtil;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

public class MultiTenantHourlyCronS3CheckpointOverrideIT extends AbstractMultiTenantAnomalyDetectorRestTestCase {

    private static final String INTERNAL_API_TOKEN = System
        .getProperty("tests.opensearch.plugins.timeseries.internal_api_shared_secret", "integration-test-internal-token");
    private static final String MODEL_CLUSTER_PROPERTY = "tests.model.rest.cluster";

    public void testHourlyCronCheckpointSavingFreqOverrideUpdatesS3CheckpointTimestamp() throws Exception {
        Assume.assumeTrue("S3 checkpoint bucket must be configured", hasText(bucketName()));
        Assume.assumeTrue("AWS region must be configured", hasText(region()));
        Assume.assumeTrue("Cloud map table must be configured", hasText(cloudMapTableName()));
        Assume.assumeTrue("Cloud map service must be configured", hasText(cloudMapService()));

        String tenantId = tenantId("hourly-cron-s3-override");
        String indexName = indexName("hourly-cron-s3-override");
        HashRingRevisionRef hashRingRevisionRef = null;

        try (
            RestClient modelClient = buildClient(restClientSettings(), modelHosts());
            S3Client s3Client = s3Client();
            DynamoDbClient dynamoDbClient = dynamoDbClient()
        ) {
            hashRingRevisionRef = ensureLatestHashRingRevisionTargetsCoordinator(dynamoDbClient);
            createIndex(modelClient, indexName);

            Instant baseTime = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(60, ChronoUnit.MINUTES);
            ingestSeries(modelClient, indexName, baseTime, 50);

            Feature feature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", true);
            AnomalyDetector detector = detectorBuilder(indexName, detectorName("hourly-cron-s3-override"), List.of(feature)).build();
            String detectorId = createTenantDetector(detector, tenantId).getId();

            Instant periodStart = baseTime.plus(48, ChronoUnit.MINUTES);
            Instant periodEnd = baseTime.plus(49, ChronoUnit.MINUTES);
            Awaitility.await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
                Response response = runDetector(detectorId, periodStart, periodEnd, tenantId);
                assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
                assertTrue(fetchModelCount(modelClient, detectorId, tenantId) > 0);
            });

            String checkpointPrefix = checkpointPrefix(tenantId, detectorId);
            Instant initialLastModified = Awaitility
                .await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> latestCheckpointLastModified(s3Client, checkpointPrefix), instant -> instant != null);

            updateCheckpointSavingFreq(client(), "1m");
            updateCheckpointSavingFreq(modelClient, "1m");

            Awaitility.await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertTrue(fetchModelCount(modelClient, detectorId, tenantId) > 0);
                triggerCron(modelClient);
                Instant updatedLastModified = latestCheckpointLastModified(s3Client, checkpointPrefix);
                assertNotNull(updatedLastModified);
                assertTrue(
                    "Expected S3 checkpoint lastModified to advance past " + initialLastModified + " but was " + updatedLastModified,
                    updatedLastModified.isAfter(initialLastModified)
                );
            });
        } finally {
            try {
                clearCheckpointSavingFreq(client());
                try (RestClient modelClient = buildClient(restClientSettings(), modelHosts())) {
                    clearCheckpointSavingFreq(modelClient);
                }
            } finally {
                deleteHashRingRevisionIfPresent(hashRingRevisionRef);
            }
        }
    }

    protected String checkpointPrefix(String tenantId, String detectorId) {
        return StringUtil.sanitizeId(tenantId) + "/" + detectorId;
    }

    private void createIndex(RestClient client, String indexName) throws IOException {
        TestHelpers.createIndexWithTimeField(client, indexName, TIME_FIELD);
    }

    private void ingestSeries(RestClient client, String indexName, Instant baseTime, int numberOfPoints) throws IOException {
        for (int i = 0; i < numberOfPoints; i++) {
            Instant timestamp = baseTime.plus(i, ChronoUnit.MINUTES).plusSeconds(30);
            String document = String.format(Locale.ROOT, "{\"%s\":%d,\"%s\":%d}", TIME_FIELD, timestamp.toEpochMilli(), VALUE_FIELD, i + 1);
            TestHelpers.ingestDataToIndex(client, indexName, TestHelpers.toHttpEntity(document));
        }
    }

    private Response runDetector(String detectorId, Instant periodStart, Instant periodEnd, String tenantId) throws IOException {
        String requestBody = String
            .format(Locale.ROOT, "{\"period_start\":%d,\"period_end\":%d}", periodStart.toEpochMilli(), periodEnd.toEpochMilli());
        return TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/_run",
                Map.of(),
                TestHelpers.toHttpEntity(requestBody),
                tenantHeaders(tenantId)
            );
    }

    private long fetchModelCount(RestClient client, String detectorId, String tenantId) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_INTERNAL_DETECTORS_URI + "/" + detectorId + "/_node_profile/models",
                Map.of(),
                "",
                internalTenantHeaders(tenantId)
            );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        Object modelCount = entityAsMap(response).get(CommonName.MODEL_COUNT);
        return modelCount == null ? 0L : ((Number) modelCount).longValue();
    }

    private Instant latestCheckpointLastModified(S3Client s3Client, String checkpointPrefix) {
        return s3Client
            .listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName()).prefix(checkpointPrefix).build())
            .contents()
            .stream()
            .map(S3Object::lastModified)
            .filter(lastModified -> lastModified != null)
            .max(Instant::compareTo)
            .orElse(null);
    }

    private void triggerCron(RestClient client) throws IOException {
        Request request = new Request("POST", TimeSeriesAnalyticsPlugin.TIMESERIES_BASE_URI + "/_cron");
        Response response = client.performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private void updateCheckpointSavingFreq(RestClient client, String checkpointSavingFreq) throws Exception {
        Request request = new Request("PUT", "_cluster/settings");
        var builder = XContentFactory
            .jsonBuilder()
            .startObject()
            .startObject("persistent")
            .field("plugins.anomaly_detection.checkpoint_saving_freq", checkpointSavingFreq)
            .endObject()
            .endObject();
        request.setJsonEntity(builder.toString());
        Response response = client.performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private void clearCheckpointSavingFreq(RestClient client) throws Exception {
        Request request = new Request("PUT", "_cluster/settings");
        var builder = XContentFactory
            .jsonBuilder()
            .startObject()
            .startObject("persistent")
            .nullField("plugins.anomaly_detection.checkpoint_saving_freq")
            .endObject()
            .endObject();
        request.setJsonEntity(builder.toString());
        client.performRequest(request);
    }

    private DynamoDbClient dynamoDbClient() {
        return DynamoDbClient.builder().region(Region.of(region())).credentialsProvider(SecurityUtil.createCredentialsProvider()).build();
    }

    private S3Client s3Client() {
        return S3Client.builder().region(Region.of(region())).credentialsProvider(SecurityUtil.createCredentialsProvider()).build();
    }

    private HashRingRevisionRef ensureLatestHashRingRevisionTargetsCoordinator(DynamoDbClient dynamoDbClient) {
        String partitionKey = "service#" + cloudMapService();
        QueryRequest latestRevisionRequest = QueryRequest
            .builder()
            .tableName(cloudMapTableName())
            .keyConditionExpression("PK = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(partitionKey)))
            .scanIndexForward(false)
            .limit(1)
            .build();

        var latestRevisionResponse = dynamoDbClient.query(latestRevisionRequest);
        long latestRevisionId = -1L;
        List<String> latestTasks = List.of();
        if (latestRevisionResponse.hasItems() && !latestRevisionResponse.items().isEmpty()) {
            Map<String, AttributeValue> latestItem = latestRevisionResponse.items().get(0);
            AttributeValue revisionValue = latestItem.get("revisionId");
            if (revisionValue != null && revisionValue.n() != null) {
                latestRevisionId = Long.parseLong(revisionValue.n());
            }
            latestTasks = extractTasks(latestItem);
        }

        if (latestTasks.equals(List.of("127.0.0.1"))) {
            return null;
        }

        long revisionId = Math.max(latestRevisionId + 1, clockEpochMillis() * 1000L);
        long expiresAt = Instant.now().plus(3650, ChronoUnit.DAYS).getEpochSecond();

        PutItemRequest putItemRequest = PutItemRequest
            .builder()
            .tableName(cloudMapTableName())
            .item(
                Map
                    .of(
                        "PK",
                        AttributeValue.fromS(partitionKey),
                        "revisionId",
                        AttributeValue.fromN(Long.toString(revisionId)),
                        "tasks",
                        AttributeValue.fromL(List.of(AttributeValue.fromS("127.0.0.1"))),
                        "expiresAt",
                        AttributeValue.fromN(Long.toString(expiresAt))
                    )
            )
            .build();

        dynamoDbClient.putItem(putItemRequest);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            var latestResponse = dynamoDbClient.query(latestRevisionRequest);
            assertFalse(latestResponse.items().isEmpty());
            Map<String, AttributeValue> latestItem = latestResponse.items().get(0);
            assertEquals(Long.toString(revisionId), latestItem.get("revisionId").n());
            assertEquals(List.of("127.0.0.1"), extractTasks(latestItem));
        });
        return new HashRingRevisionRef(partitionKey, revisionId);
    }

    private List<String> extractTasks(Map<String, AttributeValue> item) {
        AttributeValue tasks = item.get("tasks");
        if (tasks == null || tasks.l() == null) {
            return List.of();
        }
        return tasks.l().stream().map(AttributeValue::s).filter(this::hasText).toList();
    }

    private long clockEpochMillis() {
        return Instant.now().toEpochMilli();
    }

    private void deleteHashRingRevisionIfPresent(HashRingRevisionRef hashRingRevisionRef) {
        if (hashRingRevisionRef == null) {
            return;
        }

        try (DynamoDbClient dynamoDbClient = dynamoDbClient()) {
            dynamoDbClient
                .deleteItem(
                    DeleteItemRequest
                        .builder()
                        .tableName(cloudMapTableName())
                        .key(
                            Map
                                .of(
                                    "PK",
                                    AttributeValue.fromS(hashRingRevisionRef.partitionKey),
                                    "revisionId",
                                    AttributeValue.fromN(Long.toString(hashRingRevisionRef.revisionId))
                                )
                        )
                        .build()
                );
        }
    }

    private List<Header> internalTenantHeaders(String tenantId) {
        return List
            .of(
                new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
                new BasicHeader(CommonName.TENANT_ID_HEADER, tenantId),
                new BasicHeader(CommonName.INTERNAL_API_TOKEN_HEADER, INTERNAL_API_TOKEN)
            );
    }

    private String bucketName() {
        return System.getProperty("tests.opensearch.plugins.anomaly_detection.s3_checkpoint_bucket");
    }

    private String region() {
        return System.getProperty("tests.opensearch.plugins.timeseries.region");
    }

    private String cloudMapTableName() {
        return System.getProperty("tests.opensearch.plugins.timeseries.cloud_map_table_name");
    }

    private String cloudMapService() {
        return System.getProperty("tests.opensearch.plugins.timeseries.cloud_map_service");
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private HttpHost[] modelHosts() {
        String cluster = System.getProperty(MODEL_CLUSTER_PROPERTY);
        if (cluster == null || cluster.isBlank()) {
            throw new IllegalStateException("Must specify [" + MODEL_CLUSTER_PROPERTY + "] to run " + getClass().getSimpleName());
        }

        String[] stringUrls = cluster.split(",");
        HttpHost[] hosts = new HttpHost[stringUrls.length];
        for (int i = 0; i < stringUrls.length; i++) {
            String stringUrl = stringUrls[i].trim();
            int portSeparator = stringUrl.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            String host = stringUrl.substring(0, portSeparator);
            int port = Integer.parseInt(stringUrl.substring(portSeparator + 1));
            hosts[i] = buildHttpHost(host, port);
        }
        return hosts;
    }

    private static final class HashRingRevisionRef {
        private final String partitionKey;
        private final long revisionId;

        private HashRingRevisionRef(String partitionKey, long revisionId) {
            this.partitionKey = partitionKey;
            this.revisionId = revisionId;
        }
    }
}
