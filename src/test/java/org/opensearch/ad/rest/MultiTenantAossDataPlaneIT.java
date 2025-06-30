/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assume;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.SigningRestClientProvider;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.rest.handler.store.endpoint.StaticApiDataSourceEndpointResolverFactory;
import org.opensearch.timeseries.rest.handler.store.endpoint.StaticDataSourceEndpointResolverFactory;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.StringUtil;

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.GetScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.GetScheduleResponse;
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class MultiTenantAossDataPlaneIT extends AbstractMultiTenantAnomalyDetectorRestTestCase {

    private static final int AOSS_BULK_MAX_ATTEMPTS = 20;
    private static final int SQS_SEND_MAX_ATTEMPTS = 20;

    private static final String CUSTOMER_ENDPOINT_PROPERTY = "tests.opensearch.plugins.timeseries.dataplane.endpoint";
    private static final String API_ENDPOINT_PROPERTY = "tests.opensearch.plugins.timeseries.api_dataplane.endpoint";
    private static final String REGION_PROPERTY = "tests.opensearch.plugins.timeseries.region";
    private static final String CLOUD_MAP_TABLE_PROPERTY = "tests.opensearch.plugins.timeseries.cloud_map_table_name";
    private static final String CLOUD_MAP_SERVICE_PROPERTY = "tests.opensearch.plugins.timeseries.cloud_map_service";
    private static final String SQS_ACCOUNT_IDS_PROPERTY = "tests.opensearch.plugins.timeseries.sqs.account_ids";
    private static final String SQS_QUEUE_NAME_PROPERTY = "tests.opensearch.plugins.anomaly_detection.sqs.queue_name";
    private static final String SCHEDULE_MANAGEMENT_ROLE_NAME_PROPERTY =
        "tests.opensearch.plugins.anomaly_detection.event_bridge.schedule_management_role_name";
    private static final String SQS_DELIVERY_ROLE_NAME_PROPERTY =
        "tests.opensearch.plugins.anomaly_detection.event_bridge.sqs_delivery_role_name";
    private static final String S3_CHECKPOINT_BUCKET_PROPERTY = "tests.opensearch.plugins.anomaly_detection.s3_checkpoint_bucket";
    private static final String SCHEDULER_GROUP_PROPERTY = "tests.opensearch.plugins.anomaly_detection.scheduler_group";
    private static final String DATA_SOURCE_RESOLVER_PROPERTY =
        "tests.opensearch.plugins.anomaly_detection.data_source_endpoint_resolver_factory_class";
    private static final String API_DATA_SOURCE_RESOLVER_PROPERTY =
        "tests.opensearch.plugins.anomaly_detection.api_data_source_endpoint_resolver_factory_class";
    private static final String DEFAULT_EVENT_BRIDGE_CELL_ID = "793040377150";
    private static final String DEFAULT_SCHEDULE_MANAGEMENT_ROLE_NAME = "ADScheduleManagementRole";
    private static final String DEFAULT_SQS_DELIVERY_ROLE_NAME = "SchedulerToSQSRole";
    private static final String EVENT_BRIDGE_CELL_ID_HEADER = "x-eb-cell-id";
    private static final String AD_QUEUE_NAME = "ad-jobs.fifo";
    private static final String HCAD_ENTITY_FIELD = "entity_id";
    private static final String AUTOSCALING_WORKLOAD_DETECTOR_COUNT_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.verifyScale.workload.detectorCount";
    private static final String AUTOSCALING_WORKLOAD_ENTITY_COUNT_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.verifyScale.workload.entityCount";
    private static final String AUTOSCALING_WORKLOAD_HISTORY_INTERVALS_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.verifyScale.workload.historyIntervals";
    private static final String AUTOSCALING_WORKLOAD_MESSAGES_PER_DETECTOR_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.verifyScale.workload.messagesPerDetector";
    private static final String AUTOSCALING_WORKLOAD_BULK_BATCH_SIZE_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.verifyScale.workload.bulkBatchSize";
    private static final String AUTOSCALING_WORKLOAD_BURST_DURATION_SECONDS_PROPERTY =
        "tests.multiTenant.ecs.autoScaling.verifyScale.workload.burstDurationSeconds";

    private final List<String> aossIndicesToDelete = new ArrayList<>();
    private SigningProxy signingProxy;

    @After
    public void cleanupAossResources() {
        if (signingProxy != null) {
            signingProxy.close();
            signingProxy = null;
        }
        try {
            if (hasText(customerEndpoint()) && hasText(region())) {
                RestClient dataClient = SigningRestClientProvider.getRestClient(customerEndpoint(), region(), "aoss");
                for (String index : aossIndicesToDelete) {
                    try {
                        TestHelpers.makeRequest(dataClient, "DELETE", "/" + index, ImmutableMap.of(), "", null);
                    } catch (Exception ignored) {
                        // Best-effort cleanup; the detector cleanup path may have already removed the result index.
                    }
                }
            }
        } finally {
            SigningRestClientProvider.closeAll();
        }
        aossIndicesToDelete.clear();
    }

    public void testApiProxyAndSqsDirectSigningUseAossCustomerDataPlane() throws Exception {
        assumeAossDataPlaneConfigured();

        if (isLoopbackEndpoint(apiDataPlaneEndpoint())) {
            signingProxy = SigningProxy.start(URI.create(apiDataPlaneEndpoint()), customerEndpoint(), region());
        }

        String tenantId = tenantId("aoss-dataplane");
        String sourceIndex = indexName("aoss-source");
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        aossIndicesToDelete.add(sourceIndex);
        aossIndicesToDelete.add(resultIndex);

        RestClient dataClient = SigningRestClientProvider.getRestClient(customerEndpoint(), region(), "aoss");
        createAossSourceIndex(dataClient, sourceIndex);
        Instant baseTime = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(150, ChronoUnit.MINUTES);
        int numberOfPoints = 180;
        ingestAossSeries(dataClient, sourceIndex, baseTime, numberOfPoints);
        waitForAossFeatureQuery(dataClient, sourceIndex, baseTime, numberOfPoints);
        createAossResultIndex(dataClient, resultIndex);

        HashRingRevisionRef hashRingRevisionRef = null;
        try (
            DynamoDbClient dynamoDbClient = dynamoDbClient();
            SchedulerClient schedulerClient = schedulerClient();
            SqsClient sqsClient = sqsClient()
        ) {
            hashRingRevisionRef = prepareModelNodeRouting(dynamoDbClient);
            assumeQueueHasRedrivePolicy(sqsClient);

            Feature feature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", true);
            AnomalyDetector detector = detectorBuilder(sourceIndex, detectorName("aoss-dataplane"), List.of(feature))
                .setResultIndex(resultIndex)
                .build();

            AnomalyDetector createdDetector = createTenantDetector(detector, tenantId);
            waitForAossIndex(dataClient, resultIndex);

            Response startResponse = startTenantDetector(createdDetector.getId(), tenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(startResponse));
            awaitScheduleExists(schedulerClient, tenantId, createdDetector.getId());

            Instant firstManualTick = Instant.now().truncatedTo(ChronoUnit.MINUTES).plus(2, ChronoUnit.MINUTES);
            sendSqsJobMessage(sqsClient, createdDetector, tenantId, firstManualTick);
            sendSqsJobMessage(sqsClient, createdDetector, tenantId, firstManualTick.plus(1, ChronoUnit.MINUTES));

            Awaitility.await().atMost(Duration.ofMinutes(4)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
                Map<String, Object> result = latestSuccessfulResult(dataClient, resultIndex, createdDetector.getId());
                assertFalse("Expected at least one result in " + resultIndex, result.isEmpty());
                assertEquals(createdDetector.getId(), result.get("detector_id"));
            });
            afterSuccessfulSqsProcessing(createdDetector, tenantId, sqsClient, firstManualTick.plus(2, ChronoUnit.MINUTES));
        } finally {
            cleanupModelNodeRouting(hashRingRevisionRef);
        }
    }

    public void testCrossAccountEventBridgeScheduleTargetsConfiguredSqsQueue() throws Exception {
        assumeAossDataPlaneConfigured();

        String tenantId = tenantId("cross-account-schedule");
        String sourceIndex = indexName("cross-account-source");
        aossIndicesToDelete.add(sourceIndex);

        RestClient dataClient = SigningRestClientProvider.getRestClient(customerEndpoint(), region(), "aoss");
        createAossSourceIndex(dataClient, sourceIndex);
        Instant baseTime = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(150, ChronoUnit.MINUTES);
        int numberOfPoints = 180;
        ingestAossSeries(dataClient, sourceIndex, baseTime, numberOfPoints);
        waitForAossFeatureQuery(dataClient, sourceIndex, baseTime, numberOfPoints);

        Feature feature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", true);
        AnomalyDetector detector = detectorBuilder(sourceIndex, detectorName("cross-account-schedule"), List.of(feature)).build();

        AnomalyDetector createdDetector = createTenantDetector(detector, tenantId);
        String resultIndex = createdDetector.getCustomResultIndexOrAlias();
        assertNotNull("Expected created detector to include a result index", resultIndex);
        aossIndicesToDelete.add(resultIndex);

        HashRingRevisionRef hashRingRevisionRef = null;
        try (DynamoDbClient dynamoDbClient = dynamoDbClient(); SchedulerClient schedulerClient = schedulerClient()) {
            hashRingRevisionRef = prepareModelNodeRouting(dynamoDbClient);

            Response startResponse = startTenantDetector(createdDetector.getId(), tenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(startResponse));
            awaitScheduleExists(schedulerClient, tenantId, createdDetector.getId());

            Awaitility
                .await()
                .ignoreExceptionsMatching(this::isRetryableAossSearchResponse)
                .atMost(Duration.ofMinutes(6))
                .pollInterval(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    Map<String, Object> result = latestSuccessfulResult(dataClient, resultIndex, createdDetector.getId());
                    assertFalse("Expected at least one result in " + resultIndex, result.isEmpty());
                    assertEquals(createdDetector.getId(), result.get("detector_id"));
                });
        } finally {
            cleanupModelNodeRouting(hashRingRevisionRef);
        }
    }

    protected void afterSuccessfulSqsProcessing(AnomalyDetector detector, String tenantId, SqsClient sqsClient, Instant nextManualTick)
        throws Exception {}

    protected AutoScalingWorkload createHighCardinalityAutoScalingWorkload(SqsClient sqsClient, String tenantId, Instant firstScheduledTime)
        throws Exception {
        int detectorCount = intProperty(AUTOSCALING_WORKLOAD_DETECTOR_COUNT_PROPERTY, 10);
        int entityCount = intProperty(AUTOSCALING_WORKLOAD_ENTITY_COUNT_PROPERTY, 1500);
        int historyIntervals = intProperty(AUTOSCALING_WORKLOAD_HISTORY_INTERVALS_PROPERTY, 260);
        int messagesPerDetector = intProperty(AUTOSCALING_WORKLOAD_MESSAGES_PER_DETECTOR_PROPERTY, 240);
        int bulkBatchSize = intProperty(AUTOSCALING_WORKLOAD_BULK_BATCH_SIZE_PROPERTY, 2000);
        int burstDurationSeconds = intProperty(AUTOSCALING_WORKLOAD_BURST_DURATION_SECONDS_PROPERTY, 180);
        LOG
            .info(
                "Creating autoscaling workload with detectorCount={}, entityCount={}, historyIntervals={}, messagesPerDetector={}, bulkBatchSize={}, burstDurationSeconds={}",
                detectorCount,
                entityCount,
                historyIntervals,
                messagesPerDetector,
                bulkBatchSize,
                burstDurationSeconds
            );

        String sourceIndex = indexName("autoscale-hc-source");
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        aossIndicesToDelete.add(sourceIndex);
        aossIndicesToDelete.add(resultIndex);

        RestClient dataClient = SigningRestClientProvider.getRestClient(customerEndpoint(), region(), "aoss");
        createAossHcadSourceIndex(dataClient, sourceIndex);
        Instant baseTime = firstScheduledTime.truncatedTo(ChronoUnit.MINUTES).minus(historyIntervals + 5L, ChronoUnit.MINUTES);
        dataClient = ingestAossHcadSeries(
            dataClient,
            sourceIndex,
            baseTime,
            historyIntervals + messagesPerDetector + 10,
            entityCount,
            bulkBatchSize
        );
        createAossResultIndex(dataClient, resultIndex);

        List<AnomalyDetector> createdDetectors = new ArrayList<>();
        for (int i = 0; i < detectorCount; i++) {
            LOG.info("Creating autoscaling workload detector {}/{}", i + 1, detectorCount);
            Feature feature = TestHelpers.randomFeature("sum_value_" + i, VALUE_FIELD, "sum", true);
            AnomalyDetector detector = detectorBuilder(sourceIndex, detectorName("autoscale-hc-" + i), List.of(feature))
                .setCategoryFields(List.of(HCAD_ENTITY_FIELD))
                .setResultIndex(resultIndex)
                .build();
            AnomalyDetector createdDetector = createTenantDetector(detector, tenantId);
            createdDetectors.add(createdDetector);
        }
        for (int i = 0; i < createdDetectors.size(); i++) {
            AnomalyDetector createdDetector = createdDetectors.get(i);
            LOG.info("Starting autoscaling workload detector {}/{}", i + 1, createdDetectors.size());
            Response startResponse = startTenantDetector(createdDetector.getId(), tenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(startResponse));
        }
        return new AutoScalingWorkload(
            sqsClient,
            tenantId,
            firstScheduledTime,
            messagesPerDetector,
            burstDurationSeconds,
            createdDetectors
        );
    }

    private void assumeAossDataPlaneConfigured() {
        Assume.assumeTrue("Customer AOSS endpoint must be configured", hasText(customerEndpoint()));
        Assume.assumeTrue("API data-plane proxy endpoint must be configured", hasText(apiDataPlaneEndpoint()));
        Assume.assumeTrue("AWS region must be configured", hasText(region()));
        Assume.assumeTrue("Cloud map table must be configured", hasText(cloudMapTableName()));
        Assume.assumeTrue("Cloud map service must be configured", hasText(cloudMapService()));
        Assume
            .assumeTrue(
                "Background data-source resolver must be static for this live AOSS IT",
                StaticDataSourceEndpointResolverFactory.class.getName().equals(System.getProperty(DATA_SOURCE_RESOLVER_PROPERTY))
            );
        Assume
            .assumeTrue(
                "API data-source resolver must be static for this live AOSS IT",
                StaticApiDataSourceEndpointResolverFactory.class.getName().equals(System.getProperty(API_DATA_SOURCE_RESOLVER_PROPERTY))
            );
    }

    private void createAossSourceIndex(RestClient dataClient, String indexName) throws IOException {
        String mapping = String
            .format(
                Locale.ROOT,
                "{\"mappings\":{\"properties\":{\"%s\":{\"type\":\"date\"},\"%s\":{\"type\":\"double\"}}}}",
                TIME_FIELD,
                VALUE_FIELD
            );
        TestHelpers.makeRequest(dataClient, "PUT", "/" + indexName, ImmutableMap.of(), TestHelpers.toHttpEntity(mapping), null);
        waitForAossIndex(dataClient, indexName);
    }

    private void createAossHcadSourceIndex(RestClient dataClient, String indexName) throws IOException {
        String mapping = String
            .format(
                Locale.ROOT,
                "{\"mappings\":{\"properties\":{\"%s\":{\"type\":\"date\"},\"%s\":{\"type\":\"double\"},\"%s\":{\"type\":\"keyword\"}}}}",
                TIME_FIELD,
                VALUE_FIELD,
                HCAD_ENTITY_FIELD
            );
        TestHelpers.makeRequest(dataClient, "PUT", "/" + indexName, ImmutableMap.of(), TestHelpers.toHttpEntity(mapping), null);
        waitForAossIndex(dataClient, indexName);
    }

    private void waitForAossIndex(RestClient dataClient, String indexName) {
        Awaitility
            .await()
            .ignoreExceptionsMatching(this::isNotFoundResponse)
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                Response response = TestHelpers.makeRequest(dataClient, "HEAD", "/" + indexName, ImmutableMap.of(), "", null);
                assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
            });
    }

    private void createAossResultIndex(RestClient dataClient, String indexName) throws IOException {
        String body = "{\"mappings\":" + ADIndexManagement.getResultMappings() + "}";
        TestHelpers.makeRequest(dataClient, "PUT", "/" + indexName, ImmutableMap.of(), TestHelpers.toHttpEntity(body), null);
        waitForAossIndex(dataClient, indexName);
    }

    private void ingestAossSeries(RestClient dataClient, String indexName, Instant baseTime, int numberOfPoints) throws IOException {
        for (int i = 0; i < numberOfPoints; i++) {
            Instant timestamp = baseTime.plus(i, ChronoUnit.MINUTES).plusSeconds(30);
            String document = String.format(Locale.ROOT, "{\"%s\":%d,\"%s\":%d}", TIME_FIELD, timestamp.toEpochMilli(), VALUE_FIELD, i + 1);
            TestHelpers
                .makeRequest(dataClient, "POST", "/" + indexName + "/_doc", ImmutableMap.of(), TestHelpers.toHttpEntity(document), null);
        }
        waitForAossDocumentCount(dataClient, indexName, numberOfPoints);
    }

    private RestClient ingestAossHcadSeries(
        RestClient dataClient,
        String indexName,
        Instant baseTime,
        int intervalCount,
        int entityCount,
        int bulkBatchSize
    ) throws IOException {
        int expectedDocuments = intervalCount * entityCount;
        RestClient activeDataClient = dataClient;
        StringBuilder bulkBody = new StringBuilder();
        int pendingDocuments = 0;
        for (int interval = 0; interval < intervalCount; interval++) {
            long timestampMillis = baseTime.plus(interval, ChronoUnit.MINUTES).plusSeconds(30).toEpochMilli();
            for (int entity = 0; entity < entityCount; entity++) {
                String entityId = "entity-" + entity;
                bulkBody
                    .append("{\"index\":{\"_index\":\"")
                    .append(indexName)
                    .append("\",\"_id\":\"")
                    .append(interval)
                    .append('-')
                    .append(entity)
                    .append("\"}}\n");
                bulkBody
                    .append('{')
                    .append('"')
                    .append(TIME_FIELD)
                    .append("\":")
                    .append(timestampMillis)
                    .append(",\"")
                    .append(VALUE_FIELD)
                    .append("\":")
                    .append(interval + entity + 1)
                    .append(",\"")
                    .append(HCAD_ENTITY_FIELD)
                    .append("\":\"")
                    .append(entityId)
                    .append("\"}\n");
                pendingDocuments++;
                if (pendingDocuments >= bulkBatchSize) {
                    activeDataClient = flushAossBulk(activeDataClient, bulkBody);
                    pendingDocuments = 0;
                }
            }
        }
        if (pendingDocuments > 0) {
            activeDataClient = flushAossBulk(activeDataClient, bulkBody);
        }
        waitForAossDocumentCount(activeDataClient, indexName, expectedDocuments);
        return activeDataClient;
    }

    private RestClient flushAossBulk(RestClient dataClient, StringBuilder bulkBody) throws IOException {
        if (bulkBody.length() == 0) {
            return dataClient;
        }
        String body = bulkBody.toString();
        RestClient activeDataClient = dataClient;
        for (int attempt = 1; attempt <= AOSS_BULK_MAX_ATTEMPTS; attempt++) {
            try {
                Response response = TestHelpers
                    .makeRequest(
                        activeDataClient,
                        "POST",
                        "/_bulk",
                        ImmutableMap.of(),
                        new StringEntity(body, ContentType.create("application/x-ndjson")),
                        null
                    );
                Map<String, Object> responseMap = entityAsMap(response);
                assertFalse("AOSS bulk ingest failed: " + responseMap, Boolean.TRUE.equals(responseMap.get("errors")));
                bulkBody.setLength(0);
                return activeDataClient;
            } catch (IOException e) {
                if (attempt == AOSS_BULK_MAX_ATTEMPTS || !isRetryableAossBulkException(e)) {
                    throw e;
                }
                if (shouldRefreshAossBulkClient(e)) {
                    SigningRestClientProvider.closeAll();
                    activeDataClient = SigningRestClientProvider.getRestClient(customerEndpoint(), region(), "aoss");
                }
                LOG
                    .warn(
                        "AOSS bulk ingest transient failure on attempt {}/{}: {}; retrying",
                        attempt,
                        AOSS_BULK_MAX_ATTEMPTS,
                        e.toString()
                    );
                LOG.debug("AOSS bulk ingest transient failure stack", e);
                sleepBeforeAossBulkRetry(attempt);
            }
        }
        return activeDataClient;
    }

    private boolean isRetryableAossBulkException(IOException e) {
        if (e instanceof ResponseException) {
            ResponseException responseException = (ResponseException) e;
            int statusCode = responseException.getResponse() == null ? -1 : responseException.getResponse().getStatusLine().getStatusCode();
            return statusCode == HttpStatus.SC_REQUEST_TIMEOUT
                || statusCode == HttpStatus.SC_TOO_MANY_REQUESTS
                || statusCode == HttpStatus.SC_INTERNAL_SERVER_ERROR
                || statusCode == HttpStatus.SC_BAD_GATEWAY
                || statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE
                || statusCode == HttpStatus.SC_GATEWAY_TIMEOUT;
        }
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketException || cause instanceof SocketTimeoutException || cause instanceof UnknownHostException) {
                return true;
            }
            String message = cause.getMessage();
            if (message != null && isRetryableAossTransportMessage(message)) {
                return true;
            }
        }
        return false;
    }

    private boolean isRetryableAossTransportMessage(String message) {
        String lowerCaseMessage = message.toLowerCase(Locale.ROOT);
        return lowerCaseMessage.contains("broken pipe")
            || lowerCaseMessage.contains("connection reset")
            || lowerCaseMessage.contains("connection closed")
            || lowerCaseMessage.contains("connection is closed")
            || lowerCaseMessage.contains("network is unreachable")
            || lowerCaseMessage.contains("unknownhost")
            || lowerCaseMessage.contains("remote host terminated")
            || lowerCaseMessage.contains("premature end")
            || lowerCaseMessage.contains("read timed out")
            || lowerCaseMessage.contains("timed out");
    }

    private boolean shouldRefreshAossBulkClient(IOException e) {
        return !(e instanceof ResponseException);
    }

    private boolean isRetryableAwsSdkException(SdkException e) {
        String message = e.getMessage();
        if (message != null) {
            String lowerCaseMessage = message.toLowerCase(Locale.ROOT);
            if (lowerCaseMessage.contains("expired") || isRetryableAossTransportMessage(message)) {
                return true;
            }
        }
        for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketException || cause instanceof SocketTimeoutException || cause instanceof UnknownHostException) {
                return true;
            }
            String causeMessage = cause.getMessage();
            if (causeMessage != null && isRetryableAossTransportMessage(causeMessage)) {
                return true;
            }
        }
        return e instanceof SdkClientException;
    }

    private void sleepBeforeAossBulkRetry(int attempt) throws IOException {
        sleepBeforeAwsRetry(attempt);
    }

    private void sleepBeforeAwsRetry(int attempt) throws IOException {
        try {
            long baseDelaySeconds = Math.min(60L, 1L << Math.min(attempt, 5));
            long jitterMillis = randomLongBetween(250L, 1_249L);
            Thread.sleep(TimeUnit.SECONDS.toMillis(baseDelaySeconds) + jitterMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry AWS request", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void waitForAossDocumentCount(RestClient dataClient, String indexName, int expectedCount) {
        Awaitility
            .await()
            .ignoreExceptionsMatching(this::isNotFoundResponse)
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                String query = "{\"size\":0,\"track_total_hits\":true}";
                Response response = TestHelpers
                    .makeRequest(
                        dataClient,
                        "POST",
                        "/" + indexName + "/_search",
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(query),
                        null
                    );
                Map<String, Object> responseMap = entityAsMap(response);
                Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
                Map<String, Object> total = (Map<String, Object>) hits.get("total");
                assertTrue(
                    "Expected at least " + expectedCount + " searchable documents",
                    ((Number) total.get("value")).intValue() >= expectedCount
                );
            });
    }

    private void waitForAossFeatureQuery(RestClient dataClient, String indexName, Instant baseTime, int numberOfPoints) {
        long start = baseTime.toEpochMilli();
        long end = baseTime.plus(numberOfPoints, ChronoUnit.MINUTES).toEpochMilli();
        Awaitility
            .await()
            .ignoreExceptionsMatching(this::isRetryableAossSearchResponse)
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                String query = String
                    .format(
                        Locale.ROOT,
                        "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"%s\":{\"from\":%d,\"to\":%d,\"include_lower\":true,\"include_upper\":false,\"format\":\"epoch_millis\",\"boost\":1.0}}},{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"aggregations\":{\"sum_value\":{\"sum\":{\"field\":\"%s\"}}}}",
                        TIME_FIELD,
                        start,
                        end,
                        VALUE_FIELD
                    );
                Response response = TestHelpers
                    .makeRequest(
                        dataClient,
                        "POST",
                        "/" + indexName + "/_search",
                        ImmutableMap
                            .of(
                                "typed_keys",
                                "true",
                                "ignore_unavailable",
                                "false",
                                "expand_wildcards",
                                "open",
                                "allow_no_indices",
                                "true"
                            ),
                        TestHelpers.toHttpEntity(query),
                        null
                    );
                assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
            });
    }

    private boolean isNotFoundResponse(Throwable e) {
        return e instanceof ResponseException
            && ((ResponseException) e).getResponse() != null
            && ((ResponseException) e).getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND;
    }

    private boolean isRetryableAossSearchResponse(Throwable e) {
        if (e instanceof ResponseException == false || ((ResponseException) e).getResponse() == null) {
            return false;
        }
        int status = ((ResponseException) e).getResponse().getStatusLine().getStatusCode();
        return status == HttpStatus.SC_BAD_REQUEST || status == HttpStatus.SC_NOT_FOUND;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> latestSuccessfulResult(RestClient dataClient, String resultIndex, String detectorId) throws IOException {
        String query = String
            .format(
                Locale.ROOT,
                "{\"size\":1,\"query\":{\"bool\":{\"filter\":[{\"term\":{\"detector_id\":\"%s\"}}],\"must_not\":[{\"exists\":{\"field\":\"%s\"}}]}},\"sort\":[{\"execution_start_time\":{\"order\":\"desc\"}}]}",
                detectorId,
                CommonName.ERROR_FIELD
            );
        Response response = TestHelpers
            .makeRequest(dataClient, "POST", "/" + resultIndex + "/_search", ImmutableMap.of(), TestHelpers.toHttpEntity(query), null);
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> hitsObject = (Map<String, Object>) responseMap.get("hits");
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsObject.get("hits");
        if (hits == null || hits.isEmpty()) {
            return Map.of();
        }
        return (Map<String, Object>) hits.get(0).get("_source");
    }

    protected void sendSqsJobMessage(SqsClient sqsClient, AnomalyDetector detector, String tenantId, Instant scheduledTime)
        throws IOException {
        String messageGroupId = EventBridgeHandler.buildScheduleName(AnalysisType.AD, tenantId, detector.getId());
        SendMessageRequest request = SendMessageRequest
            .builder()
            .queueUrl(queueUrl())
            .messageGroupId(messageGroupId)
            .messageDeduplicationId(messageGroupId + "-" + scheduledTime.toEpochMilli())
            .messageBody(buildJobMessageBody(detector, tenantId, scheduledTime))
            .build();
        sendSqsMessageWithRetry(sqsClient, request);
    }

    private void sendSqsMessageWithRetry(SqsClient sqsClient, SendMessageRequest request) throws IOException {
        for (int attempt = 1; attempt <= SQS_SEND_MAX_ATTEMPTS; attempt++) {
            try {
                sqsClient.sendMessage(request);
                return;
            } catch (SdkException e) {
                if (attempt == SQS_SEND_MAX_ATTEMPTS || isRetryableAwsSdkException(e) == false) {
                    throw e;
                }
                LOG.warn("SQS send transient failure on attempt {}/{}: {}; retrying", attempt, SQS_SEND_MAX_ATTEMPTS, e.toString());
                sleepBeforeAwsRetry(attempt);
            }
        }
    }

    private String buildJobMessageBody(AnomalyDetector detector, String tenantId, Instant scheduledTime) throws IOException {
        Job job = new Job(
            detector.getId(),
            new IntervalSchedule(scheduledTime, 1, ChronoUnit.MINUTES),
            detector.getWindowDelay(),
            true,
            scheduledTime,
            null,
            scheduledTime,
            Duration.ofMinutes(1).getSeconds(),
            detector.getUser(),
            tenantId,
            detector.getCustomResultIndexOrAlias(),
            AnalysisType.AD
        );
        org.opensearch.core.xcontent.XContentBuilder builder = job.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
        Map<String, Object> jobMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
        jobMap.put(CommonName.EB_SCHEDULED_TIME_FIELD, scheduledTime.toString());

        org.opensearch.core.xcontent.XContentBuilder decoratedBuilder = JsonXContent.contentBuilder();
        decoratedBuilder.map(jobMap);
        return BytesReference.bytes(decoratedBuilder).utf8ToString();
    }

    private void awaitScheduleExists(SchedulerClient schedulerClient, String tenantId, String detectorId) {
        String scheduleName = EventBridgeHandler.buildScheduleName(AnalysisType.AD, tenantId, detectorId);
        Awaitility
            .await()
            .ignoreExceptionsMatching(e -> e instanceof ResourceNotFoundException)
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                GetScheduleResponse schedule = schedulerClient
                    .getSchedule(GetScheduleRequest.builder().groupName(schedulerGroup()).name(scheduleName).build());
                assertNotNull("Expected EventBridge schedule target for " + scheduleName, schedule.target());
                assertEquals(queueArn(), schedule.target().arn());
                assertEquals(sqsDeliveryRoleArn(), schedule.target().roleArn());
            });
    }

    private void assumeQueueHasRedrivePolicy(SqsClient sqsClient) {
        try {
            String redrivePolicy = sqsClient
                .getQueueAttributes(
                    GetQueueAttributesRequest.builder().queueUrl(queueUrl()).attributeNames(QueueAttributeName.REDRIVE_POLICY).build()
                )
                .attributes()
                .get(QueueAttributeName.REDRIVE_POLICY);
            Assume.assumeTrue("SQS queue must have a redrive policy", hasText(redrivePolicy));
        } catch (Exception e) {
            Assume.assumeNoException("SQS queue must be reachable", e);
        }
    }

    private DynamoDbClient dynamoDbClient() {
        return DynamoDbClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(Region.of(region()))
            .credentialsProvider(SecurityUtil.createCredentialsProvider())
            .build();
    }

    private SchedulerClient schedulerClient() {
        String scheduleManagementRoleArn = scheduleManagementRoleArn();
        return SchedulerClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(Region.of(region()))
            .credentialsProvider(
                scheduleManagementRoleArn == null
                    ? SecurityUtil.createCredentialsProvider()
                    : SecurityUtil
                        .createAssumeRoleCredentialsProvider(region(), scheduleManagementRoleArn, "ad-it-scheduler-" + firstSqsAccountId())
            )
            .build();
    }

    private SqsClient sqsClient() {
        return SqsClient
            .builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .region(Region.of(region()))
            .credentialsProvider(
                SecurityUtil.createAssumeRoleCredentialsProvider(region(), sqsDeliveryRoleArn(), "ad-it-sqs-" + firstSqsAccountId())
            )
            .build();
    }

    protected HashRingRevisionRef prepareModelNodeRouting(DynamoDbClient dynamoDbClient) {
        return ensureLatestHashRingRevisionTargetsModelNode(dynamoDbClient);
    }

    protected void cleanupModelNodeRouting(HashRingRevisionRef hashRingRevisionRef) {
        deleteHashRingRevisionIfPresent(hashRingRevisionRef);
    }

    private HashRingRevisionRef ensureLatestHashRingRevisionTargetsModelNode(DynamoDbClient dynamoDbClient) {
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

        long revisionId = Math.max(latestRevisionId + 1, Instant.now().toEpochMilli() * 1000L);
        long expiresAt = Instant.now().plus(3650, ChronoUnit.DAYS).getEpochSecond();

        dynamoDbClient
            .putItem(
                PutItemRequest
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
                    .build()
            );

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

    @Override
    protected List<Header> tenantHeaders(String tenantId) {
        return List
            .of(
                new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
                new BasicHeader(CommonName.TENANT_ID_HEADER, tenantId),
                new BasicHeader(EVENT_BRIDGE_CELL_ID_HEADER, firstSqsAccountId())
            );
    }

    private String queueUrl() {
        return "https://sqs." + region() + ".amazonaws.com/" + firstSqsAccountId() + "/" + queueName();
    }

    private String queueArn() {
        return "arn:aws:sqs:" + region() + ":" + firstSqsAccountId() + ":" + queueName();
    }

    private String queueName() {
        return System.getProperty(SQS_QUEUE_NAME_PROPERTY, AD_QUEUE_NAME);
    }

    private String firstSqsAccountId() {
        String configured = System.getProperty(SQS_ACCOUNT_IDS_PROPERTY, DEFAULT_EVENT_BRIDGE_CELL_ID);
        String normalized = configured.replace("[", "").replace("]", "").replace("\"", "");
        for (String candidate : normalized.split(",")) {
            String trimmed = candidate.trim();
            if (trimmed.isEmpty() == false) {
                return trimmed;
            }
        }
        return DEFAULT_EVENT_BRIDGE_CELL_ID;
    }

    private String scheduleManagementRoleArn() {
        String configuredRoleName = System.getProperty(SCHEDULE_MANAGEMENT_ROLE_NAME_PROPERTY);
        if (hasText(configuredRoleName) == false || "null".equals(configuredRoleName)) {
            return null;
        }
        return "arn:aws:iam::" + firstSqsAccountId() + ":role/" + configuredRoleName;
    }

    private String sqsDeliveryRoleArn() {
        return roleArn(SQS_DELIVERY_ROLE_NAME_PROPERTY, DEFAULT_SQS_DELIVERY_ROLE_NAME);
    }

    private String roleArn(String roleNameProperty, String defaultRoleName) {
        return "arn:aws:iam::" + firstSqsAccountId() + ":role/" + System.getProperty(roleNameProperty, defaultRoleName);
    }

    private int intProperty(String key, int defaultValue) {
        String value = System.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
    }

    private void waitForSqsQueueToDrain(SqsClient sqsClient) {
        Awaitility.await().atMost(Duration.ofMinutes(20)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            Map<QueueAttributeName, String> attributes;
            try {
                attributes = sqsClient
                    .getQueueAttributes(
                        GetQueueAttributesRequest
                            .builder()
                            .queueUrl(queueUrl())
                            .attributeNames(
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE
                            )
                            .build()
                    )
                    .attributes();
            } catch (SdkClientException e) {
                throw new AssertionError("Transient AWS client failure while waiting for autoscaling workload SQS queue to drain", e);
            }
            int visible = Integer.parseInt(attributes.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0"));
            int inFlight = Integer.parseInt(attributes.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0"));
            assertEquals("Expected autoscaling workload SQS queue to drain visible messages", 0, visible);
            assertEquals("Expected autoscaling workload SQS queue to drain in-flight messages", 0, inFlight);
        });
    }

    private void waitForSqsInFlightMessagesToDrain(SqsClient sqsClient) {
        Awaitility.await().atMost(Duration.ofMinutes(10)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
            Map<QueueAttributeName, String> attributes;
            try {
                attributes = sqsClient
                    .getQueueAttributes(
                        GetQueueAttributesRequest
                            .builder()
                            .queueUrl(queueUrl())
                            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                            .build()
                    )
                    .attributes();
            } catch (SdkClientException e) {
                throw new AssertionError(
                    "Transient AWS client failure while waiting for autoscaling workload SQS in-flight work to drain",
                    e
                );
            }
            int inFlight = Integer.parseInt(attributes.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0"));
            assertEquals("Expected autoscaling workload SQS in-flight messages to drain before detector deletion", 0, inFlight);
        });
    }

    private String customerEndpoint() {
        return System.getProperty(CUSTOMER_ENDPOINT_PROPERTY);
    }

    private String apiDataPlaneEndpoint() {
        return System.getProperty(API_ENDPOINT_PROPERTY);
    }

    private String region() {
        return System.getProperty(REGION_PROPERTY);
    }

    private String cloudMapTableName() {
        return System.getProperty(CLOUD_MAP_TABLE_PROPERTY);
    }

    private String cloudMapService() {
        return System.getProperty(CLOUD_MAP_SERVICE_PROPERTY);
    }

    private String schedulerGroup() {
        return EventBridgeHandler.resolveConfigScheduleGroup(System.getProperty(SCHEDULER_GROUP_PROPERTY), AnalysisType.AD);
    }

    private boolean hasText(String value) {
        return value != null && value.isBlank() == false;
    }

    private boolean isLoopbackEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return false;
        }
        URI uri = URI.create(endpoint);
        String host = uri.getHost();
        return "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host) || "::1".equals(host);
    }

    protected static final class HashRingRevisionRef {
        private final String partitionKey;
        private final long revisionId;

        private HashRingRevisionRef(String partitionKey, long revisionId) {
            this.partitionKey = partitionKey;
            this.revisionId = revisionId;
        }
    }

    protected final class AutoScalingWorkload {
        private final SqsClient sqsClient;
        private final String tenantId;
        private final Instant firstScheduledTime;
        private final int messagesPerDetector;
        private final int burstDurationSeconds;
        private final List<AnomalyDetector> detectors;
        private Supplier<RestClient> coordinatorClientSupplier = () -> client();
        private boolean triggered;
        private boolean stopped;

        private AutoScalingWorkload(
            SqsClient sqsClient,
            String tenantId,
            Instant firstScheduledTime,
            int messagesPerDetector,
            int burstDurationSeconds,
            List<AnomalyDetector> detectors
        ) {
            this.sqsClient = sqsClient;
            this.tenantId = tenantId;
            this.firstScheduledTime = firstScheduledTime;
            this.messagesPerDetector = messagesPerDetector;
            this.burstDurationSeconds = burstDurationSeconds;
            this.detectors = detectors;
        }

        protected AutoScalingWorkload coordinatorClientSupplier(Supplier<RestClient> coordinatorClientSupplier) {
            this.coordinatorClientSupplier = coordinatorClientSupplier;
            return this;
        }

        protected AutoScalingWorkload includeDetectorForCleanup(AnomalyDetector detector) {
            if (detector != null && detectors.stream().noneMatch(existing -> Objects.equals(existing.getId(), detector.getId()))) {
                detectors.add(detector);
            }
            return this;
        }

        protected void trigger() throws Exception {
            long burstDurationMillis = Math.max(0L, TimeUnit.SECONDS.toMillis(burstDurationSeconds));
            long startedAtMillis = System.currentTimeMillis();
            LOG
                .info(
                    "Triggering autoscaling SQS workload with {} detectors, {} messages per detector over {} seconds",
                    detectors.size(),
                    messagesPerDetector,
                    burstDurationSeconds
                );
            for (int message = 0; message < messagesPerDetector; message++) {
                Instant scheduledTime = firstScheduledTime.plus(message, ChronoUnit.MINUTES);
                for (AnomalyDetector detector : detectors) {
                    sendSqsJobMessage(sqsClient, detector, tenantId, scheduledTime);
                }
                sleepUntilNextBurstSlot(startedAtMillis, burstDurationMillis, message + 1);
            }
            triggered = true;
        }

        private void sleepUntilNextBurstSlot(long startedAtMillis, long burstDurationMillis, int completedMessageSlots)
            throws InterruptedException {
            if (burstDurationMillis == 0L || messagesPerDetector <= 1 || completedMessageSlots >= messagesPerDetector) {
                return;
            }
            long targetElapsedMillis = burstDurationMillis * completedMessageSlots / messagesPerDetector;
            long sleepMillis = startedAtMillis + targetElapsedMillis - System.currentTimeMillis();
            if (sleepMillis > 0L) {
                Thread.sleep(sleepMillis);
            }
        }

        protected void stop() throws Exception {
            if (stopped) {
                return;
            }
            stopped = true;
            Exception drainFailure = null;
            for (AnomalyDetector detector : detectors) {
                stopDetectorQuietly(detector);
            }
            if (triggered) {
                try {
                    waitForSqsInFlightMessagesToDrain(sqsClient);
                } catch (Exception e) {
                    drainFailure = e;
                }
            }
            for (AnomalyDetector detector : detectors) {
                deleteDetectorQuietly(detector);
            }
            if (triggered) {
                try {
                    waitForSqsQueueToDrain(sqsClient);
                } catch (Exception e) {
                    if (drainFailure == null) {
                        drainFailure = e;
                    }
                }
            }
            if (drainFailure != null) {
                throw drainFailure;
            }
        }

        protected void verifyCheckpointDeletion() {
            waitForAutoScalingCheckpointDeletion(tenantId, detectors);
        }

        private void stopDetectorQuietly(AnomalyDetector detector) {
            Awaitility.await().atMost(Duration.ofMinutes(8)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
                try {
                    stopTenantDetector(detector.getId(), tenantId, coordinatorClientSupplier.get());
                } catch (ResponseException e) {
                    if (isNotFound(e)) {
                        return;
                    }
                    throw new AssertionError("Failed to stop autoscaling workload detector " + detector.getId(), e);
                } catch (IOException | RuntimeException e) {
                    throw new AssertionError("Transient failure stopping autoscaling workload detector " + detector.getId(), e);
                }
            });
        }

        private void deleteDetectorQuietly(AnomalyDetector detector) {
            Awaitility.await().atMost(Duration.ofMinutes(8)).pollInterval(Duration.ofSeconds(5)).untilAsserted(() -> {
                try {
                    deleteTenantDetector(detector.getId(), tenantId, coordinatorClientSupplier.get());
                } catch (ResponseException e) {
                    if (isNotFound(e)) {
                        return;
                    }
                    throw new AssertionError("Failed to delete autoscaling workload detector " + detector.getId(), e);
                } catch (IOException | RuntimeException e) {
                    throw new AssertionError("Transient failure deleting autoscaling workload detector " + detector.getId(), e);
                }
            });
        }

        private boolean isNotFound(ResponseException e) {
            return e.getResponse() != null && e.getResponse().getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND;
        }
    }

    private void waitForAutoScalingCheckpointDeletion(String tenantId, List<AnomalyDetector> detectors) {
        String bucket = System.getProperty(S3_CHECKPOINT_BUCKET_PROPERTY, "");
        if (bucket.isBlank() || detectors.isEmpty() || region().isBlank()) {
            return;
        }
        try (
            S3Client s3Client = S3Client
                .builder()
                .region(Region.of(region()))
                .credentialsProvider(SecurityUtil.createCredentialsProvider())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()
        ) {
            Awaitility.await().atMost(Duration.ofMinutes(20)).pollInterval(Duration.ofSeconds(15)).untilAsserted(() -> {
                List<String> remainingPrefixes = new ArrayList<>();
                for (AnomalyDetector detector : detectors) {
                    String prefix = StringUtil.sanitizeId(tenantId) + "/" + detector.getId();
                    var response = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(1).build());
                    if (response.contents().isEmpty() == false) {
                        remainingPrefixes.add(prefix);
                    }
                }
                assertTrue(
                    "Expected stop/delete detector to delete S3 checkpoints for autoscaling workload, remaining prefixes="
                        + remainingPrefixes,
                    remainingPrefixes.isEmpty()
                );
            });
        }
    }

    @SuppressForbidden(reason = "Test-only local signing proxy uses the JDK HTTP server to forward REST requests through SigV4 signing.")
    private static final class SigningProxy implements AutoCloseable {
        private final HttpServer server;
        private final ExecutorService executor;

        private SigningProxy(HttpServer server, ExecutorService executor) {
            this.server = server;
            this.executor = executor;
        }

        private static SigningProxy start(URI listenUri, String targetEndpoint, String region) throws IOException {
            if (listenUri.getPort() <= 0) {
                throw new IllegalArgumentException("API data-plane proxy endpoint must include an explicit port: " + listenUri);
            }
            String host = listenUri.getHost() == null ? "127.0.0.1" : listenUri.getHost();
            HttpServer server = HttpServer.create(new InetSocketAddress(host, listenUri.getPort()), 0);
            ExecutorService executor = Executors.newCachedThreadPool();
            RestClient signedClient = SigningRestClientProvider.getRestClient(targetEndpoint, region, "aoss");
            server.createContext("/", exchange -> forward(exchange, signedClient));
            server.setExecutor(executor);
            server.start();
            return new SigningProxy(server, executor);
        }

        private static void forward(HttpExchange exchange, RestClient signedClient) throws IOException {
            try {
                Request request = new Request(exchange.getRequestMethod(), rawPathAndQuery(exchange.getRequestURI()));
                byte[] requestBody = exchange.getRequestBody().readAllBytes();
                if (requestBody.length > 0) {
                    request.setEntity(new ByteArrayEntity(requestBody, org.apache.hc.core5.http.ContentType.APPLICATION_JSON));
                }
                Response response = signedClient.performRequest(request);
                writeExchangeResponse(exchange, response);
            } catch (ResponseException e) {
                writeExchangeResponse(exchange, e.getResponse());
            } catch (Exception e) {
                byte[] body = e.toString().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, body.length);
                exchange.getResponseBody().write(body);
            } finally {
                exchange.close();
            }
        }

        private static String rawPathAndQuery(URI uri) {
            String path = uri.getRawPath() == null || uri.getRawPath().isBlank() ? "/" : uri.getRawPath();
            return uri.getRawQuery() == null ? path : path + "?" + uri.getRawQuery();
        }

        private static void writeExchangeResponse(HttpExchange exchange, Response response) throws IOException {
            byte[] body = response.getEntity() == null ? new byte[0] : EntityUtils.toByteArray(response.getEntity());
            for (Header header : response.getHeaders()) {
                if (HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(header.getName())) {
                    exchange.getResponseHeaders().add(header.getName(), header.getValue());
                }
            }
            boolean headRequest = "HEAD".equalsIgnoreCase(exchange.getRequestMethod());
            exchange.sendResponseHeaders(response.getStatusLine().getStatusCode(), headRequest ? -1 : body.length);
            if (!headRequest) {
                exchange.getResponseBody().write(body);
            }
        }

        @Override
        public void close() {
            server.stop(0);
            executor.shutdownNow();
        }
    }
}
