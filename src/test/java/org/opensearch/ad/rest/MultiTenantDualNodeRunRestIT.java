/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad.rest;

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.awaitility.Awaitility;
import org.junit.After;
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.util.SecurityUtil;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.DeleteScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException;

public class MultiTenantDualNodeRunRestIT extends AnomalyDetectorRestTestCase {

    private static final String INTERNAL_API_TOKEN = System
        .getProperty("tests.opensearch.plugins.timeseries.internal_api_shared_secret", "integration-test-internal-token");
    private static final String MODEL_CLUSTER_PROPERTY = "tests.model.rest.cluster";
    private static final String TENANT_ID = "tenant-dual-node";
    private static final String INDEX_NAME = "mt-dual-node-data";
    private static final String HC_ENTITY_FIELD = "entity";
    private static final String TIME_FIELD = "timestamp";
    private static final String VALUE_FIELD = "value";
    private static final String EVENT_BRIDGE_CELL_ID_HEADER = "x-eb-cell-id";
    private static final String EVENT_BRIDGE_CELL_ID = "793040377150";
    private static final String REGION_PROPERTY = "tests.opensearch.plugins.timeseries.region";
    private static final String SCHEDULER_GROUP_PROPERTY = "tests.opensearch.plugins.anomaly_detection.scheduler_group";
    private static final int DELETE_RETRY_TIMES = 3;

    private RestClient modelClient;
    private final List<TenantDetectorRef> tenantDetectorsToCleanup = new ArrayList<>();

    @After
    public void cleanupResources() throws Exception {
        try {
            cleanupTenantDetectors();
        } finally {
            if (modelClient != null) {
                modelClient.close();
                modelClient = null;
            }
        }
    }

    public void testRunSingleStreamDetectorAcrossCoordinatorAndModelNodes() throws Exception {
        RestClient modelClient = modelClient();
        TestHelpers.createIndexWithTimeField(modelClient, INDEX_NAME, TIME_FIELD);

        Instant baseTime = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(60, ChronoUnit.MINUTES);
        ingestSeries(modelClient, INDEX_NAME, baseTime, 50);

        Feature feature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", true);
        String customResultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        AnomalyDetector detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance(1)
            .setName("dual-node-mt-detector-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT))
            .setDescription("Local multi-tenant dual-node run IT")
            .setTimeField(TIME_FIELD)
            .setIndices(List.of(INDEX_NAME))
            .setFeatureAttributes(List.of(feature))
            .setFilterQuery(QueryBuilders.matchAllQuery())
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setWindowDelay(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setShingleSize(1)
            .setResultIndex(customResultIndex)
            .build();

        String detectorId = createTenantDetector(detector, TENANT_ID);

        Instant firstPeriodStart = baseTime.plus(48, ChronoUnit.MINUTES);
        Instant firstPeriodEnd = baseTime.plus(49, ChronoUnit.MINUTES);
        Awaitility.await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            Response response = runDetector(detectorId, firstPeriodStart, firstPeriodEnd, TENANT_ID);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
            assertTrue(fetchModelCount(modelClient, detectorId, TENANT_ID) > 0);
        });

        Instant secondPeriodStart = baseTime.plus(49, ChronoUnit.MINUTES);
        Instant secondPeriodEnd = baseTime.plus(50, ChronoUnit.MINUTES);
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            Response response = runDetector(detectorId, secondPeriodStart, secondPeriodEnd, TENANT_ID);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
            assertTrue(fetchDocumentCount(modelClient, customResultIndex) > 0);
        });
    }

    public void testDirectSingleStreamResultAndDeleteModelEndpointsOnModelNode() throws Exception {
        RestClient modelClient = modelClient();
        String tenantId = tenantId("single-stream");
        String indexName = "mt-direct-single-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        String detectorName = "direct-single-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        TestHelpers.createIndexWithTimeField(modelClient, indexName, TIME_FIELD);
        Instant baseTime = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(60, ChronoUnit.MINUTES);
        ingestSeries(modelClient, indexName, baseTime, 50);

        AnomalyDetector detector = createSingleStreamDetector(detectorName, indexName);
        String detectorId = createTenantDetector(detector, tenantId);

        Instant firstPeriodStart = baseTime.plus(48, ChronoUnit.MINUTES);
        Instant firstPeriodEnd = baseTime.plus(49, ChronoUnit.MINUTES);
        Awaitility.await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            Response response = runDetector(detectorId, firstPeriodStart, firstPeriodEnd, tenantId);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
            assertTrue(fetchModelCount(modelClient, detectorId, tenantId) > 0);
        });

        String modelId = fetchFirstModelId(modelClient, detectorId, tenantId);

        Instant periodStart = firstPeriodEnd;
        Instant periodEnd = periodStart.plus(1, ChronoUnit.MINUTES);
        Response singleStreamResponse = invokeSingleStreamResult(
            modelClient,
            detectorId,
            modelId,
            periodStart,
            periodEnd,
            tenantId,
            new double[] { 42.0 },
            TestHelpers.toJsonString(detector)
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(singleStreamResponse));
        assertEquals(Boolean.TRUE, parseResponse(singleStreamResponse).get("acknowledged"));

        Response deleteModelResponse = deleteModel(modelClient, detectorId, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteModelResponse));
        List<Map<String, Object>> deletedNodes = (List<Map<String, Object>>) parseResponse(deleteModelResponse).get("nodes");
        assertNotNull(deletedNodes);
        assertFalse(deletedNodes.isEmpty());

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertEquals(0L, fetchModelCount(modelClient, detectorId, tenantId));
        });
    }

    public void testDirectEntityResultAndHCImputeEndpointsOnModelNode() throws Exception {
        RestClient modelClient = modelClient();
        String tenantId = tenantId("entity");
        String indexName = "mt-direct-hc-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        String detectorName = "direct-hc-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        String entityValue = "host-a";

        createHCIndex(modelClient, indexName);
        Instant baseTime = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(90, ChronoUnit.MINUTES);
        ingestHCSeries(modelClient, indexName, baseTime, 60, entityValue);

        AnomalyDetector detector = createHCDetector(detectorName, indexName);
        String detectorId = createTenantDetector(modelClient, detector, tenantId);

        Instant periodStart = baseTime.plus(58, ChronoUnit.MINUTES);
        Instant periodEnd = periodStart.plus(1, ChronoUnit.MINUTES);
        Response entityResultResponse = invokeEntityResult(
            modelClient,
            detectorId,
            periodStart,
            periodEnd,
            tenantId,
            entityValue,
            new double[] { 59.0 }
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(entityResultResponse));
        assertEquals(Boolean.TRUE, parseResponse(entityResultResponse).get("acknowledged"));

        Awaitility.await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertTrue(fetchModelCount(modelClient, detectorId, tenantId) > 0);
        });

        Instant imputeStart = periodEnd;
        Instant imputeEnd = imputeStart.plus(1, ChronoUnit.MINUTES);
        Response hcImputeResponse = invokeHCImpute(client(), detectorId, imputeStart, imputeEnd, tenantId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(hcImputeResponse));
        List<Map<String, Object>> imputeNodes = (List<Map<String, Object>>) parseResponse(hcImputeResponse).get("nodes");
        assertNotNull(imputeNodes);
        assertTrue(imputeNodes.isEmpty() || imputeNodes.get(0).containsKey("exception") == false);
    }

    private RestClient modelClient() throws IOException {
        if (modelClient == null) {
            modelClient = buildClient(restClientSettings(), modelHosts());
        }
        return modelClient;
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

    private void ingestSeries(RestClient client, String indexName, Instant baseTime, int numberOfPoints) throws IOException {
        for (int i = 0; i < numberOfPoints; i++) {
            Instant timestamp = baseTime.plus(i, ChronoUnit.MINUTES).plusSeconds(30);
            String document = String.format(Locale.ROOT, "{\"%s\":%d,\"%s\":%d}", TIME_FIELD, timestamp.toEpochMilli(), VALUE_FIELD, i + 1);
            TestHelpers.ingestDataToIndex(client, indexName, TestHelpers.toHttpEntity(document));
        }
    }

    private void createHCIndex(RestClient client, String indexName) throws IOException {
        String mapping = String
            .format(
                Locale.ROOT,
                "{\"properties\":{\"%s\":{\"type\":\"keyword\"},\"%s\":{\"type\":\"double\"},\"%s\":{\"type\":\"date\"}}}",
                HC_ENTITY_FIELD,
                VALUE_FIELD,
                TIME_FIELD
            );
        TestHelpers.createEmptyIndex(client, indexName);
        TestHelpers.createIndexMapping(client, indexName, TestHelpers.toHttpEntity(mapping));
    }

    private void ingestHCSeries(RestClient client, String indexName, Instant baseTime, int numberOfPoints, String entityValue)
        throws IOException {
        for (int i = 0; i < numberOfPoints; i++) {
            Instant timestamp = baseTime.plus(i, ChronoUnit.MINUTES).plusSeconds(30);
            String document = String
                .format(
                    Locale.ROOT,
                    "{\"%s\":\"%s\",\"%s\":%d,\"%s\":%d}",
                    HC_ENTITY_FIELD,
                    entityValue,
                    TIME_FIELD,
                    timestamp.toEpochMilli(),
                    VALUE_FIELD,
                    i + 1
                );
            TestHelpers.ingestDataToIndex(client, indexName, TestHelpers.toHttpEntity(document));
        }
    }

    private AnomalyDetector createSingleStreamDetector(String detectorName, String indexName) throws IOException {
        Feature feature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", true);
        String customResultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        return TestHelpers.AnomalyDetectorBuilder
            .newInstance(1)
            .setName(detectorName)
            .setDescription("Direct single-stream internal REST IT")
            .setTimeField(TIME_FIELD)
            .setIndices(List.of(indexName))
            .setFeatureAttributes(List.of(feature))
            .setFilterQuery(QueryBuilders.matchAllQuery())
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setWindowDelay(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setShingleSize(1)
            .setResultIndex(customResultIndex)
            .build();
    }

    private AnomalyDetector createHCDetector(String detectorName, String indexName) throws IOException {
        Feature feature = TestHelpers.randomFeature("sum_value", VALUE_FIELD, "sum", true);
        String customResultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        return TestHelpers.AnomalyDetectorBuilder
            .newInstance(1)
            .setName(detectorName)
            .setDescription("Direct HCAD internal REST IT")
            .setTimeField(TIME_FIELD)
            .setIndices(List.of(indexName))
            .setFeatureAttributes(List.of(feature))
            .setFilterQuery(QueryBuilders.matchAllQuery())
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setWindowDelay(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setShingleSize(1)
            .setCategoryFields(List.of(HC_ENTITY_FIELD))
            .setResultIndex(customResultIndex)
            .build();
    }

    private String createTenantDetector(AnomalyDetector detector, String tenantId) throws Exception {
        return createTenantDetector(client(), detector, tenantId);
    }

    private String createTenantDetector(RestClient client, AnomalyDetector detector, String tenantId) throws Exception {
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI,
                Map.<String, String>of(),
                TestHelpers.toHttpEntity(detector),
                tenantHeaders(tenantId)
            );
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        String detectorId = (String) parseResponse(response).get("_id");
        trackTenantDetector(detectorId, tenantId);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            Response getResponse = TestHelpers
                .makeRequest(
                    client,
                    "GET",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId,
                    Map.<String, String>of(),
                    "",
                    tenantHeaders(tenantId)
                );
            assertEquals(RestStatus.OK, TestHelpers.restStatus(getResponse));
        });

        return detectorId;
    }

    private void trackTenantDetector(String detectorId, String tenantId) {
        if (detectorId != null && detectorId.isBlank() == false && tenantId != null && tenantId.isBlank() == false) {
            tenantDetectorsToCleanup.add(new TenantDetectorRef(detectorId, tenantId));
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
                Map.<String, String>of(),
                TestHelpers.toHttpEntity(requestBody),
                tenantHeaders(tenantId)
            );
    }

    private Response invokeSingleStreamResult(
        RestClient client,
        String detectorId,
        String modelId,
        Instant periodStart,
        Instant periodEnd,
        String tenantId,
        double[] datapoint,
        String configJson
    ) throws IOException {
        String requestBody = String
            .format(
                Locale.ROOT,
                "{\"model_id\":\"%s\",\"start\":%d,\"end\":%d,\"value_list\":[%.2f],\"tenant_id\":\"%s\",\"config_json\":%s}",
                modelId,
                periodStart.toEpochMilli(),
                periodEnd.toEpochMilli(),
                datapoint[0],
                tenantId,
                configJson
            );
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_INTERNAL_DETECTORS_URI + "/" + detectorId + "/_single_stream_result",
                Map.<String, String>of(),
                TestHelpers.toHttpEntity(requestBody),
                internalTenantHeaders(tenantId)
            );
    }

    private Response invokeEntityResult(
        RestClient client,
        String detectorId,
        Instant periodStart,
        Instant periodEnd,
        String tenantId,
        String entityValue,
        double[] datapoint
    ) throws IOException {
        String requestBody = String
            .format(
                Locale.ROOT,
                "{\"start\":%d,\"end\":%d,\"tenant_id\":\"%s\",\"entities\":[{\"entity\":[{\"name\":\"%s\",\"value\":\"%s\"}],\"value\":[%.2f]}]}",
                periodStart.toEpochMilli(),
                periodEnd.toEpochMilli(),
                tenantId,
                HC_ENTITY_FIELD,
                entityValue,
                datapoint[0]
            );
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_INTERNAL_DETECTORS_URI + "/" + detectorId + "/_entity_result",
                Map.<String, String>of(),
                TestHelpers.toHttpEntity(requestBody),
                internalTenantHeaders(tenantId)
            );
    }

    private Response invokeHCImpute(RestClient client, String detectorId, Instant dataStart, Instant dataEnd, String tenantId)
        throws IOException {
        String requestBody = String
            .format(
                Locale.ROOT,
                "{\"tenant_id\":\"%s\",\"data_start_millis\":%d,\"data_end_millis\":%d}",
                tenantId,
                dataStart.toEpochMilli(),
                dataEnd.toEpochMilli()
            );
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_INTERNAL_DETECTORS_URI + "/" + detectorId + "/_hc_impute",
                Map.<String, String>of(),
                TestHelpers.toHttpEntity(requestBody),
                internalTenantHeaders(tenantId)
            );
    }

    private Response deleteModel(RestClient client, String detectorId, String tenantId) throws IOException {
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_INTERNAL_DETECTORS_URI + "/" + detectorId + "/_delete_model",
                Map.<String, String>of(),
                "",
                internalTenantHeaders(tenantId)
            );
    }

    private long fetchModelCount(RestClient client, String detectorId, String tenantId) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_INTERNAL_DETECTORS_URI + "/" + detectorId + "/_node_profile/models",
                Map.<String, String>of(),
                "",
                internalTenantHeaders(tenantId)
            );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        Object modelCount = parseResponse(response).get(CommonName.MODEL_COUNT);
        return modelCount == null ? 0L : ((Number) modelCount).longValue();
    }

    @SuppressWarnings("unchecked")
    private String fetchFirstModelId(RestClient client, String detectorId, String tenantId) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_INTERNAL_DETECTORS_URI + "/" + detectorId + "/_node_profile/models",
                Map.<String, String>of(),
                "",
                internalTenantHeaders(tenantId)
            );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        List<Map<String, Object>> models = (List<Map<String, Object>>) parseResponse(response).get(CommonName.MODELS);
        assertNotNull(models);
        assertFalse(models.isEmpty());
        return (String) models.get(0).get(CommonName.MODEL_ID_FIELD);
    }

    private long fetchDocumentCount(RestClient client, String indexName) throws IOException {
        Response response = TestHelpers.makeRequest(client, "GET", "/" + indexName + "/_count", Map.<String, String>of(), "", null);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        return ((Number) parseResponse(response).get("count")).longValue();
    }

    private Map<String, Object> parseResponse(Response response) throws IOException {
        return jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent())
            .map();
    }

    private List<Header> tenantHeaders(String tenantId) {
        return List
            .of(
                new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
                new BasicHeader(CommonName.TENANT_ID_HEADER, tenantId),
                new BasicHeader(EVENT_BRIDGE_CELL_ID_HEADER, EVENT_BRIDGE_CELL_ID)
            );
    }

    private List<Header> internalTenantHeaders(String tenantId) {
        return List
            .of(
                new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
                new BasicHeader(CommonName.TENANT_ID_HEADER, tenantId),
                new BasicHeader(CommonName.INTERNAL_API_TOKEN_HEADER, INTERNAL_API_TOKEN)
            );
    }

    private String tenantId(String suffix) {
        return TENANT_ID + "-" + suffix + "-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
    }

    private void cleanupTenantDetectors() throws Exception {
        if (tenantDetectorsToCleanup.isEmpty()) {
            return;
        }

        SchedulerClient schedulerClient = schedulerClient();
        try {
            for (int i = tenantDetectorsToCleanup.size() - 1; i >= 0; i--) {
                cleanupTenantDetector(tenantDetectorsToCleanup.get(i), schedulerClient);
            }
        } finally {
            if (schedulerClient != null) {
                schedulerClient.close();
            }
            tenantDetectorsToCleanup.clear();
        }
    }

    private void cleanupTenantDetector(TenantDetectorRef detectorRef, SchedulerClient schedulerClient) throws Exception {
        try {
            stopTenantDetectorIfPresent(detectorRef);
            for (int attempt = 0; attempt < DELETE_RETRY_TIMES; attempt++) {
                if (deleteTenantDetectorIfPresent(detectorRef)) {
                    return;
                }
                stopTenantDetectorIfPresent(detectorRef);
                Thread.sleep(1000L);
            }
            LOG.warn("Failed to clean up multi-tenant detector {}", detectorRef.detectorId);
        } finally {
            deleteTenantScheduleIfPresent(detectorRef, schedulerClient);
        }
    }

    private void stopTenantDetectorIfPresent(TenantDetectorRef detectorRef) throws Exception {
        try {
            TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorRef.detectorId + "/_stop",
                    Map.<String, String>of(),
                    "",
                    tenantHeaders(detectorRef.tenantId)
                );
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                LOG.warn("Failed to stop multi-tenant detector {} during cleanup", detectorRef.detectorId, e);
            }
        }
    }

    private boolean deleteTenantDetectorIfPresent(TenantDetectorRef detectorRef) throws Exception {
        try {
            TestHelpers
                .makeRequest(
                    client(),
                    "DELETE",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorRef.detectorId,
                    Map.<String, String>of(),
                    "",
                    tenantHeaders(detectorRef.tenantId)
                );
            return true;
        } catch (ResponseException e) {
            int statusCode = e.getResponse().getStatusLine().getStatusCode();
            if (statusCode == 404) {
                return true;
            }
            if (statusCode == 400 && e.getMessage().contains("Job is running")) {
                return false;
            }
            LOG.warn("Failed to delete multi-tenant detector {} during cleanup", detectorRef.detectorId, e);
            return false;
        }
    }

    private SchedulerClient schedulerClient() {
        String region = System.getProperty(REGION_PROPERTY);
        if (region == null || region.isBlank()) {
            return null;
        }
        return SchedulerClient.builder().region(Region.of(region)).credentialsProvider(SecurityUtil.createCredentialsProvider()).build();
    }

    private void deleteTenantScheduleIfPresent(TenantDetectorRef detectorRef, SchedulerClient schedulerClient) {
        if (schedulerClient == null) {
            return;
        }

        String scheduleName = EventBridgeHandler.buildScheduleName(AnalysisType.AD, detectorRef.tenantId, detectorRef.detectorId);
        DeleteScheduleRequest request = DeleteScheduleRequest.builder().groupName(schedulerGroup()).name(scheduleName).build();
        try {
            schedulerClient.deleteSchedule(request);
        } catch (ResourceNotFoundException e) {
            LOG.info("EventBridge Scheduler trigger {} not found for detector {}", scheduleName, detectorRef.detectorId);
        } catch (Exception e) {
            LOG.warn("Failed to remove EventBridge Scheduler trigger {} during cleanup", scheduleName, e);
        }
    }

    private String schedulerGroup() {
        return EventBridgeHandler.resolveConfigScheduleGroup(System.getProperty(SCHEDULER_GROUP_PROPERTY), AnalysisType.AD);
    }

    private static final class TenantDetectorRef {
        private final String detectorId;
        private final String tenantId;

        private TenantDetectorRef(String detectorId, String tenantId) {
            this.detectorId = detectorId;
            this.tenantId = tenantId;
        }
    }
}
