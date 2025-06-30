/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.awaitility.Awaitility;
import org.junit.Assume;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.StringUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

public class MultiTenantADRealTimeInferencerIT extends AbstractMultiTenantAnomalyDetectorRestTestCase {
    private static final String MODEL_CLUSTER_PROPERTY = "tests.model.rest.cluster";
    private static final String INTERNAL_API_TOKEN = System
        .getProperty("tests.opensearch.plugins.timeseries.internal_api_shared_secret", "integration-test-internal-token");
    private static final int INTERVAL_MINUTES = 1;
    private static final long INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(INTERVAL_MINUTES);
    private static final int SHINGLE_SIZE = 8;
    private static final int CHECKPOINT_BUCKET = 320;
    private static final int GAP_BUCKETS = 36;
    private static final int POST_RESTORE_BUCKETS = 80;
    private static final int FIRST_GAP_BUCKET = CHECKPOINT_BUCKET + 1;
    private static final int FIRST_POST_RESTORE_BUCKET = FIRST_GAP_BUCKET + GAP_BUCKETS;
    private static final Instant BASE_TIME = Instant.parse("2025-01-01T00:00:00Z");

    public void testCatchUpReplayPreservesLongTermPrecisionAndRecallAfterCheckpointRestoreInMultiTenantMode() throws Exception {
        Assume.assumeTrue("S3 checkpoint bucket must be configured", hasText(bucketName()));
        Assume.assumeTrue("AWS region must be configured", hasText(region()));

        String tenantId = tenantId("replay");
        String dataIndex = indexName("replay-data");

        try (RestClient modelClient = buildClient(restClientSettings(), modelHosts()); S3Client s3Client = s3Client()) {
            ingestSyntheticDataWithGapBuckets(modelClient, dataIndex);

            Feature feature = sumFeature("total_value", VALUE_FIELD, "value");
            AnomalyDetector seedDetector = createTenantDetector(detector("seed-detector", dataIndex, feature), tenantId);
            String seedModelId = SingleStreamModelIdMapper.getRcfModelId(tenantId, seedDetector.getId(), 0);
            runAndWaitForResult(modelClient, seedDetector, seedModelId, CHECKPOINT_BUCKET, tenantId);
            waitForCheckpoint(s3Client, tenantId, seedDetector.getId(), seedModelId);

            AnomalyDetector continuousDetector = createTenantDetector(detector("continuous-detector", dataIndex, feature), tenantId);
            String continuousModelId = SingleStreamModelIdMapper.getRcfModelId(tenantId, continuousDetector.getId(), 0);

            AnomalyDetector catchUpDetector = createTenantDetector(detector("catchup-detector", dataIndex, feature), tenantId);
            String catchUpModelId = SingleStreamModelIdMapper.getRcfModelId(tenantId, catchUpDetector.getId(), 0);

            copyCheckpoint(s3Client, tenantId, seedDetector.getId(), seedModelId, continuousDetector.getId(), continuousModelId);
            copyCheckpoint(s3Client, tenantId, seedDetector.getId(), seedModelId, catchUpDetector.getId(), catchUpModelId);

            DecisionMetrics continuousMetrics = new DecisionMetrics();
            List<Boolean> continuousDecisions = new ArrayList<>();
            for (int bucket = FIRST_GAP_BUCKET; bucket < FIRST_POST_RESTORE_BUCKET + POST_RESTORE_BUCKETS; bucket++) {
                Map<String, Object> result = runAndWaitForResult(modelClient, continuousDetector, continuousModelId, bucket, tenantId);
                if (bucket >= FIRST_POST_RESTORE_BUCKET) {
                    boolean positive = isPositive(result);
                    continuousMetrics.record(isInjectedAnomaly(bucket - FIRST_POST_RESTORE_BUCKET), positive);
                    continuousDecisions.add(positive);
                }
            }

            DecisionMetrics catchUpMetrics = new DecisionMetrics();
            for (int offset = 0; offset < POST_RESTORE_BUCKETS; offset++) {
                int bucket = FIRST_POST_RESTORE_BUCKET + offset;
                Map<String, Object> result = runAndWaitForResult(modelClient, catchUpDetector, catchUpModelId, bucket, tenantId);
                boolean positive = isPositive(result);
                assertEquals(
                    "Catch-up replay should match the continuous model decision at post-restore offset " + offset,
                    continuousDecisions.get(offset),
                    positive
                );
                catchUpMetrics.record(isInjectedAnomaly(offset), positive);
            }

            assertTrue("The catch-up window must include missing buckets", missingGapBucketCount() > 0);
            assertTrue("The labeled post-restore stream must contain anomalies", continuousMetrics.positiveLabels > 0);
            assertTrue("The continuous detector should detect at least one labeled anomaly", continuousMetrics.truePositives > 0);
            assertEquals(continuousMetrics.truePositives, catchUpMetrics.truePositives);
            assertEquals(continuousMetrics.falsePositives, catchUpMetrics.falsePositives);
            assertEquals(continuousMetrics.falseNegatives, catchUpMetrics.falseNegatives);
            assertEquals(continuousMetrics.trueNegatives, catchUpMetrics.trueNegatives);
            assertEquals(continuousMetrics.precision(), catchUpMetrics.precision(), 0.0);
            assertEquals(continuousMetrics.recall(), catchUpMetrics.recall(), 0.0);
        }
    }

    private void ingestSyntheticDataWithGapBuckets(RestClient modelClient, String indexName) throws IOException {
        createTestDataIndex(modelClient, indexName);
        StringBuilder bulkBody = new StringBuilder();
        int docs = 0;
        int endBucket = FIRST_POST_RESTORE_BUCKET + POST_RESTORE_BUCKETS;
        for (int bucket = 0; bucket < endBucket; bucket++) {
            if (isMissingGapBucket(bucket)) {
                continue;
            }
            bulkBody.append("{\"index\":{\"_index\":\"").append(indexName).append("\"}}\n");
            bulkBody
                .append(
                    String
                        .format(
                            Locale.ROOT,
                            "{\"%s\":%d,\"%s\":%s,\"type\":\"single-stream\"}\n",
                            TIME_FIELD,
                            bucketStart(bucket).toEpochMilli(),
                            VALUE_FIELD,
                            Double.toString(valueForBucket(bucket))
                        )
                );
            docs++;
        }
        assertTrue("test data should contain gap buckets", docs < endBucket);
        Response response = TestHelpers
            .makeRequest(modelClient, "POST", "_bulk?refresh=true", Map.of(), TestHelpers.toHttpEntity(bulkBody.toString()), null);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        assertEquals(docs, countDocs(modelClient, indexName));
    }

    private void createTestDataIndex(RestClient modelClient, String indexName) throws IOException {
        TestHelpers.createEmptyIndex(modelClient, indexName);
        String mappings = "{\"properties\":{\""
            + TIME_FIELD
            + "\":{\"type\":\"date\",\"format\":\"strict_date_time||epoch_millis\"},\""
            + VALUE_FIELD
            + "\":{\"type\":\"double\"},\"type\":{\"type\":\"keyword\"}}}";
        TestHelpers.createIndexMapping(modelClient, indexName, TestHelpers.toHttpEntity(mappings));
    }

    private long countDocs(RestClient modelClient, String indexName) throws IOException {
        Response response = TestHelpers.makeRequest(modelClient, "GET", "/" + indexName + "/_count", Map.of(), "", null);
        return ((Number) entityAsMap(response).get("count")).longValue();
    }

    private AnomalyDetector detector(String name, String indexName, Feature feature) {
        return new AnomalyDetector(
            null,
            0L,
            name,
            "checkpoint replay multi-tenant detector",
            TIME_FIELD,
            ImmutableList.of(indexName),
            ImmutableList.of(feature),
            QueryBuilders.matchAllQuery(),
            new IntervalTimeConfiguration(INTERVAL_MINUTES, ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
            SHINGLE_SIZE,
            null,
            0,
            Instant.now(),
            null,
            null,
            resultIndexName(),
            new ImputationOption(ImputationMethod.PREVIOUS),
            TimeSeriesSettings.DEFAULT_RECENCY_EMPHASIS,
            null,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            null,
            null,
            null,
            null,
            null,
            null,
            new IntervalTimeConfiguration(INTERVAL_MINUTES, ChronoUnit.MINUTES),
            null,
            null
        );
    }

    private Feature sumFeature(String aggregationName, String fieldName, String featureName) throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers
            .parseAggregation("{\"" + aggregationName + "\":{\"sum\":{\"field\":\"" + fieldName + "\"}}}");
        return new Feature("replay_feature", featureName, true, aggregationBuilder);
    }

    private Map<String, Object> runAndWaitForResult(
        RestClient modelClient,
        AnomalyDetector detector,
        String modelId,
        int bucket,
        String tenantId
    ) throws Exception {
        long start = bucketStart(bucket).toEpochMilli();
        Response response = TestHelpers
            .makeRequest(
                modelClient,
                "POST",
                TestHelpers.AD_BASE_INTERNAL_DETECTORS_URI + "/" + detector.getId() + "/" + RestHandlerUtils.SINGLE_STREAM_RESULT,
                ImmutableMap.of(),
                singleStreamRequestBody(modelId, start, start + INTERVAL_MILLIS, featureForBucket(bucket), tenantId),
                internalTenantHeaders(tenantId)
            );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        return waitForResult(modelClient, detector, start + INTERVAL_MILLIS, tenantId);
    }

    private String singleStreamRequestBody(String modelId, long start, long end, double[] valueList, String tenantId) {
        return String
            .format(
                Locale.ROOT,
                "{\"%s\":\"%s\",\"%s\":%d,\"%s\":%d,\"%s\":[%s],\"%s\":\"%s\"}",
                CommonName.MODEL_ID_KEY,
                modelId,
                CommonName.START_JSON_KEY,
                start,
                CommonName.END_JSON_KEY,
                end,
                CommonName.VALUE_LIST_FIELD,
                valueListJson(valueList),
                CommonName.TENANT_ID_FIELD,
                tenantId
            );
    }

    private String valueListJson(double[] valueList) {
        List<String> values = new ArrayList<>(valueList.length);
        for (double value : valueList) {
            values.add(Double.isNaN(value) ? "\"NaN\"" : Double.toString(value));
        }
        return String.join(",", values);
    }

    private Map<String, Object> waitForResult(RestClient modelClient, AnomalyDetector detector, long dataEndTimeMillis, String tenantId)
        throws Exception {
        @SuppressWarnings("unchecked")
        final Map<String, Object>[] result = new Map[1];
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            TestHelpers.makeRequest(modelClient, "POST", "/" + detector.getCustomResultIndexOrAlias() + "/_refresh", Map.of(), "", null);
            Response response = searchResult(modelClient, detector, dataEndTimeMillis, tenantId);
            List<Map<String, Object>> hits = searchHits(response);
            assertFalse("expected one anomaly result for detector " + detector.getId() + " at " + dataEndTimeMillis, hits.isEmpty());
            result[0] = source(hits.get(0));
        });
        return result[0];
    }

    private Response searchResult(RestClient modelClient, AnomalyDetector detector, long dataEndTimeMillis, String tenantId)
        throws IOException {
        String query = String
            .format(
                Locale.ROOT,
                "{"
                    + "\"query\":{\"bool\":{\"filter\":["
                    + "{\"term\":{\"%s\":\"%s\"}},"
                    + "{\"range\":{\"%s\":{\"gte\":%d,\"lte\":%d}}}"
                    + "]}},"
                    + "\"sort\":[{\"%s\":{\"order\":\"desc\"}}],"
                    + "\"size\":1"
                    + "}",
                AnomalyResult.DETECTOR_ID_FIELD,
                detector.getId(),
                CommonName.DATA_END_TIME_FIELD,
                dataEndTimeMillis,
                dataEndTimeMillis,
                CommonName.EXECUTION_END_TIME_FIELD
            );
        return TestHelpers
            .makeRequest(
                modelClient,
                "POST",
                "/" + detector.getCustomResultIndexOrAlias() + "/_search",
                Map.of(),
                query,
                tenantHeaders(tenantId)
            );
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> searchHits(Response response) throws IOException {
        return (List<Map<String, Object>>) ((Map<String, Object>) entityAsMap(response).get("hits")).get("hits");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> source(Map<String, Object> hit) {
        return (Map<String, Object>) hit.get("_source");
    }

    private void waitForCheckpoint(S3Client s3Client, String tenantId, String detectorId, String modelId) {
        String key = checkpointKey(tenantId, detectorId, modelId);
        Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertTrue("checkpoint should exist at s3://" + bucketName() + "/" + key, objectExists(s3Client, key));
        });
    }

    private void copyCheckpoint(
        S3Client s3Client,
        String tenantId,
        String sourceDetectorId,
        String sourceModelId,
        String targetDetectorId,
        String targetModelId
    ) {
        String sourceKey = checkpointKey(tenantId, sourceDetectorId, sourceModelId);
        String targetKey = checkpointKey(tenantId, targetDetectorId, targetModelId);
        s3Client
            .copyObject(CopyObjectRequest.builder().copySource(bucketName() + "/" + sourceKey).bucket(bucketName()).key(targetKey).build());
        assertTrue("copied checkpoint should exist at s3://" + bucketName() + "/" + targetKey, objectExists(s3Client, targetKey));
    }

    private boolean objectExists(S3Client s3Client, String key) {
        return s3Client
            .listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName()).prefix(key).build())
            .contents()
            .stream()
            .anyMatch(object -> key.equals(object.key()));
    }

    private String checkpointKey(String tenantId, String detectorId, String modelId) {
        return StringUtil.sanitizeId(tenantId) + "/" + detectorId + "/" + modelId;
    }

    private double[] featureForBucket(int bucket) {
        return isMissingGapBucket(bucket) ? new double[] { Double.NaN } : new double[] { valueForBucket(bucket) };
    }

    private double valueForBucket(int bucket) {
        return isInjectedAnomaly(bucket - FIRST_POST_RESTORE_BUCKET) ? anomalyValue(bucket) : normalValue(bucket);
    }

    private double normalValue(int bucket) {
        double daily = Math.sin(2 * Math.PI * bucket / 1440.0);
        double shortCycle = Math.cos(2 * Math.PI * bucket / 37.0);
        double deterministicNoise = 0.2 * Math.sin(bucket * 12.9898 + 78.233);
        return 100 + 8 * daily + 2 * shortCycle + deterministicNoise;
    }

    private double anomalyValue(int bucket) {
        return normalValue(bucket) + 140.0;
    }

    private boolean isInjectedAnomaly(int postRestoreOffset) {
        return postRestoreOffset == 12 || postRestoreOffset == 13 || postRestoreOffset == 50 || postRestoreOffset == 51;
    }

    private boolean isMissingGapBucket(int bucket) {
        return bucket >= FIRST_GAP_BUCKET && bucket < FIRST_POST_RESTORE_BUCKET && (bucket - FIRST_GAP_BUCKET) % 6 == 3;
    }

    private int missingGapBucketCount() {
        int count = 0;
        for (int bucket = FIRST_GAP_BUCKET; bucket < FIRST_POST_RESTORE_BUCKET; bucket++) {
            if (isMissingGapBucket(bucket)) {
                count++;
            }
        }
        return count;
    }

    private Instant bucketStart(int bucket) {
        return BASE_TIME.plus(bucket * INTERVAL_MINUTES, ChronoUnit.MINUTES);
    }

    private boolean isPositive(Map<String, Object> result) {
        return ((Number) result.get(AnomalyResult.ANOMALY_GRADE_FIELD)).doubleValue() > 0;
    }

    private List<Header> internalTenantHeaders(String tenantId) {
        return List
            .of(
                new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
                new BasicHeader(CommonName.TENANT_ID_HEADER, tenantId),
                new BasicHeader(CommonName.INTERNAL_API_TOKEN_HEADER, INTERNAL_API_TOKEN)
            );
    }

    private S3Client s3Client() {
        return S3Client.builder().region(Region.of(region())).credentialsProvider(SecurityUtil.createCredentialsProvider()).build();
    }

    private String bucketName() {
        return System.getProperty("tests.opensearch.plugins.anomaly_detection.s3_checkpoint_bucket");
    }

    private String region() {
        return System.getProperty("tests.opensearch.plugins.timeseries.region");
    }

    private boolean hasText(String value) {
        return value != null && value.isBlank() == false;
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

    private static class DecisionMetrics {
        private int positiveLabels;
        private int truePositives;
        private int falsePositives;
        private int falseNegatives;
        private int trueNegatives;

        private void record(boolean expectedAnomaly, boolean positiveDecision) {
            if (expectedAnomaly) {
                positiveLabels++;
            }
            if (expectedAnomaly && positiveDecision) {
                truePositives++;
            } else if (!expectedAnomaly && positiveDecision) {
                falsePositives++;
            } else if (expectedAnomaly) {
                falseNegatives++;
            } else {
                trueNegatives++;
            }
        }

        private double precision() {
            int positiveDecisions = truePositives + falsePositives;
            return positiveDecisions == 0 ? 0 : (double) truePositives / positiveDecisions;
        }

        private double recall() {
            return positiveLabels == 0 ? 0 : (double) truePositives / positiveLabels;
        }
    }
}
