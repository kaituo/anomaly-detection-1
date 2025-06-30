/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.transport.ADSingleStreamResultAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.SingleStreamResultRequest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ADRealTimeInferencerIT extends ADIntegTestCase {
    private static final int INTERVAL_MINUTES = 1;
    private static final long INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(INTERVAL_MINUTES);
    private static final int SHINGLE_SIZE = 8;
    private static final int CHECKPOINT_BUCKET = 320;
    private static final int GAP_BUCKETS = 36;
    private static final int POST_RESTORE_BUCKETS = 80;
    private static final int FIRST_GAP_BUCKET = CHECKPOINT_BUCKET + 1;
    private static final int FIRST_POST_RESTORE_BUCKET = FIRST_GAP_BUCKET + GAP_BUCKETS;
    private static final Instant BASE_TIME = Instant.parse("2025-01-01T00:00:00Z");

    public void testCatchUpReplayPreservesLongTermPrecisionAndRecallAfterCheckpointRestore() throws Exception {
        String dataIndex = "ad-replay-it-data-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ingestSyntheticDataWithGapBuckets(dataIndex);

        Feature feature = sumFeature("total_value", "value", "value");
        AnomalyDetector seedDetector = detector("seed-detector", dataIndex, feature);
        String seedDetectorId = createDetectorThroughTransportAction(seedDetector);
        String seedModelId = SingleStreamModelIdMapper.getRcfModelId(null, seedDetectorId, 0);
        runAndWaitForResult(seedDetectorId, seedModelId, CHECKPOINT_BUCKET);
        Map<String, Object> checkpointSource = waitForCheckpoint(seedModelId);

        AnomalyDetector continuousDetector = detector("continuous-detector", dataIndex, feature);
        String continuousDetectorId = createDetectorThroughTransportAction(continuousDetector);
        String continuousModelId = SingleStreamModelIdMapper.getRcfModelId(null, continuousDetectorId, 0);

        AnomalyDetector catchUpDetector = detector("catchup-detector", dataIndex, feature);
        String catchUpDetectorId = createDetectorThroughTransportAction(catchUpDetector);
        String catchUpModelId = SingleStreamModelIdMapper.getRcfModelId(null, catchUpDetectorId, 0);

        writeCheckpoint(continuousDetectorId, continuousModelId, checkpointSource);
        writeCheckpoint(catchUpDetectorId, catchUpModelId, checkpointSource);

        DecisionMetrics continuousMetrics = new DecisionMetrics();
        List<Boolean> continuousDecisions = new ArrayList<>();
        for (int bucket = FIRST_GAP_BUCKET; bucket < FIRST_POST_RESTORE_BUCKET + POST_RESTORE_BUCKETS; bucket++) {
            Map<String, Object> result = runAndWaitForResult(continuousDetectorId, continuousModelId, bucket);
            if (bucket >= FIRST_POST_RESTORE_BUCKET) {
                boolean positive = isPositive(result);
                continuousMetrics.record(isInjectedAnomaly(bucket - FIRST_POST_RESTORE_BUCKET), positive);
                continuousDecisions.add(positive);
            }
        }

        DecisionMetrics catchUpMetrics = new DecisionMetrics();
        for (int offset = 0; offset < POST_RESTORE_BUCKETS; offset++) {
            int bucket = FIRST_POST_RESTORE_BUCKET + offset;
            Map<String, Object> result = runAndWaitForResult(catchUpDetectorId, catchUpModelId, bucket);
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

    private void ingestSyntheticDataWithGapBuckets(String indexName) {
        createTestDataIndex(indexName);
        List<Map<String, ?>> docs = new ArrayList<>();
        int endBucket = FIRST_POST_RESTORE_BUCKET + POST_RESTORE_BUCKETS;
        for (int bucket = 0; bucket < endBucket; bucket++) {
            if (isMissingGapBucket(bucket)) {
                continue;
            }
            docs
                .add(
                    ImmutableMap.of(timeField, bucketStart(bucket).toEpochMilli(), "value", valueForBucket(bucket), "type", "single-stream")
                );
        }
        assertFalse("test data should contain gap buckets", docs.size() == endBucket);
        assertEquals(RestStatus.OK, bulkIndexDocs(indexName, docs, 30_000).status());
        assertEquals(docs.size(), countDocs(indexName));
    }

    private AnomalyDetector detector(String name, String indexName, Feature feature) {
        return new AnomalyDetector(
            null,
            0L,
            name,
            "checkpoint replay e2e detector",
            timeField,
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
            null,
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

    private String createDetectorThroughTransportAction(AnomalyDetector detector) {
        IndexAnomalyDetectorRequest request = new IndexAnomalyDetectorRequest(
            "",
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            detector,
            RestRequest.Method.POST,
            TimeValue.timeValueSeconds(30),
            1000,
            1000,
            10,
            2,
            null,
            null
        );
        IndexAnomalyDetectorResponse response = client().execute(IndexAnomalyDetectorAction.INSTANCE, request).actionGet(30_000);
        assertNotNull(response.getId());
        return response.getId();
    }

    private Map<String, Object> waitForCheckpoint(String modelId) throws Exception {
        final Map<String, Object>[] checkpoint = new Map[1];
        assertBusy(() -> {
            GetResponse response = client().get(new GetRequest(ADCommonName.CHECKPOINT_INDEX_NAME, modelId)).actionGet(10_000);
            assertTrue("checkpoint should exist for model " + modelId, response.isExists());
            checkpoint[0] = response.getSourceAsMap();
        }, 30, TimeUnit.SECONDS);
        return checkpoint[0];
    }

    private void writeCheckpoint(String detectorId, String modelId, Map<String, Object> source) throws IOException {
        if (false == indexExists(ADCommonName.CHECKPOINT_INDEX_NAME)) {
            createIndex(ADCommonName.CHECKPOINT_INDEX_NAME, ADIndexManagement.getCheckpointMappings());
        }
        Map<String, Object> sourceCopy = new HashMap<>(source);
        sourceCopy.put(ADCommonName.DETECTOR_ID, detectorId);
        IndexResponse response = client()
            .index(
                new IndexRequest(ADCommonName.CHECKPOINT_INDEX_NAME)
                    .id(modelId)
                    .source(sourceCopy)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            )
            .actionGet(30_000);
        assertTrue(response.status() == RestStatus.CREATED || response.status() == RestStatus.OK);
    }

    private Map<String, Object> runAndWaitForResult(String detectorId, String modelId, int bucket) throws Exception {
        long start = bucketStart(bucket).toEpochMilli();
        AcknowledgedResponse response = client()
            .execute(
                ADSingleStreamResultAction.INSTANCE,
                new SingleStreamResultRequest(detectorId, modelId, start, start + INTERVAL_MILLIS, featureForBucket(bucket), null, null)
            )
            .actionGet(30_000);
        assertTrue(response.isAcknowledged());
        return waitForResult(detectorId, start + INTERVAL_MILLIS);
    }

    private Map<String, Object> waitForResult(String detectorId, long dataEndTimeMillis) throws Exception {
        final Map<String, Object>[] result = new Map[1];
        assertBusy(() -> {
            SearchResponse response;
            try {
                response = searchResult(detectorId, dataEndTimeMillis);
            } catch (Exception e) {
                throw new AssertionError("result index search should become available", e);
            }
            assertTrue(
                "expected one anomaly result for detector " + detectorId + " at " + dataEndTimeMillis,
                response.getHits().getHits().length > 0
            );
            SearchHit hit = response.getHits().getAt(0);
            result[0] = hit.getSourceAsMap();
        }, 30, TimeUnit.SECONDS);
        return result[0];
    }

    private SearchResponse searchResult(String detectorId, long dataEndTimeMillis) {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .query(
                QueryBuilders
                    .boolQuery()
                    .filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId))
                    .filter(QueryBuilders.rangeQuery(CommonName.DATA_END_TIME_FIELD).gte(dataEndTimeMillis).lte(dataEndTimeMillis))
            )
            .sort(CommonName.EXECUTION_END_TIME_FIELD, SortOrder.DESC)
            .size(1);
        SearchRequest request = new SearchRequest(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)
            .indicesOptions(IndicesOptions.lenientExpandOpen())
            .source(source);
        return client().search(request).actionGet(10_000);
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
