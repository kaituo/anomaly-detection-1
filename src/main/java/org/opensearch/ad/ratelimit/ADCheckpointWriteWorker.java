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

package org.opensearch.ad.ratelimit;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.ml.ADCheckpointStore;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.util.IndexOperations;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADCheckpointWriteWorker extends
    CheckpointWriteWorker<ThresholdedRandomCutForest, ADIndex, ADDelegatingDataManagement, ADCheckpointStore> {
    public static final String WORKER_NAME = "ad-checkpoint-write";

    public ADCheckpointWriteWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Duration executionTtl,
        ADCheckpointStore checkpoint,
        String indexName,
        Duration checkpointInterval,
        StateManager adNodeStateManager,
        Duration stateTtl,
        IndexOperations indexOperations
    ) {
        super(
            WORKER_NAME,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            ADCommonName.AD_THREAD_POOL_NAME,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            AD_CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl,
            adNodeStateManager,
            checkpoint,
            indexName,
            checkpointInterval,
            AnalysisType.AD,
            indexOperations
        );
    }
}
