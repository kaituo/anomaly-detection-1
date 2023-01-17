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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;

import org.opensearch.ad.ADNodeState;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADEntityColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.ad.transport.handler.ADIndexMemoryPressureAwareResultHandler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * a queue for loading model checkpoint. The read is a multi-get query. Possible results are:
 * a). If a checkpoint is not found, we forward that request to the cold start queue.
 * b). When a request gets errors, the queue does not change its expiry time and puts
 *  that request to the end of the queue and automatically retries them before they expire.
 * c) When a checkpoint is found, we load that point to memory and score the input
 * data point and save the result if a complete model exists. Otherwise, we enqueue
 * the sample. If we can host that model in memory (e.g., there is enough memory),
 * we put the loaded model to cache. Otherwise (e.g., a cold entity), we write the
 * updated checkpoint back to disk.
 *
 */
public class ADCheckpointReadWorker extends
    CheckpointReadWorker<ADNodeState, ADNodeStateManager, ThresholdedRandomCutForest, AnomalyResult, ADResultWriteRequest, ADResultBulkRequest, ThresholdingResult, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADEntityColdStart, ADModelManager, ADPriorityCache, ADColdStartWorker, ADIndexMemoryPressureAwareResultHandler, ADResultWriteWorker> {
    public static final String WORKER_NAME = "ad-checkpoint-read";

    public ADCheckpointReadWorker(
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
        ADModelManager modelManager,
        ADCheckpointDao checkpointDao,
        ADColdStartWorker entityColdStartQueue,
        ADResultWriteWorker resultWriteQueue,
        ADNodeStateManager stateManager,
        ADIndexManagement indexUtil,
        Provider<ADPriorityCache> cacheProvider,
        Duration stateTtl,
        ADCheckpointWriteWorker checkpointWriteQueue,
        Stats adStats
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
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            executionTtl,
            modelManager,
            checkpointDao,
            entityColdStartQueue,
            resultWriteQueue,
            stateManager,
            indexUtil,
            cacheProvider,
            stateTtl,
            checkpointWriteQueue,
            adStats,
            AD_CHECKPOINT_READ_QUEUE_CONCURRENCY,
            AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
            ADCommonName.CHECKPOINT_INDEX_NAME
        );
    }

    @Override
    protected void saveResult(
        ThresholdingResult result,
        Config config,
        FeatureRequest origRequest,
        Optional<Entity> entity,
        String modelId
    ) {
        if (result != null && result.getRcfScore() > 0) {
            IndexableResult resultToSave = result
                .toIndexableResult(
                    config,
                    Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                    Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + config.getIntervalInMilliseconds()),
                    Instant.now(),
                    Instant.now(),
                    ParseUtils.getFeatureData(origRequest.getCurrentFeature(), config),
                    entity,
                    indexUtil.getSchemaVersion(ADIndex.RESULT),
                    modelId,
                    null,
                    null
                );

            resultWriteWorker
                .put(
                    new ADResultWriteRequest(
                        origRequest.getExpirationEpochMs(),
                        config.getId(),
                        result.getGrade() > 0 ? RequestPriority.HIGH : RequestPriority.MEDIUM,
                        (AnomalyResult) resultToSave,
                        config.getCustomResultIndex()
                    )
                );
        }
    }
}
