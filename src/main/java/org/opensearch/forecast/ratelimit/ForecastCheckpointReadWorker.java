/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.transport.ForecastResultBulkRequest;
import org.opensearch.forecast.transport.handler.ForecastIndexMemoryPressureAwareResultHandler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastCheckpointReadWorker extends
    CheckpointReadWorker<RCFCaster, ForecastResult, ForecastResultWriteRequest, ForecastResultBulkRequest, RCFCasterResult, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastColdStart, ForecastModelManager, ForecastPriorityCache, ForecastColdStartWorker, ForecastIndexMemoryPressureAwareResultHandler, ForecastResultWriteWorker> {
    public static final String WORKER_NAME = "forecast-checkpoint-read";

    public ForecastCheckpointReadWorker(
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
        ForecastModelManager modelManager,
        ForecastCheckpointDao checkpointDao,
        ForecastColdStartWorker entityColdStartQueue,
        ForecastResultWriteWorker resultWriteQueue,
        NodeStateManager stateManager,
        ForecastIndexManagement indexUtil,
        Provider<ForecastPriorityCache> cacheProvider,
        Duration stateTtl,
        ForecastCheckpointWriteWorker checkpointWriteQueue,
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
            FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY,
            FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
            ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME,
            StatNames.FORECAST_MODEL_CORRUTPION_COUNT,
            AnalysisType.FORECAST
        );
    }

    @Override
    protected void saveResult(RCFCasterResult result, Config config, FeatureRequest origRequest, Optional<Entity> entity, String modelId) {
        if (result != null && result.getRcfScore() > 0) {
            result
                .toIndexableResult(
                    config,
                    Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                    Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + config.getIntervalInMilliseconds()),
                    Instant.now(),
                    Instant.now(),
                    ParseUtils.getFeatureData(origRequest.getCurrentFeature(), config),
                    entity,
                    indexUtil.getSchemaVersion(ForecastIndex.RESULT),
                    modelId,
                    null,
                    null
                )
                .ifPresent(
                    r -> resultWriteWorker
                        .put(
                            new ForecastResultWriteRequest(
                                origRequest.getExpirationEpochMs(),
                                config.getId(),
                                RequestPriority.MEDIUM,
                                (ForecastResult) r,
                                config.getCustomResultIndex()
                            )
                        )
                );
        }
    }
}
