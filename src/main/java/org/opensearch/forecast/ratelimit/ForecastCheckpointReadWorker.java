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

package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndices;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastModelState;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.transport.ForecastResultBulkRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.EntityCacheProvider;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.EntityFeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.TimeSeriesHCCheckpointReadWorker;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastCheckpointReadWorker extends
TimeSeriesHCCheckpointReadWorker<ForecastNodeState, RCFCaster, ForecastResult, ForecastResultWriteRequest, ForecastResultBulkRequest, ForecastModelState<EntityModel<RCFCaster>>, RCFCasterResult, ForecastModelManager, ForecastCheckpointDao> {
    public static final String WORKER_NAME = "forecast-checkpoint-read";

    public ForecastCheckpointReadWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        TimeSeriesCircuitBreakerService adCircuitBreakerService,
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
        ForecastNodeStateManager stateManager,
        ForecastIndices indexUtil,
        EntityCacheProvider<RCFCaster, ForecastModelState<EntityModel<RCFCaster>>> cacheProvider,
        Duration stateTtl,
        ForecastCheckpointWriteWorker checkpointWriteQueue,
        TimeSeriesStats adStats
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
            ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME
        );
    }

    @Override
    protected void saveResult(RCFCasterResult result, Config config, EntityFeatureRequest origRequest, Entity entity,
            String modelId) {
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
                    indexUtil.getSchemaVersion(ForecastIndex.RESULT),
                    modelId,
                    null,
                    null
                );

            resultWriteWorker
                .put(
                    new ForecastResultWriteRequest(
                        origRequest.getExpirationEpochMs(),
                        config.getId(),
                        RequestPriority.MEDIUM,
                        (ForecastResult)resultToSave,
                        config.getCustomResultIndex()
                    )
                );
        }
    }
}
