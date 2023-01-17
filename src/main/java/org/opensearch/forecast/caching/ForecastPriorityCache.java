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

package org.opensearch.forecast.caching;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_DEDICATED_CACHE_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MODEL_MAX_SIZE_PERCENTAGE;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.caching.PriorityCache;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastPriorityCache extends
    PriorityCache<RCFCaster, NodeState, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastCheckpointMaintainWorker, ForecastHCCacheBuffer> {
    private ForecastCheckpointWriteWorker checkpointWriteQueue;
    private ForecastCheckpointMaintainWorker checkpointMaintainQueue;

    public ForecastPriorityCache(
        ForecastCheckpointDao checkpointDao,
        int dedicatedCacheSize,
        Setting<TimeValue> checkpointTtl,
        int maxInactiveStates,
        MemoryTracker memoryTracker,
        int numberOfTrees,
        Clock clock,
        ClusterService clusterService,
        Duration modelTtl,
        ThreadPool threadPool,
        String threadPoolName,
        ForecastCheckpointWriteWorker checkpointWriteQueue,
        int maintenanceFreqConstant,
        ForecastCheckpointMaintainWorker checkpointMaintainQueue,
        Settings settings,
        Setting<TimeValue> checkpointSavingFreq
    ) {
        super(
            checkpointDao,
            dedicatedCacheSize,
            checkpointTtl,
            maxInactiveStates,
            memoryTracker,
            numberOfTrees,
            clock,
            clusterService,
            modelTtl,
            threadPool,
            threadPoolName,
            maintenanceFreqConstant,
            settings,
            checkpointSavingFreq,
            Origin.REAL_TIME_HC_FORECASTER,
            FORECAST_DEDICATED_CACHE_SIZE,
            FORECAST_MODEL_MAX_SIZE_PERCENTAGE
        );

        this.checkpointWriteQueue = checkpointWriteQueue;
        this.checkpointMaintainQueue = checkpointMaintainQueue;
    }

    @Override
    protected ForecastHCCacheBuffer createEmptyCacheBuffer(Config config, long requiredMemory) {
        return new ForecastHCCacheBuffer(
            dedicatedCacheSize,
            clock,
            memoryTracker,
            checkpointIntervalHrs,
            modelTtl,
            requiredMemory,
            checkpointWriteQueue,
            checkpointMaintainQueue,
            config.getId(),
            config.getIntervalInSeconds()
        );
    }

    @Override
    protected Callable<ModelState<RCFCaster>> createInactiveEntityCacheLoader(String modelId, String detectorId) {
        return new Callable<ModelState<RCFCaster>>() {
            @Override
            public ModelState<RCFCaster> call() {
                return new ModelState<>(
                    null,
                    modelId,
                    detectorId,
                    ModelManager.ModelType.RCFCASTER.getName(),
                    clock,
                    0,
                    new Sample(),
                    Optional.empty(),
                    new ArrayDeque<>()
                );
            }
        };
    }

    @Override
    protected boolean isAnalysisEnabled() {
        return ForecastEnabledSetting.isDoorKeeperInCacheEnabled();
    }

}
