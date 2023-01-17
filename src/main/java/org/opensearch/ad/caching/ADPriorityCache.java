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

package org.opensearch.ad.caching;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.DEDICATED_CACHE_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Callable;

import org.opensearch.ad.ADNodeState;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADModelState;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TimeSeriesMemoryTracker;
import org.opensearch.timeseries.TimeSeriesMemoryTracker.Origin;
import org.opensearch.timeseries.caching.TimeSeriesPriorityCache;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;


public class ADPriorityCache extends TimeSeriesPriorityCache<
    ThresholdedRandomCutForest,
    ADModelState<EntityModel<ThresholdedRandomCutForest>>,
    ADNodeState,
    ADCheckpointDao,
    ADCheckpointWriteWorker,
    ADCheckpointMaintainWorker,
    ADHCCacheBuffer> {
    private ADCheckpointWriteWorker checkpointWriteQueue;
    private ADCheckpointMaintainWorker checkpointMaintainQueue;

    public ADPriorityCache(
            ADCheckpointDao checkpointDao,
            int dedicatedCacheSize,
            Setting<TimeValue> checkpointTtl,
            int maxInactiveStates,
            TimeSeriesMemoryTracker memoryTracker,
            int numberOfTrees,
            Clock clock,
            ClusterService clusterService,
            Duration modelTtl,
            ThreadPool threadPool,
            String threadPoolName,
            int maintenanceFreqConstant,
            Settings settings,
            Setting<TimeValue> checkpointSavingFreq,
        ADCheckpointWriteWorker checkpointWriteQueue,
        ADCheckpointMaintainWorker checkpointMaintainQueue
    ) {
        super( checkpointDao,
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
        Origin.HC_DETECTOR,
        DEDICATED_CACHE_SIZE,
        MODEL_MAX_SIZE_PERCENTAGE);

        this.checkpointWriteQueue = checkpointWriteQueue;
        this.checkpointMaintainQueue = checkpointMaintainQueue;
    }

    @Override
    protected ADHCCacheBuffer createEmptyCacheBuffer(Config detector, long requiredMemory) {
        return new ADHCCacheBuffer(
                dedicatedCacheSize,
                clock,
                memoryTracker,
                checkpointIntervalHrs,
                modelTtl,
                requiredMemory,
                checkpointWriteQueue,
                checkpointMaintainQueue,
                detector.getId(),
                detector.getIntervalInSeconds()
            );
    }

    @Override
    protected Callable<ADModelState<EntityModel<ThresholdedRandomCutForest>>> createInactiveEntityCacheLoader(String modelId, String detectorId) {
        return new Callable<ADModelState<EntityModel<ThresholdedRandomCutForest>>>() {
            @Override
            public ADModelState<EntityModel<ThresholdedRandomCutForest>> call() {
                return new ADModelState<>(null, modelId, detectorId, ModelManager.ModelType.ENTITY.getName(), clock, 0);
            }
        };
    }

    @Override
    protected boolean isAnalysisEnabled() {
        return ADEnabledSetting.isDoorKeeperInCacheEnabled();
    }
}
