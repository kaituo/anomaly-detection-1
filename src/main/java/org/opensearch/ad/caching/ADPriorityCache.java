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
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.opensearch.ad.ADNodeState;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.caching.PriorityCache;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADPriorityCache extends
    PriorityCache<ThresholdedRandomCutForest, ADNodeState, ADNodeStateManager, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADCheckpointMaintainWorker, ADCacheBuffer> {
    private ADCheckpointWriteWorker checkpointWriteQueue;
    private ADCheckpointMaintainWorker checkpointMaintainQueue;

    public ADPriorityCache(
        ADCheckpointDao checkpointDao,
        int hcDedicatedCacheSize,
        Setting<TimeValue> checkpointTtl,
        int maxInactiveStates,
        MemoryTracker memoryTracker,
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
        super(
            checkpointDao,
            hcDedicatedCacheSize,
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
            Origin.REAL_TIME_DETECTOR,
            DEDICATED_CACHE_SIZE,
            MODEL_MAX_SIZE_PERCENTAGE
        );

        this.checkpointWriteQueue = checkpointWriteQueue;
        this.checkpointMaintainQueue = checkpointMaintainQueue;
    }

    @Override
    protected ADCacheBuffer createEmptyCacheBuffer(Config detector, long memoryConsumptionPerEntity) {
        return new ADCacheBuffer(
            detector.isHighCardinality() ? hcDedicatedCacheSize : 1,
            clock,
            memoryTracker,
            checkpointIntervalHrs,
            modelTtl,
            memoryConsumptionPerEntity,
            checkpointWriteQueue,
            checkpointMaintainQueue,
            detector.getId(),
            detector.getIntervalInSeconds()
        );
    }

    @Override
    protected Callable<ModelState<ThresholdedRandomCutForest>> createInactiveEntityCacheLoader(String modelId, String detectorId) {
        return new Callable<ModelState<ThresholdedRandomCutForest>>() {
            @Override
            public ModelState<ThresholdedRandomCutForest> call() {
                return new ModelState<>(
                    null,
                    modelId,
                    detectorId,
                    ModelManager.ModelType.TRCF.getName(),
                    clock,
                    0,
                    null,
                    Optional.empty(),
                    new ArrayDeque<>()
                );
            }
        };
    }

    @Override
    protected boolean isDoorKeeperInCacheEnabled() {
        return ADEnabledSetting.isDoorKeeperInCacheEnabled();
    }
}
