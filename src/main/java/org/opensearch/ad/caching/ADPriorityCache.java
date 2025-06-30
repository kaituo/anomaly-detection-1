/*
f * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad.caching;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_DEDICATED_CACHE_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Optional;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.ml.ADCheckpointStore;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.caching.PriorityCache;
import org.opensearch.timeseries.caching.PriorityTracker;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADPriorityCache extends
    PriorityCache<ThresholdedRandomCutForest, ADIndex, ADDelegatingDataManagement, ADCheckpointStore, ADCheckpointWriteWorker, ADCheckpointMaintainWorker, ADCacheBuffer> {
    private ADCheckpointWriteWorker checkpointWriteQueue;
    private ADCheckpointMaintainWorker checkpointMaintainQueue;

    public ADPriorityCache(
        ADCheckpointStore checkpointDao,
        int hcDedicatedCacheSize,
        Setting<TimeValue> checkpointTtl,
        int maxInactiveStates,
        MemoryTracker memoryTracker,
        int numberOfTrees,
        Clock clock,
        ClusterService clusterService,
        Duration modelTtl,
        ThreadPool threadPool,
        int maintenanceFreqConstant,
        Settings settings,
        Setting<TimeValue> checkpointSavingFreq,
        ADCheckpointWriteWorker checkpointWriteQueue,
        ADCheckpointMaintainWorker checkpointMaintainQueue,
        StateManager nodeStateManager
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
            ADCommonName.AD_THREAD_POOL_NAME,
            maintenanceFreqConstant,
            settings,
            checkpointSavingFreq,
            Origin.REAL_TIME_DETECTOR,
            AD_DEDICATED_CACHE_SIZE,
            AD_MODEL_MAX_SIZE_PERCENTAGE,
            nodeStateManager,
            AnalysisType.AD
        );

        this.checkpointWriteQueue = checkpointWriteQueue;
        this.checkpointMaintainQueue = checkpointMaintainQueue;
    }

    @Override
    protected ADCacheBuffer createEmptyCacheBuffer(Config detector, long memoryConsumptionPerEntity, PriorityTracker tracker) {
        return new ADCacheBuffer(
            detector.isHighCardinality() ? hcDedicatedCacheSize : 1,
            clock,
            memoryTracker,
            checkpointIntervalMins,
            modelTtl,
            memoryConsumptionPerEntity,
            checkpointWriteQueue,
            checkpointMaintainQueue,
            detector.getId(),
            tracker,
            detector.getTenantId()
        );
    }

    @Override
    protected ModelState<ThresholdedRandomCutForest> createEmptyModelState(String modelId, String detectorId, String tenantId) {
        return new ModelState<>(
            null,
            modelId,
            detectorId,
            tenantId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            0,
            Optional.empty(),
            new ArrayDeque<>()
        );
    }

    @Override
    protected boolean isDoorKeeperInCacheEnabled() {
        return ADEnabledSetting.isDoorKeeperInCacheEnabled();
    }
}
