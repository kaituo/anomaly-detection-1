/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.caching;

import java.time.Clock;
import java.time.Duration;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.caching.BaseSingleStreamCache;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastSingleStreamCache extends
    BaseSingleStreamCache<RCFCaster, NodeState, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastCheckpointMaintainWorker, ForecastSingleStreamCacheBuffer> {
    private ForecastCheckpointWriteWorker checkpointWriteQueue;
    private ForecastCheckpointMaintainWorker checkpointMaintainQueue;

    public ForecastSingleStreamCache(
        MemoryTracker memoryTracker,
        Duration modelTtl,
        Setting<TimeValue> checkpointIntervalSetting,
        Settings settings,
        ClusterService clusterService,
        ForecastCheckpointDao checkpointDao,
        int numberOfTrees,
        Origin origin,
        Clock clock,
        Setting<TimeValue> checkpointSavingFreq
    ) {
        super(
            memoryTracker,
            modelTtl,
            checkpointIntervalSetting,
            settings,
            clusterService,
            checkpointDao,
            numberOfTrees,
            origin,
            clock,
            checkpointSavingFreq
        );
    }

    @Override
    protected ForecastSingleStreamCacheBuffer createEmptyCacheBuffer(Config config, long memoryConsumptionPerEntity) {
        return new ForecastSingleStreamCacheBuffer(
            clock,
            memoryTracker,
            checkpointIntervalHrs,
            modelTtl,
            memoryConsumptionPerEntity,
            checkpointWriteQueue,
            checkpointMaintainQueue,
            config.getId(),
            origin
        );
    }

}
