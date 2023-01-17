/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.caching;

import java.time.Clock;
import java.time.Duration;

import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.caching.SingleStreamCacheBuffer;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastSingleStreamCacheBuffer extends
    SingleStreamCacheBuffer<RCFCaster, NodeState, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastCheckpointMaintainWorker> {

    public ForecastSingleStreamCacheBuffer(
        Clock clock,
        MemoryTracker memoryTracker,
        int checkpointIntervalHrs,
        Duration modelTtl,
        long memoryConsumptionPerEntity,
        ForecastCheckpointWriteWorker checkpointWriteQueue,
        ForecastCheckpointMaintainWorker checkpointMaintainQueue,
        String configId,
        Origin origin
    ) {
        super(
            clock,
            memoryTracker,
            checkpointIntervalHrs,
            modelTtl,
            memoryConsumptionPerEntity,
            checkpointWriteQueue,
            checkpointMaintainQueue,
            configId,
            origin
        );
    }

}
