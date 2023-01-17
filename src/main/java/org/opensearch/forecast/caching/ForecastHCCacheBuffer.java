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

import java.time.Clock;
import java.time.Duration;

import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastModelState;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.timeseries.TimeSeriesMemoryTracker;
import org.opensearch.timeseries.TimeSeriesMemoryTracker.Origin;
import org.opensearch.timeseries.caching.TimeSeriesHCCacheBuffer;
import org.opensearch.timeseries.ml.EntityModel;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastHCCacheBuffer extends TimeSeriesHCCacheBuffer<RCFCaster, ForecastModelState<EntityModel<RCFCaster>>, ForecastNodeState, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastCheckpointMaintainWorker> {

    public ForecastHCCacheBuffer(
            int minimumCapacity, Clock clock, TimeSeriesMemoryTracker memoryTracker,
            int checkpointIntervalHrs, Duration modelTtl, long memoryConsumptionPerEntity,
            ForecastCheckpointWriteWorker checkpointWriteQueue, ForecastCheckpointMaintainWorker checkpointMaintainQueue,
            String configId, long intervalSecs) {
        super(1, clock,
                memoryTracker,
                checkpointIntervalHrs,
                modelTtl,
                intervalSecs,
                checkpointWriteQueue,
                checkpointMaintainQueue,
                configId,
                intervalSecs,
                Origin.REAL_TIME_HC_FORECASTER);
    }
}
