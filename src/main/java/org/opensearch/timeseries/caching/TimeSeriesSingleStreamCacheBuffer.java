package org.opensearch.timeseries.caching;

import java.time.Clock;
import java.time.Duration;

import org.opensearch.timeseries.TimeSeriesMemoryTracker;
import org.opensearch.timeseries.TimeSeriesMemoryTracker.Origin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.ml.TimeSeriesCheckpointDao;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointWriteWorker;

public class TimeSeriesSingleStreamCacheBuffer<RCFModelType, NodeState extends ExpiringState,
CheckpointDaoType extends TimeSeriesCheckpointDao<RCFModelType>,
CheckpointWriterType extends TimeSeriesCheckpointWriteWorker<NodeState, RCFModelType, CheckpointDaoType>,
CheckpointMaintainerType extends TimeSeriesCheckpointMaintainWorker<NodeState>> extends
        TimeSeriesCacheBuffer<RCFModelType, NodeState, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType> {

    private static final Logger LOG = LogManager.getLogger(TimeSeriesSingleStreamCacheBuffer.class);

    protected final TimeSeriesModelState<RCFModelType> modelState;

    public TimeSeriesSingleStreamCacheBuffer(
            Clock clock,
            TimeSeriesMemoryTracker memoryTracker,
            int checkpointIntervalHrs,
            Duration modelTtl,
            long memoryConsumptionPerEntity,
            CheckpointWriterType checkpointWriteQueue,
            CheckpointMaintainerType checkpointMaintainQueue,
            String configId,
            Origin origin,
            TimeSeriesModelState<RCFModelType> modelState
            ) {
        super(1, // we only need 1 slot for single-stream cache
                clock, memoryTracker, checkpointIntervalHrs, modelTtl, memoryConsumptionPerEntity, checkpointWriteQueue, checkpointMaintainQueue, configId, origin);
        this.modelState = modelState;
    }

    @Override
    protected void clear() {
        // race conditions can happen between the put and remove/maintenance/put:
        // not a problem as we are releasing memory in MemoryTracker.
        // The newly added one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.
        memoryTracker.releaseMemory(getReservedBytes(), true, origin);

    }
}
