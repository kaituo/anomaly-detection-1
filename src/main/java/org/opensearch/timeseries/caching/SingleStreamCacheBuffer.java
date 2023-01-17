/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.caching;

import java.time.Clock;
import java.time.Duration;

import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainRequest;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.RequestPriority;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class SingleStreamCacheBuffer<RCFModelType extends ThresholdedRandomCutForest, NodeStateType extends NodeState, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, CheckpointMaintainerType extends CheckpointMaintainWorker<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>>
    extends
    CacheBuffer<RCFModelType, NodeStateType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType> {

    protected ModelState<RCFModelType> modelState;

    public SingleStreamCacheBuffer(
        Clock clock,
        MemoryTracker memoryTracker,
        int checkpointIntervalHrs,
        Duration modelTtl,
        long memoryConsumptionPerEntity,
        CheckpointWriterType checkpointWriteQueue,
        CheckpointMaintainerType checkpointMaintainQueue,
        String configId,
        Origin origin
    ) {
        super(
            1, // we only need 1 slot for single-stream cache
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
        this.modelState = null;
    }

    @Override
    protected void clear() {
        // race conditions can happen between the put and remove/maintenance/put:
        // not a problem as we are releasing memory in MemoryTracker.
        // The newly added one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.
        memoryTracker.releaseMemory(getReservedBytes(), true, origin);
    }

    protected void maintenance() {
        checkpointMaintainQueue
            .put(
                new CheckpointMaintainRequest(
                    // the request expires when the next maintainance starts
                    System.currentTimeMillis() + modelTtl.toMillis(),
                    configId,
                    RequestPriority.LOW,
                    modelState.getModelId()
                )
            );
    }

    public ModelState<RCFModelType> getModelState() {
        return modelState;
    }

    public void setModelState(ModelState<RCFModelType> modelState) {
        this.modelState = modelState;
    }
}
