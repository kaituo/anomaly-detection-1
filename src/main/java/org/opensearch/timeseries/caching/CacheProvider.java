/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.caching;

import org.opensearch.common.inject.Provider;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A wrapper to call concrete implementation of caching.  Used in transport
 * action.  Don't use interface because transport action handler constructor
 * requires a concrete class as input.
 *
 */
public class CacheProvider<
    RCFModelType extends ThresholdedRandomCutForest,
    NodeStateType extends NodeState,
    NodeStateManagerType extends NodeStateManager<NodeStateType>,
    IndexType extends Enum<IndexType> & TimeSeriesIndex,
    IndexManagementType extends IndexManagement<IndexType>,
    CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>,
    CheckpointWriterType extends CheckpointWriteWorker<NodeStateType, NodeStateManagerType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>,
    CheckpointMaintainerType extends CheckpointMaintainWorker<NodeStateType, NodeStateManagerType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>,
    CacheBufferType extends CacheBuffer<RCFModelType, NodeStateType, NodeStateManagerType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType>,
    PriorityCacheType extends PriorityCache<RCFModelType, NodeStateType, NodeStateManagerType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType, CacheBufferType>
    > implements Provider<PriorityCacheType> {
    private PriorityCacheType cache;

    public CacheProvider() {

    }

    @Override
    public PriorityCacheType get() {
        return cache;
    }

    public void set(PriorityCacheType cache) {
        this.cache = cache;
    }
}