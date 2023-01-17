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

package org.opensearch.timeseries.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class CheckpointMaintainWorker<
    NodeStateType extends NodeState,
    NodeStateManagerType extends NodeStateManager<NodeStateType>,
    RCFModelType extends ThresholdedRandomCutForest,
    IndexType extends Enum<IndexType> & TimeSeriesIndex,
    IndexManagementType extends IndexManagement<IndexType>,
    CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>
    >
    extends ScheduledWorker<CheckpointMaintainRequest, CheckpointWriteRequest, NodeStateType, NodeStateManagerType> {

    private Function<CheckpointMaintainRequest, Optional<CheckpointWriteRequest>> converter;

    public CheckpointMaintainWorker(
        String workerName,
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        RateLimitedRequestWorker<CheckpointWriteRequest, NodeStateType, NodeStateManagerType> targetQueue,
        Duration stateTtl,
        NodeStateManagerType nodeStateManager,
        Function<CheckpointMaintainRequest, Optional<CheckpointWriteRequest>> converter
    ) {
        super(
            workerName,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            targetQueue,
            stateTtl,
            nodeStateManager
        );
        this.converter = converter;
    }

    @Override
    protected List<CheckpointWriteRequest> transformRequests(List<CheckpointMaintainRequest> requests) {
        List<CheckpointWriteRequest> allRequests = new ArrayList<>();
        for (CheckpointMaintainRequest request : requests) {
            Optional<CheckpointWriteRequest> converted = converter.apply(request);
            if (!converted.isEmpty()) {
                allRequests.add(converted.get());
            }
        }
        return allRequests;
    }
}
