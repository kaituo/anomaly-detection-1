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
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.transport.ResultBulkRequest;
import org.opensearch.timeseries.transport.ResultBulkResponse;
import org.opensearch.timeseries.transport.handler.IndexMemoryPressureAwareResultHandler;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ColdEntityWorker<NodeStateType extends NodeState, NodeStateManagerType extends NodeStateManager<NodeStateType>, RCFModelType extends ThresholdedRandomCutForest, IndexableResultType extends IndexableResult, ResultWriteRequestType extends ResultWriteRequest<IndexableResultType>, ResultWriteBatchRequestType extends ResultBulkRequest<IndexableResultType, ResultWriteRequestType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, RCFResultType extends IntermediateResult, ModelManagerType extends ModelManager<RCFModelType, NodeStateType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ColdStarterType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<NodeStateType, NodeStateManagerType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType>, CacheType extends TimeSeriesCache<RCFModelType>, ColdStartWorkerType extends ColdStartWorker<NodeStateType, NodeStateManagerType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ColdStarterType, CacheType>, ResultHandlerType extends IndexMemoryPressureAwareResultHandler<ResultWriteBatchRequestType, ResultBulkResponse, IndexType, IndexManagementType>,
    ResultWriteWorkerType extends ResultWriteWorker<IndexableResultType, ResultWriteRequestType, ResultWriteBatchRequestType, NodeStateType, NodeStateManagerType, IndexType, IndexManagementType, ResultHandlerType>,
    CheckpointReadWorkerType extends CheckpointReadWorker<NodeStateType, NodeStateManagerType, RCFModelType, IndexableResultType, ResultWriteRequestType, ResultWriteBatchRequestType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ColdStarterType, ModelManagerType, CacheType, ColdStartWorkerType, ResultHandlerType, ResultWriteWorkerType>>
    extends ScheduledWorker<FeatureRequest, FeatureRequest, NodeStateType, NodeStateManagerType> {

    public ColdEntityWorker(
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
        CheckpointReadWorkerType checkpointReadQueue,
        Duration stateTtl,
        NodeStateManagerType nodeStateManager,
        Setting<Integer> checkpointReadBatchSizeSetting,
        Setting<Integer> expectedColdEntityExecutionMillsSetting
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
            checkpointReadQueue,
            stateTtl,
            nodeStateManager
        );

        this.batchSize = checkpointReadBatchSizeSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(checkpointReadBatchSizeSetting, it -> this.batchSize = it);

        this.expectedExecutionTimeInMilliSecsPerRequest = expectedColdEntityExecutionMillsSetting.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(expectedColdEntityExecutionMillsSetting, it -> this.expectedExecutionTimeInMilliSecsPerRequest = it);
    }

    @Override
    protected List<FeatureRequest> transformRequests(List<FeatureRequest> requests) {
        // guarantee we only send low priority requests
        return requests.stream().filter(request -> request.getPriority() == RequestPriority.LOW).collect(Collectors.toList());
    }
}
