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
import java.util.Random;

import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.Cache;
import org.opensearch.timeseries.caching.EntityCacheProvider;
import org.opensearch.timeseries.caching.SingleStreamCacheProvider;
import org.opensearch.timeseries.indices.TimeSeriesIndices;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.TimeSeriesCheckpointDao;
import org.opensearch.timeseries.ml.TimeSeriesColdStarter;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkRequest;

public class TimeSeriesSingleStreamCheckpointReadWorker<NodeState extends ExpiringState,
    RCFModelType,
    ResultType extends IndexableResult,
    ResultWriteRequestType extends ResultWriteRequest<ResultType>,
    ResultWriteBatchRequestType extends TimeSeriesResultBulkRequest<ResultType, ResultWriteRequestType>,
    ModelState extends TimeSeriesModelState<RCFModelType>,
    RCFResultType extends IntermediateResult,
    CheckpointType extends TimeSeriesCheckpointDao<RCFModelType>,
    CheckpointWriteWorkerType extends TimeSeriesCheckpointWriteWorker<NodeState, RCFModelType, CheckpointType>,
    ColdStarterType extends TimeSeriesColdStarter<NodeState, RCFModelType, CheckpointType, CheckpointWriteWorkerType>,
    ModelManagerType extends ModelManager<RCFModelType, NodeState, RCFResultType, CheckpointType, CheckpointWriteWorkerType, ColdStarterType>,
    CacheType extends Cache<RCFModelType, ModelState>,
    ColdStartWorkerType extends TimeSeriesColdStartWorker<NodeState, RCFModelType, RCFModelType, ModelState, CheckpointType, CheckpointWriteWorkerType, ColdStarterType, CacheType>
    > extends SingleRequestWorker<FeatureRequest, NodeState>  {

    protected final ModelManagerType modelManager;
    protected final CheckpointType checkpointDao;
    protected final ColdStartWorkerType coldStartWorker;
    protected final TimeSeriesResultWriteWorker<ResultType, ResultWriteRequestType, ResultWriteBatchRequestType, NodeState> resultWriteWorker;
    protected final TimeSeriesIndices indexUtil;
    protected final TimeSeriesStats timeSeriesStats;
    protected final CheckpointWriteWorkerType checkpointWriteWorker;
    protected final SingleStreamCacheProvider<RCFModelType, ModelState> cacheProvider;
    protected final String checkpointIndexName;

    public TimeSeriesSingleStreamCheckpointReadWorker(
            String workerName,
            long heapSizeInBytes,
            int singleRequestSizeInBytes,
            Setting<Float> maxHeapPercentForQueueSetting,
            ClusterService clusterService,
            Random random,
            TimeSeriesCircuitBreakerService adCircuitBreakerService,
            ThreadPool threadPool,
            Settings settings,
            float maxQueuedTaskRatio,
            Clock clock,
            float mediumSegmentPruneRatio,
            float lowSegmentPruneRatio,
            int maintenanceFreqConstant,
            Duration executionTtl,
            ModelManagerType modelManager,
            CheckpointType checkpointDao,
            ColdStartWorkerType coldStartWorker,
            TimeSeriesResultWriteWorker<ResultType, ResultWriteRequestType, ResultWriteBatchRequestType, NodeState> resultWriteWorker,
            TimeSeriesNodeStateManager<NodeState> stateManager,
            TimeSeriesIndices indexUtil,
            SingleStreamCacheProvider<RCFModelType, ModelState> cacheProvider,
            Duration stateTtl,
            CheckpointWriteWorkerType checkpointWriteWorker,
            TimeSeriesStats timeSeriesStats,
            Setting<Integer> concurrencySetting,
            String checkpointIndexName
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
            concurrencySetting,
            executionTtl,
            stateTtl,
            stateManager
        );

        this.modelManager = modelManager;
        this.checkpointDao = checkpointDao;
        this.coldStartWorker = coldStartWorker;
        this.resultWriteWorker = resultWriteWorker;
        this.indexUtil = indexUtil;
        this.cacheProvider = cacheProvider;
        this.checkpointWriteWorker = checkpointWriteWorker;
        this.timeSeriesStats = timeSeriesStats;
        this.checkpointIndexName = checkpointIndexName;
    }

    @Override
    protected void executeRequest(FeatureRequest request, ActionListener<Void> listener) {
        GetRequest getRequest = new GetRequest(checkpointIndexName, request.getModelId());
        checkpointDao.read(getRequest, );
    }

}
