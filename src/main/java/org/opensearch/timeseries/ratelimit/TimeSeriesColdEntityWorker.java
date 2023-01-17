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
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.TimeSeriesCheckpointDao;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkRequest;

public class TimeSeriesColdEntityWorker<NodeStateType extends ExpiringState, RCFModelType, IndexableResultType extends IndexableResult, ResultWriteRequestType extends ResultWriteRequest<IndexableResultType>, ResultWriteBatchRequestType extends TimeSeriesResultBulkRequest<IndexableResultType, ResultWriteRequestType>, ModelState extends TimeSeriesModelState<EntityModel<RCFModelType>>, RCFResultType extends IntermediateResult, ModelManagerType extends ModelManager<RCFModelType, NodeStateType, ModelState, RCFResultType>, CheckpointType extends TimeSeriesCheckpointDao<RCFModelType, ModelState>>
 extends ScheduledWorker<EntityFeatureRequest, EntityFeatureRequest, NodeStateType> {

    public TimeSeriesColdEntityWorker(
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
            TimeSeriesHCCheckpointReadWorker<NodeStateType, RCFModelType, IndexableResultType, ResultWriteRequestType, ResultWriteBatchRequestType, ModelState, RCFResultType, ModelManagerType, CheckpointType>
             checkpointReadQueue,
            Duration stateTtl,
            TimeSeriesNodeStateManager<NodeStateType> nodeStateManager,
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

            this.expectedExecutionTimeInMilliSecsPerRequest = expectedColdEntityExecutionMillsSetting
                .get(settings);
            clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(
                        expectedColdEntityExecutionMillsSetting,
                    it -> this.expectedExecutionTimeInMilliSecsPerRequest = it
                );
        }

    @Override
    protected List<EntityFeatureRequest> transformRequests(List<EntityFeatureRequest> requests) {
        // guarantee we only send low priority requests
        return requests.stream().filter(request -> request.getPriority() == RequestPriority.LOW).collect(Collectors.toList());
    }
}
