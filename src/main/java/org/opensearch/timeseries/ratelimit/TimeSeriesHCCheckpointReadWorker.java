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
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.Cache;
import org.opensearch.timeseries.caching.EntityCacheProvider;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.TimeSeriesIndices;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ml.TimeSeriesCheckpointDao;
import org.opensearch.timeseries.ml.TimeSeriesColdStarter;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkRequest;
import org.opensearch.timeseries.util.ExceptionUtil;



public abstract class TimeSeriesHCCheckpointReadWorker<NodeState extends ExpiringState,
    RCFModelType,
    ResultType extends IndexableResult,
    ResultWriteRequestType extends ResultWriteRequest<ResultType>,
    ResultWriteBatchRequestType extends TimeSeriesResultBulkRequest<ResultType, ResultWriteRequestType>,
    ModelState extends TimeSeriesModelState<EntityModel<RCFModelType>>,
    RCFResultType extends IntermediateResult,
    CheckpointType extends TimeSeriesCheckpointDao<RCFModelType>,
    CheckpointWriteWorkerType extends TimeSeriesCheckpointWriteWorker<NodeState, RCFModelType, CheckpointType>,
    ColdStarterType extends TimeSeriesColdStarter<NodeState, RCFModelType, CheckpointType, CheckpointWriteWorkerType>,
    ModelManagerType extends ModelManager<RCFModelType, NodeState, RCFResultType, CheckpointType, CheckpointWriteWorkerType, ColdStarterType>,
    CacheType extends Cache<EntityModel<RCFModelType>, ModelState>,
    ColdStartWorkerType extends TimeSeriesColdStartWorker<NodeState, RCFModelType, EntityModel<RCFModelType>, ModelState, CheckpointType, CheckpointWriteWorkerType, ColdStarterType, CacheType>>
        extends BatchWorker<EntityFeatureRequest, MultiGetRequest, MultiGetResponse, NodeState> {
    private static final Logger LOG = LogManager.getLogger(TimeSeriesHCCheckpointReadWorker.class);

    protected final ModelManagerType modelManager;
    protected final CheckpointType checkpointDao;
    protected final ColdStartWorkerType entityColdStartWorker;
    protected final TimeSeriesResultWriteWorker<ResultType, ResultWriteRequestType, ResultWriteBatchRequestType, NodeState> resultWriteWorker;
    protected final TimeSeriesIndices indexUtil;
    protected final TimeSeriesStats timeSeriesStats;
    protected final CheckpointWriteWorkerType checkpointWriteWorker;
    protected final EntityCacheProvider<RCFModelType, ModelState> cacheProvider;
    protected final String checkpointIndexName;

    public TimeSeriesHCCheckpointReadWorker(
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
            ColdStartWorkerType entityColdStartWorker,
            TimeSeriesResultWriteWorker<ResultType, ResultWriteRequestType, ResultWriteBatchRequestType, NodeState> resultWriteWorker,
            TimeSeriesNodeStateManager<NodeState> stateManager,
            TimeSeriesIndices indexUtil,
            EntityCacheProvider<RCFModelType, ModelState> cacheProvider,
            Duration stateTtl,
            CheckpointWriteWorkerType checkpointWriteWorker,
            TimeSeriesStats timeSeriesStats,
            Setting<Integer> concurrencySetting,
            Setting<Integer> batchSizeSetting,
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
                batchSizeSetting,
                stateTtl,
                stateManager
            );

            this.modelManager = modelManager;
            this.checkpointDao = checkpointDao;
            this.entityColdStartWorker = entityColdStartWorker;
            this.resultWriteWorker = resultWriteWorker;
            this.indexUtil = indexUtil;
            this.cacheProvider = cacheProvider;
            this.checkpointWriteWorker = checkpointWriteWorker;
            this.timeSeriesStats = timeSeriesStats;
            this.checkpointIndexName = checkpointIndexName;
        }

    @Override
    protected void executeBatchRequest(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        checkpointDao.batchRead(request, listener);
    }

    /**
     * Convert the input list of EntityFeatureRequest to a multi-get request.
     * RateLimitedRequestWorker.getRequests has already limited the number of
     * requests in the input list. So toBatchRequest method can take the input
     * and send the multi-get directly.
     * @return The converted multi-get request
     */
    @Override
    protected MultiGetRequest toBatchRequest(List<EntityFeatureRequest> toProcess) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (EntityRequest request : toProcess) {
            Optional<String> modelId = request.getModelId();
            if (false == modelId.isPresent()) {
                continue;
            }
            multiGetRequest.add(new MultiGetRequest.Item(checkpointIndexName, modelId.get()));
        }
        return multiGetRequest;
    }

    @Override
    protected ActionListener<MultiGetResponse> getResponseListener(List<EntityFeatureRequest> toProcess, MultiGetRequest batchRequest) {
        return ActionListener.wrap(response -> {
            final MultiGetItemResponse[] itemResponses = response.getResponses();
            Map<String, MultiGetItemResponse> successfulRequests = new HashMap<>();

            // lazy init since we don't expect retryable requests to happen often
            Set<String> retryableRequests = null;
            Set<String> notFoundModels = null;
            boolean printedUnexpectedFailure = false;
            // contain requests that we will set the detector's exception to
            // EndRunException (stop now = false)
            Map<String, Exception> stopDetectorRequests = null;
            for (MultiGetItemResponse itemResponse : itemResponses) {
                String modelId = itemResponse.getId();
                if (itemResponse.isFailed()) {
                    final Exception failure = itemResponse.getFailure().getFailure();
                    if (failure instanceof IndexNotFoundException) {
                        for (EntityRequest origRequest : toProcess) {
                            // If it is checkpoint index not found exception, I don't
                            // need to retry as checkpoint read is bound to fail. Just
                            // send everything to the cold start queue and return.
                            entityColdStartWorker.put(origRequest);
                        }
                        return;
                    } else if (ExceptionUtil.isRetryAble(failure)) {
                        if (retryableRequests == null) {
                            retryableRequests = new HashSet<>();
                        }
                        retryableRequests.add(modelId);
                    } else if (ExceptionUtil.isOverloaded(failure)) {
                        LOG.error("too many get AD model checkpoint requests or shard not available");
                        setCoolDownStart();
                    } else {
                        // some unexpected bug occurred or cluster is unstable (e.g., ClusterBlockException) or index is red (e.g.
                        // NoShardAvailableActionException) while fetching a checkpoint. As this might happen for a large amount
                        // of entities, we don't want to flood logs with such exception trace. Only print it once.
                        if (!printedUnexpectedFailure) {
                            LOG.error("Unexpected failure", failure);
                            printedUnexpectedFailure = true;
                        }
                        if (stopDetectorRequests == null) {
                            stopDetectorRequests = new HashMap<>();
                        }
                        stopDetectorRequests.put(modelId, failure);
                    }
                } else if (!itemResponse.getResponse().isExists()) {
                    // lazy init as we don't expect retrying happens often
                    if (notFoundModels == null) {
                        notFoundModels = new HashSet<>();
                    }
                    notFoundModels.add(modelId);
                } else {
                    successfulRequests.put(modelId, itemResponse);
                }
            }

            // deal with not found model
            if (notFoundModels != null) {
                for (EntityRequest origRequest : toProcess) {
                    Optional<String> modelId = origRequest.getModelId();
                    if (modelId.isPresent() && notFoundModels.contains(modelId.get())) {
                        // submit to cold start queue
                        entityColdStartWorker.put(origRequest);
                    }
                }
            }

            // deal with failures that we will retry for a limited amount of times
            // before stopping the detector
            // We cannot just loop over stopDetectorRequests instead of toProcess
            // because we need detector id from toProcess' elements. stopDetectorRequests only has model id.
            if (stopDetectorRequests != null) {
                for (EntityRequest origRequest : toProcess) {
                    Optional<String> modelId = origRequest.getModelId();
                    if (modelId.isPresent() && stopDetectorRequests.containsKey(modelId.get())) {
                        String adID = origRequest.getId();
                        nodeStateManager
                            .setException(
                                adID,
                                new EndRunException(adID, CommonMessages.BUG_RESPONSE, stopDetectorRequests.get(modelId.get()), false)
                            );
                    }
                }
            }

            if (successfulRequests.isEmpty() && (retryableRequests == null || retryableRequests.isEmpty())) {
                // don't need to proceed further since no checkpoint is available
                return;
            }

            processCheckpointIteration(0, toProcess, successfulRequests, retryableRequests);
        }, exception -> {
            if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get AD model checkpoint requests or shard not available");
                setCoolDownStart();
            } else if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                putAll(toProcess);
            } else {
                LOG.error("Fail to restore models", exception);
            }
        });
    }

    protected void processCheckpointIteration(
            int i,
            List<EntityFeatureRequest> toProcess,
            Map<String, MultiGetItemResponse> successfulRequests,
            Set<String> retryableRequests
        ) {
            if (i >= toProcess.size()) {
                return;
            }

            // whether we will process next response in callbacks
            // if false, finally will process next checkpoints
            boolean processNextInCallBack = false;
            try {
                EntityFeatureRequest origRequest = toProcess.get(i);

                Optional<String> modelIdOptional = origRequest.getModelId();
                if (false == modelIdOptional.isPresent()) {
                    return;
                }

                String configId = origRequest.getId();
                Entity entity = origRequest.getEntity();

                String modelId = modelIdOptional.get();

                MultiGetItemResponse checkpointResponse = successfulRequests.get(modelId);

                if (checkpointResponse != null) {
                    // successful requests
                    ModelState checkpoint = checkpointDao
                        .processGetResponse(checkpointResponse.getResponse(), modelId, configId);

                    if (null == checkpoint) {
                        // checkpoint is not available (e.g., too big or corrupted); cold start again
                        entityColdStartWorker.put(origRequest);
                        return;
                    }

                    nodeStateManager
                        .getConfig(
                            configId,
                            processIterationUsingConfig(
                                origRequest,
                                i,
                                configId,
                                toProcess,
                                successfulRequests,
                                retryableRequests,
                                checkpoint,
                                entity,
                                modelId
                            )
                        );
                    processNextInCallBack = true;
                } else if (retryableRequests != null && retryableRequests.contains(modelId)) {
                    // failed requests
                    super.put(origRequest);
                }
            } finally {
                if (false == processNextInCallBack) {
                    processCheckpointIteration(i + 1, toProcess, successfulRequests, retryableRequests);
                }
            }
        }

    protected ActionListener<Optional<? extends Config>> processIterationUsingConfig(
            EntityFeatureRequest origRequest,
            int index,
            String configId,
            List<EntityFeatureRequest> toProcess,
            Map<String, MultiGetItemResponse> successfulRequests,
            Set<String> retryableRequests,
            ModelState restoredModelState,
            Entity entity,
            String modelId
        ) {
            return ActionListener.wrap(detectorOptional -> {
                if (false == detectorOptional.isPresent()) {
                    LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", configId));
                    processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                    return;
                }

                Config config = detectorOptional.get();

                RCFResultType result = null;
                try {
                    result = modelManager
                        .getResultForEntity(new Sample(origRequest.getCurrentFeature(), Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                                Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + config.getIntervalInMilliseconds())
                                ), restoredModelState, modelId, entity, config);
                } catch (IllegalArgumentException e) {
                    // fail to score likely due to model corruption. Re-cold start to recover.
                    LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", origRequest.getModelId()), e);
                    timeSeriesStats.getStat(StatNames.MODEL_CORRUTPION_COUNT.getName()).increment();
                    if (origRequest.getModelId().isPresent()) {
                        String entityModelId = origRequest.getModelId().get();
                        checkpointDao
                            .deleteModelCheckpoint(
                                entityModelId,
                                ActionListener
                                    .wrap(
                                        r -> LOG.debug(new ParameterizedMessage("Succeeded in deleting checkpoint [{}].", entityModelId)),
                                        ex -> LOG.error(new ParameterizedMessage("Failed to delete checkpoint [{}].", entityModelId), ex)
                                    )
                            );
                    }

                    entityColdStartWorker.put(origRequest);
                    processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                    return;
                }

                saveResult(result, config, origRequest, entity, modelId);

                // try to load to cache
                boolean loaded = cacheProvider.get().hostIfPossible(config, restoredModelState);

                if (false == loaded) {
                    // not in memory. Maybe cold entities or some other entities
                    // have filled the slot while waiting for loading checkpoints.
                    checkpointWriteWorker.write(restoredModelState, true, RequestPriority.LOW);
                }

                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
            }, exception -> {
                LOG.error(new ParameterizedMessage("fail to get checkpoint [{}]", modelId, exception));
                nodeStateManager.setException(configId, exception);
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
            });
        }

    protected abstract void saveResult(RCFResultType result, Config config, EntityFeatureRequest origRequest, Entity entity, String modelId);
}
