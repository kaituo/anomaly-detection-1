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
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDaoInterface;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.RealTimeInferencer;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ActionListenerExecutor;
import org.opensearch.timeseries.util.ExceptionUtil;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class CheckpointReadWorker<RCFModelType extends ThresholdedRandomCutForest, ResultType extends IndexableResult, RCFResultType extends IntermediateResult<ResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, CheckpointType extends CheckpointDaoInterface<RCFModelType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, DataManagementType, CheckpointType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, DataManagementType, ResultType>, ModelManagerType extends ModelManager<RCFModelType, ResultType, RCFResultType, IndexType, DataManagementType, CheckpointType, ColdStarterType>, CacheType extends TimeSeriesCache<RCFModelType>, SaveResultStrategyType extends SaveResultStrategy<ResultType, RCFResultType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, DataManagementType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, DataManagementType, CheckpointType, CheckpointWriteWorkerType, ColdStarterType, CacheType, ResultType, RCFResultType, ModelManagerType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType>, InferencerType extends RealTimeInferencer<RCFModelType, ResultType, RCFResultType, IndexType, DataManagementType, CheckpointType, CheckpointWriteWorkerType, ColdStarterType, ModelManagerType, SaveResultStrategyType, CacheType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType>>
    extends BatchWorker<FeatureRequest, MultiGetRequest, MultiGetResponse> {

    private static final Logger LOG = LogManager.getLogger(CheckpointReadWorker.class);

    protected final ModelManagerType modelManager;
    protected final CheckpointType checkpointDao;
    protected final ColdStartWorkerType coldStartWorker;
    protected final CheckpointWriteWorkerType checkpointWriteWorker;
    protected final Provider<? extends TimeSeriesCache<RCFModelType>> cacheProvider;
    protected final String checkpointIndexName;
    protected final InferencerType inferencer;

    public CheckpointReadWorker(
        String workerName,
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
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
        StateManager stateManager,
        Provider<? extends TimeSeriesCache<RCFModelType>> cacheProvider,
        Duration stateTtl,
        CheckpointWriteWorkerType checkpointWriteWorker,
        Setting<Integer> concurrencySetting,
        Setting<Integer> batchSizeSetting,
        String checkpointIndexName,
        AnalysisType context,
        InferencerType inferencer
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
            threadPoolName,
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
            stateManager,
            context
        );

        this.modelManager = modelManager;
        this.checkpointDao = checkpointDao;
        this.coldStartWorker = entityColdStartWorker;
        this.cacheProvider = cacheProvider;
        this.checkpointWriteWorker = checkpointWriteWorker;
        this.checkpointIndexName = checkpointIndexName;
        this.inferencer = inferencer;
    }

    @Override
    protected void executeBatchRequest(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        if (request.getItems().isEmpty()) {
            listener.onResponse(new MultiGetResponse(new MultiGetItemResponse[0]));
            return;
        }
        checkpointDao.batchRead(request, listener);
    }

    /**
     * Convert the input list of FeatureRequest to a multi-get request.
     * RateLimitedRequestWorker.getRequests has already limited the number of
     * requests in the input list. So toBatchRequest method can take the input
     * and send the multi-get directly.
     * @return The converted multi-get request
     */
    @Override
    protected MultiGetRequest toBatchRequest(List<FeatureRequest> toProcess, String tenantId) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (FeatureRequest request : toProcess) {
            if (isStaleAfterConfigStateClear(request)) {
                continue;
            }
            String modelId = request.getModelId();
            if (null == modelId) {
                continue;
            }
            String indexName = checkpointDao
                .resolveCheckpointIndexName(request.getTenantId(), request.getConfigId(), modelId, checkpointIndexName);
            multiGetRequest.add(new MultiGetRequest.Item(indexName, modelId));
        }
        return multiGetRequest;
    }

    @Override
    protected ActionListener<MultiGetResponse> getResponseListener(List<FeatureRequest> toProcess, MultiGetRequest batchRequest) {
        return ActionListener.wrap(response -> {
            List<FeatureRequest> activeRequests = filterStaleRequests(toProcess);
            if (activeRequests.isEmpty()) {
                return;
            }

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
                    if (failure instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(failure)) {
                        for (FeatureRequest origRequest : activeRequests) {
                            // If it is checkpoint index not found exception, I don't
                            // need to retry as checkpoint read is bound to fail. Just
                            // send everything to the cold start queue and return.
                            coldStartWorker.put(origRequest);
                        }
                        return;
                    } else if (ExceptionUtil.isRetryAble(failure)) {
                        if (retryableRequests == null) {
                            retryableRequests = new HashSet<>();
                        }
                        retryableRequests.add(modelId);
                    } else if (ExceptionUtil.isOverloaded(failure)) {
                        LOG.error("too many get model checkpoint requests or shard not available");
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
                for (FeatureRequest origRequest : activeRequests) {
                    String modelId = origRequest.getModelId();
                    if (modelId != null && notFoundModels.contains(modelId)) {
                        // submit to cold start queue
                        coldStartWorker.put(origRequest);
                    }
                }
            }

            // deal with failures that we will retry for a limited amount of times
            // before stopping the detector
            // We cannot just loop over stopDetectorRequests instead of toProcess
            // because we need detector id from toProcess' elements. stopDetectorRequests only has model id.
            if (stopDetectorRequests != null) {
                for (FeatureRequest origRequest : activeRequests) {
                    String modelId = origRequest.getModelId();
                    if (modelId != null && stopDetectorRequests.containsKey(modelId)) {
                        String configID = origRequest.getConfigId();
                        nodeStateManager
                            .setException(
                                configID,
                                new EndRunException(configID, CommonMessages.BUG_RESPONSE, stopDetectorRequests.get(modelId), false)
                            );
                        // once one EndRunException is set, we can break; no point setting the exception repeatedly
                        break;
                    }
                }
            }

            if (successfulRequests.isEmpty() && (retryableRequests == null || retryableRequests.isEmpty())) {
                // don't need to proceed further since no checkpoint is available
                return;
            }
            processCheckpointIteration(0, activeRequests, successfulRequests, retryableRequests);
        }, exception -> {
            LOG.warn("Exception while processing checkpoints", exception);
            if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get model checkpoint requests or shard not available");
                setCoolDownStart();
            } else if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                putAll(toProcess);
            } else {
                LOG.error("Failed to restore models", exception);
            }
        });
    }

    protected void processCheckpointIteration(
        int i,
        List<FeatureRequest> toProcess,
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
            FeatureRequest origRequest = toProcess.get(i);
            if (isStaleAfterConfigStateClear(origRequest)) {
                return;
            }

            String modelId = origRequest.getModelId();
            if (null == modelId) {
                return;
            }

            String configId = origRequest.getConfigId();

            MultiGetItemResponse checkpointResponse = successfulRequests.get(modelId);

            if (checkpointResponse != null) {
                LOG
                    .info(
                        "checkpoint read hit config={} model={} foundDoc={}",
                        configId,
                        modelId,
                        checkpointResponse.getResponse().isExists()
                    );
                // successful requests
                ModelState<RCFModelType> modelState = checkpointDao
                    .processHCGetResponse(checkpointResponse.getResponse(), modelId, configId, origRequest.getTenantId());

                if (null == modelState) {
                    LOG.warn("checkpoint read produced null model state config={} model={}; falling back to cold start", configId, modelId);
                    // checkpoint is not available (e.g., too big or corrupted); cold start again
                    // a long history can cause some entity not being able to initialized in time.
                    coldStartWorker.put(origRequest);
                    return;
                }

                LOG
                    .info(
                        "checkpoint restored model state config={} model={} entityPresent={}",
                        configId,
                        modelId,
                        modelState.getEntity().isPresent()
                    );

                nodeStateManager
                    .getConfig(
                        configId,
                        origRequest.getTenantId(),
                        context,
                        true,
                        processIterationUsingConfig(
                            origRequest,
                            i,
                            configId,
                            toProcess,
                            successfulRequests,
                            retryableRequests,
                            modelState,
                            modelId
                        )
                    );
                processNextInCallBack = true;
            } else if (retryableRequests != null && retryableRequests.contains(modelId)) {
                // failed requests
                LOG.warn("checkpoint read retry scheduled config={} model={}", configId, modelId);
                super.put(origRequest);
            } else {
                LOG.warn("checkpoint read miss config={} model={}; no checkpoint response available", configId, modelId);
            }
        } finally {
            if (false == processNextInCallBack) {
                processCheckpointIteration(i + 1, toProcess, successfulRequests, retryableRequests);
            }
        }
    }

    protected ActionListener<Optional<? extends Config>> processIterationUsingConfig(
        FeatureRequest origRequest,
        int index,
        String configId,
        List<FeatureRequest> toProcess,
        Map<String, MultiGetItemResponse> successfulRequests,
        Set<String> retryableRequests,
        ModelState<RCFModelType> restoredModelState,
        String modelId
    ) {
        return ActionListenerExecutor.wrap(configOptional -> {
            if (isStaleAfterConfigStateClear(origRequest)) {
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                return;
            }

            if (configOptional.isEmpty()) {
                LOG.warn(new ParameterizedMessage("Config [{}] is not available.", configId));
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                return;
            }

            Config config = configOptional.get();

            LOG.info("Processing sample for model [{}]", modelId);
            inferencer
                .process(
                    new Sample(
                        origRequest.getCurrentFeature(),
                        Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                        Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + config.getIntervalInMilliseconds())
                    ),
                    restoredModelState,
                    config,
                    origRequest.getTaskId(),
                    ActionListener.wrap(processed -> {
                        if (isStaleAfterConfigStateClear(origRequest)) {
                            processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                            return;
                        }

                        LOG.info("checkpoint restore processed sample config={} model={} processed={}", configId, modelId, processed);
                        if (processed) {
                            // try to load to cache
                            boolean loaded = cacheProvider.get().hostIfPossible(config, restoredModelState);
                            LOG.info("checkpoint restore hostIfPossible config={} model={} loaded={}", configId, modelId, loaded);

                            if (false == loaded) {
                                // not in memory. Maybe cold entities or long interval entities
                                // Save checkpoints.
                                checkpointWriteWorker
                                    .write(
                                        restoredModelState,
                                        config.getTenantId(),
                                        true,
                                        config.isLongFrequency() ? RequestPriority.MEDIUM : RequestPriority.LOW
                                    );
                            }
                        }

                        processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                    }, e -> {
                        LOG.error("Failed to process checkpoint for model " + modelId, e);
                        nodeStateManager.setException(configId, e);
                        processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                    })
                );
        }, exception -> {
            LOG.error(new ParameterizedMessage("fail to get checkpoint [{}]", modelId, exception));
            nodeStateManager.setException(configId, exception);
            processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
        }, threadPool.executor(threadPoolName));
    }

    private List<FeatureRequest> filterStaleRequests(List<FeatureRequest> requests) {
        List<FeatureRequest> activeRequests = new java.util.ArrayList<>(requests.size());
        for (FeatureRequest request : requests) {
            if (false == isStaleAfterConfigStateClear(request)) {
                activeRequests.add(request);
            }
        }
        return activeRequests;
    }

    private boolean isStaleAfterConfigStateClear(FeatureRequest request) {
        boolean stale = nodeStateManager
            .isConfigStateClearedAfter(request.getTenantId(), request.getConfigId(), request.getRequestTimeMillis());
        if (stale) {
            LOG
                .info(
                    "Skipping stale checkpoint read request for config [{}], tenant [{}], request created at [{}] because model state was cleared later.",
                    request.getConfigId(),
                    request.getTenantId(),
                    request.getRequestTimeMillis()
                );
        }
        return stale;
    }
}
