/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDaoInterface;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.util.DateUtils;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.TenantAwareHelper;

public abstract class CheckpointWriteWorker<RCFModelType, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, CheckpointDaoType extends CheckpointDaoInterface<RCFModelType>>
    extends BatchWorker<CheckpointWriteRequest, BulkRequest, BulkResponse> {
    private static final Logger LOG = LogManager.getLogger(CheckpointWriteWorker.class);

    protected final CheckpointDaoType checkpoint;
    protected final String indexName;
    protected volatile Duration checkpointInterval;

    public CheckpointWriteWorker(
        String queueName,
        long heapSize,
        int singleRequestSize,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService circuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Setting<Integer> concurrencySetting,
        Duration executionTtl,
        Setting<Integer> batchSizeSetting,
        Duration stateTtl,
        StateManager timeSeriesNodeStateManager,
        CheckpointDaoType checkpoint,
        String indexName,
        Setting<TimeValue> checkpointIntervalSetting,
        AnalysisType context
    ) {
        super(
            queueName,
            heapSize,
            singleRequestSize,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            circuitBreakerService,
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
            timeSeriesNodeStateManager,
            context
        );
        this.checkpoint = checkpoint;
        this.indexName = indexName;
        this.checkpointInterval = DateUtils.toDuration(checkpointIntervalSetting.get(settings));
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(checkpointIntervalSetting, it -> this.checkpointInterval = DateUtils.toDuration(it));
    }

    @Override
    protected void executeBatchRequest(BulkRequest request, ActionListener<BulkResponse> listener) {
        checkpoint.batchWrite(request, listener);
    }

    @Override
    protected BulkRequest toBatchRequest(List<CheckpointWriteRequest> toProcess, String tenantId, String dataSourceId) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (CheckpointWriteRequest request : toProcess) {
            bulkRequest.add(request.getUpdateRequest());
        }
        return bulkRequest;
    }

    @Override
    protected ActionListener<BulkResponse> getResponseListener(List<CheckpointWriteRequest> toProcess, BulkRequest batchRequest) {
        return ActionListener.wrap(response -> {
            for (BulkItemResponse r : response.getItems()) {
                if (r.getFailureMessage() != null) {
                    // maybe indicating a bug
                    // don't retry failed requests since checkpoints are too large (250KB+)
                    // Later maintenance window or cold start or cache remove will retry saving
                    LOG.error(r.getFailureMessage());
                }
            }
        }, exception -> {
            if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get AD model checkpoint requests or shard not avialble");
                setCoolDownStart();
            }

            for (CheckpointWriteRequest request : toProcess) {
                nodeStateManager.setException(request.getConfigId(), exception);
            }

            // don't retry failed requests since checkpoints are too large (250KB+)
            // Later maintenance window or cold start or cache remove will retry saving
            LOG.error("Fail to save models", exception);
        });
    }

    /**
     * Prepare bulking the input model state to the checkpoint index.
     * We don't save checkpoints within checkpointInterval again, except this
     * is a high priority request (e.g., from cold start).
     * This method will update the input state's last checkpoint time if the
     *  checkpoint is staged (ready to be written in the next batch).
     * @param modelState Model state
     * @param forceWrite whether we should write no matter what
     * @param priority how urgent the write is
     */
    public void write(ModelState<RCFModelType> modelState, String tenantId, boolean forceWrite, RequestPriority priority) {
        if (!checkpoint.shouldSave(modelState, forceWrite, checkpointInterval, clock)) {
            logSkipCheckpointWrite(modelState, tenantId, forceWrite, priority);
            return;
        }

        String configId = modelState.getConfigId();
        String modelId = modelState.getModelId();
        if (modelId == null || configId == null) {
            LOG
                .warn(
                    "Cannot stage checkpoint write because configId [{}] or modelId [{}] is null. tenantId [{}], priority [{}], checkpointStore [{}]",
                    configId,
                    modelId,
                    tenantId,
                    priority,
                    checkpoint.getClass().getName()
                );
            return;
        }
        if (isStaleAfterConfigStateClear(tenantId, configId, modelState)) {
            return;
        }

        LOG
            .debug(
                "Checkpoint write eligible for config [{}], model [{}], tenant [{}], forceWrite [{}], priority [{}], checkpointStore [{}]",
                configId,
                modelId,
                tenantId,
                forceWrite,
                priority,
                checkpoint.getClass().getName()
            );

        // run once won't write checkpoint. Safe to cache config
        nodeStateManager.getConfig(configId, tenantId, context, true, onGetConfig(configId, modelId, modelState, priority));
    }

    private ActionListener<Optional<? extends Config>> onGetConfig(
        String configId,
        String modelId,
        ModelState<RCFModelType> modelState,
        RequestPriority priority
    ) {
        return ActionListener.wrap(configOptional -> {
            if (false == configOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("Config [{}] is not available.", configId));
                return;
            }

            Config config = configOptional.get();
            try {
                if (isStaleAfterConfigStateClear(config.getTenantId(), configId, modelState)) {
                    return;
                }

                Map<String, Object> source = checkpoint.toIndexSource(modelState);

                // the model state is bloated or we have bugs, skip
                if (source == null || source.isEmpty()) {
                    LOG
                        .warn(
                            "Checkpoint serialization returned empty source for config [{}], model [{}], tenant [{}], checkpointStore [{}]",
                            configId,
                            modelId,
                            config.getTenantId(),
                            checkpoint.getClass().getName()
                        );
                    return;
                }

                modelState.setLastCheckpointTime(clock.instant());
                String targetIndex = checkpoint.resolveCheckpointIndexName(config.getTenantId(), config.getId(), modelId, indexName);
                addCheckpointMetadata(source, config, targetIndex, modelId);
                LOG
                    .info(
                        "Staging checkpoint write for config [{}], model [{}], tenant [{}], target [{}], fields [{}], checkpointStore [{}]",
                        configId,
                        modelId,
                        config.getTenantId(),
                        targetIndex,
                        source.keySet(),
                        checkpoint.getClass().getName()
                    );
                CheckpointWriteRequest request = new CheckpointWriteRequest(
                    System.currentTimeMillis() + config.getInferredFrequencyInMilliseconds(),
                    configId,
                    priority,
                    // If the document does not already exist, the contents of the upsert element
                    // are inserted as a new document.
                    // If the document exists, update fields in the map
                    new UpdateRequest(targetIndex, modelId).docAsUpsert(true).doc(source),
                    config.getTenantId()
                );

                put(request);
            } catch (Exception e) {
                // Example exception:
                // ConcurrentModificationException when calling toCheckpoint
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                LOG.error(new ParameterizedMessage("Exception while serializing models for [{}]", modelId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get config [{}]", configId), exception); });
    }

    private boolean isStaleAfterConfigStateClear(String tenantId, String configId, ModelState<RCFModelType> modelState) {
        Instant lastUsedTime = modelState.getLastUsedTime();
        if (lastUsedTime == null) {
            return false;
        }
        boolean stale = nodeStateManager.isConfigStateClearedAfter(tenantId, configId, lastUsedTime.toEpochMilli());
        if (stale) {
            LOG
                .info(
                    "Skipping stale checkpoint write for config [{}], tenant [{}], model [{}], last used [{}] because model state was cleared later.",
                    configId,
                    tenantId,
                    modelState.getModelId(),
                    lastUsedTime.toEpochMilli()
                );
        }
        return stale;
    }

    private void logSkipCheckpointWrite(
        ModelState<RCFModelType> modelState,
        String tenantId,
        boolean forceWrite,
        RequestPriority priority
    ) {
        if (!LOG.isDebugEnabled()) {
            return;
        }

        if (modelState == null) {
            LOG
                .debug(
                    "Skip checkpoint write because model state is null. tenantId [{}], forceWrite [{}], priority [{}], checkpointStore [{}]",
                    tenantId,
                    forceWrite,
                    priority,
                    checkpoint.getClass().getName()
                );
            return;
        }

        Instant lastCheckpointTime = modelState.getLastCheckpointTime();
        boolean hasModel = modelState.getModel().isPresent();
        int sampleCount = modelState.getSamples() == null ? 0 : modelState.getSamples().size();
        boolean dueByTime = lastCheckpointTime != null
            && !lastCheckpointTime.equals(Instant.MIN)
            && lastCheckpointTime.plus(checkpointInterval).isBefore(clock.instant());
        LOG
            .debug(
                "Skip checkpoint write for config [{}], model [{}], tenant [{}]: hasModel [{}], sampleCount [{}], lastCheckpointTime [{}], checkpointInterval [{}], dueByTime [{}], forceWrite [{}], priority [{}], checkpointStore [{}]",
                modelState.getConfigId(),
                modelState.getModelId(),
                tenantId,
                hasModel,
                sampleCount,
                lastCheckpointTime,
                checkpointInterval,
                dueByTime,
                forceWrite,
                priority,
                checkpoint.getClass().getName()
            );
    }

    public void writeAll(
        List<ModelState<RCFModelType>> modelStates,
        String configId,
        String tenantId,
        boolean forceWrite,
        RequestPriority priority
    ) {
        ActionListener<Optional<? extends Config>> onGetForAll = ActionListener.wrap(configOptional -> {
            if (false == configOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("Config [{}] is not available.", configId));
                return;
            }

            Config config = configOptional.get();
            try {
                List<CheckpointWriteRequest> allRequests = new ArrayList<>();
                for (ModelState<RCFModelType> state : modelStates) {
                    if (!checkpoint.shouldSave(state, forceWrite, checkpointInterval, clock)) {
                        continue;
                    }

                    Map<String, Object> source = checkpoint.toIndexSource(state);
                    String modelId = state.getModelId();

                    // the model state is bloated or empty (empty samples and models), skip
                    if (source == null || source.isEmpty() || Strings.isEmpty(modelId)) {
                        continue;
                    }

                    state.setLastCheckpointTime(clock.instant());
                    String targetIndex = checkpoint.resolveCheckpointIndexName(config.getTenantId(), config.getId(), modelId, indexName);
                    addCheckpointMetadata(source, config, targetIndex, modelId);
                    allRequests
                        .add(
                            new CheckpointWriteRequest(
                                System.currentTimeMillis() + config.getInferredFrequencyInMilliseconds(),
                                configId,
                                priority,
                                // If the document does not already exist, the contents of the upsert element
                                // are inserted as a new document.
                                // If the document exists, update fields in the map
                                new UpdateRequest(targetIndex, modelId).docAsUpsert(true).doc(source),
                                config.getTenantId()
                            )
                        );
                }

                putAll(allRequests);
            } catch (Exception e) {
                // Example exception:
                // ConcurrentModificationException when calling toCheckpoint
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                LOG.info(new ParameterizedMessage("Exception while serializing models for [{}]", configId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get config [{}]", configId), exception); });

        // run once won't write checkpoint. Safe to cache config
        nodeStateManager.getConfig(configId, tenantId, context, true, onGetForAll);
    }

    private void addCheckpointMetadata(Map<String, Object> source, Config config, String checkpointIdentifier, String modelId) {
        String tenantId = config.getTenantId();
        putIfNotBlank(source, CommonName.TENANT_ID_FIELD, tenantId);

        String applicationId = config.getApplicationId();
        String workspaceId = config.getWorkspaceId();
        if ((Strings.isEmpty(applicationId) || Strings.isEmpty(workspaceId)) && false == Strings.isEmpty(tenantId)) {
            try {
                TenantAwareHelper.TenantComponents tenantComponents = TenantAwareHelper.parseTenantId(tenantId);
                if (Strings.isEmpty(applicationId)) {
                    applicationId = tenantComponents.applicationId();
                }
                if (Strings.isEmpty(workspaceId)) {
                    workspaceId = tenantComponents.workspaceId();
                }
            } catch (IllegalArgumentException e) {
                LOG.debug("Unable to parse tenant id [{}] while adding checkpoint metadata", tenantId, e);
            }
        }

        putIfNotBlank(source, CommonName.APPLICATION_ID_FIELD, applicationId);
        putIfNotBlank(source, CommonName.WORKSPACE_ID_FIELD, workspaceId);
        putIfNotBlank(source, CommonName.S3_REFERENCE_FIELD, checkpoint.resolveCheckpointReference(checkpointIdentifier, modelId));
    }

    private void putIfNotBlank(Map<String, Object> source, String fieldName, String value) {
        if (false == Strings.isEmpty(value)) {
            source.put(fieldName, value);
        }
    }
}
