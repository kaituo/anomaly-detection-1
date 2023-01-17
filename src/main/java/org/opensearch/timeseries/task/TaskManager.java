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

package org.opensearch.timeseries.task;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.TransportService;

public abstract class TaskManager<TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask> {
    private final Logger logger = LogManager.getLogger(TaskManager.class);

    protected final TaskCacheManagerType taskCacheManager;
    protected final ClusterService clusterService;
    protected final Client client;
    private final String stateIndex;
    private final List<TaskTypeEnum> realTimeTaskTypes;

    public TaskManager(
        TaskCacheManagerType taskCacheManager,
        ClusterService clusterService,
        Client client,
        String stateIndex,
        List<TaskTypeEnum> realTimeTaskTypes
    ) {
        this.taskCacheManager = taskCacheManager;
        this.clusterService = clusterService;
        this.client = client;
        this.stateIndex = stateIndex;
        this.realTimeTaskTypes = realTimeTaskTypes;
    }

    public boolean skipUpdateHCRealtimeTask(String configId, String error) {
        RealtimeTaskCache realtimeTaskCache = taskCacheManager.getRealtimeTaskCache(configId);
        return realtimeTaskCache != null
            && realtimeTaskCache.getInitProgress() != null
            && realtimeTaskCache.getInitProgress().floatValue() == 1.0
            && Objects.equals(error, realtimeTaskCache.getError());
    }

    public boolean isHCRealtimeTaskStartInitializing(String detectorId) {
        RealtimeTaskCache realtimeTaskCache = taskCacheManager.getRealtimeTaskCache(detectorId);
        return realtimeTaskCache != null
            && realtimeTaskCache.getInitProgress() != null
            && realtimeTaskCache.getInitProgress().floatValue() > 0;
    }

    /**
     * Maintain running realtime tasks. Check if realtime task cache expires or not. Remove realtime
     * task cache directly if expired.
     */
    public void maintainRunningRealtimeTasks() {
        String[] detectorIds = taskCacheManager.getDetectorIdsInRealtimeTaskCache();
        if (detectorIds == null || detectorIds.length == 0) {
            return;
        }
        for (int i = 0; i < detectorIds.length; i++) {
            String detectorId = detectorIds[i];
            RealtimeTaskCache taskCache = taskCacheManager.getRealtimeTaskCache(detectorId);
            if (taskCache != null && taskCache.expired()) {
                taskCacheManager.removeRealtimeTaskCache(detectorId);
            }
        }
    }

    public void refreshRealtimeJobRunTime(String detectorId) {
        taskCacheManager.refreshRealtimeJobRunTime(detectorId);
    }

    public void removeRealtimeTaskCache(String detectorId) {
        taskCacheManager.removeRealtimeTaskCache(detectorId);
    }

    /**
     * Update realtime task cache on realtime config's coordinating node.
     *
     * @param configId config id
     * @param state new state
     * @param rcfTotalUpdates rcf total updates
     * @param intervalInMinutes config interval in minutes
     * @param error error
     * @param listener action listener
     */
    public void updateLatestRealtimeTaskOnCoordinatingNode(
        String configId,
        String state,
        Long rcfTotalUpdates,
        Long intervalInMinutes,
        String error,
        ActionListener<UpdateResponse> listener
    ) {
        Float initProgress = null;
        String newState = null;
        // calculate init progress and task state with RCF total updates
        if (intervalInMinutes != null && rcfTotalUpdates != null) {
            newState = TaskState.INIT.name();
            if (rcfTotalUpdates < TimeSeriesSettings.NUM_MIN_SAMPLES) {
                initProgress = (float) rcfTotalUpdates / TimeSeriesSettings.NUM_MIN_SAMPLES;
            } else {
                newState = TaskState.RUNNING.name();
                initProgress = 1.0f;
            }
        }
        // Check if new state is not null and override state calculated with rcf total updates
        if (state != null) {
            newState = state;
        }

        error = Optional.ofNullable(error).orElse("");
        if (!taskCacheManager.isRealtimeTaskChangeNeeded(configId, newState, initProgress, error)) {
            // If task not changed, no need to update, just return
            listener.onResponse(null);
            return;
        }
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(TimeSeriesTask.COORDINATING_NODE_FIELD, clusterService.localNode().getId());
        if (initProgress != null) {
            updatedFields.put(TimeSeriesTask.INIT_PROGRESS_FIELD, initProgress);
            updatedFields
                .put(
                    TimeSeriesTask.ESTIMATED_MINUTES_LEFT_FIELD,
                    Math.max(0, TimeSeriesSettings.NUM_MIN_SAMPLES - rcfTotalUpdates) * intervalInMinutes
                );
        }
        if (newState != null) {
            updatedFields.put(TimeSeriesTask.STATE_FIELD, newState);
        }
        if (error != null) {
            updatedFields.put(TimeSeriesTask.ERROR_FIELD, error);
        }
        Float finalInitProgress = initProgress;
        // Variable used in lambda expression should be final or effectively final
        String finalError = error;
        String finalNewState = newState;
        updateLatestTask(configId, realTimeTaskTypes, updatedFields, ActionListener.wrap(r -> {
            logger.debug("Updated latest realtime AD task successfully for detector {}", configId);
            taskCacheManager.updateRealtimeTaskCache(configId, finalNewState, finalInitProgress, finalError);
            listener.onResponse(r);
        }, e -> {
            logger.error("Failed to update realtime task for detector " + configId, e);
            listener.onFailure(e);
        }));
    }

    /**
     * Update latest task of a config.
     *
     * @param configId config id
     * @param taskTypes task types
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateLatestTask(
        String configId,
        List<TaskTypeEnum> taskTypes,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        getAndExecuteOnLatestConfigLevelTask(configId, taskTypes, (task) -> {
            if (task.isPresent()) {
                updateTask(task.get().getTaskId(), updatedFields, listener);
            } else {
                listener.onFailure(new ResourceNotFoundException(configId, CommonMessages.CAN_NOT_FIND_LATEST_TASK));
            }
        }, null, false, listener);
    }

    /**
     * Update task for specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateTask(String taskId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(stateIndex, taskId);
        Map<String, Object> updatedContent = new HashMap<>();
        updatedContent.putAll(updatedFields);
        updatedContent.put(TimeSeriesTask.LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
        updateRequest.doc(updatedContent);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.update(updateRequest, listener);
    }

    /**
     * Get latest task and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param configId config id
     * @param taskTypes task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAndExecuteOnLatestConfigLevelTask(
        String configId,
        List<TaskTypeEnum> taskTypes,
        Consumer<Optional<TaskClass>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestADTask(configId, null, null, taskTypes, function, transportService, resetTaskState, listener);
    }

    /**
     * Get one latest task and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param configId config id
     * @param parentTaskId parent task id
     * @param entity entity value
     * @param taskTypes task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAndExecuteOnLatestADTask(
        String configId,
        String parentTaskId,
        Entity entity,
        List<TaskTypeEnum> taskTypes,
        Consumer<Optional<TaskClass>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestTasks(configId, parentTaskId, entity, taskTypes, (taskList) -> {
            if (taskList != null && taskList.size() > 0) {
                function.accept(Optional.ofNullable(taskList.get(0)));
            } else {
                function.accept(Optional.empty());
            }
        }, transportService, resetTaskState, 1, listener);
    }

    /**
     *  the function initializes the cache and only performs cleanup if it is deemed necessary.
     * @param id config id
     * @param config config accessor
     * @param transportService Transport service
     * @param listener listener to return back init success or not
     */
    public abstract void initCacheWithCleanupIfRequired(
        String id,
        Config config,
        TransportService transportService,
        ActionListener<Boolean> listener
    );

    public abstract <T> void getAndExecuteOnLatestTasks(
        String configId,
        String parentTaskId,
        Entity entity,
        List<TaskTypeEnum> taskTypes,
        Consumer<List<TaskClass>> function,
        TransportService transportService,
        boolean resetTaskState,
        int size,
        ActionListener<T> listener
    );
}
