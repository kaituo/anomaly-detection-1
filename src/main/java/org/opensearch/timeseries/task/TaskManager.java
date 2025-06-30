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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.constant.CommonMessages.CONFIG_IS_RUNNING;
import static org.opensearch.timeseries.model.TaskState.NOT_ENDED_STATES;
import static org.opensearch.timeseries.model.TaskType.taskTypeToString;
import static org.opensearch.timeseries.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.timeseries.util.RestHandlerUtils.createXContentParserFromRegistry;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.common.exception.DuplicateTaskException;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TaskCancelledException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.function.ResponseTransformer;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;

public abstract class TaskManager<TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, DataStoreType extends DelegatingDataManagement<? extends Enum<? extends TimeSeriesIndex>>> {
    protected static int DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS = 5;
    private static final int MAX_UPDATE_LATEST_REALTIME_TASK_RETRIES = 10;
    private static final TimeValue UPDATE_LATEST_REALTIME_TASK_RETRY_INTERVAL = TimeValue.timeValueMillis(2000);

    private final Logger logger = LogManager.getLogger(TaskManager.class);

    protected final TaskCacheManagerType taskCacheManager;
    protected final ClusterService clusterService;

    protected final String stateIndex;
    protected final List<TaskTypeEnum> realTimeTaskTypes;
    private final List<TaskTypeEnum> historicalTaskTypes;
    private final List<TaskTypeEnum> runOnceTaskTypes;
    protected final StateManager nodeStateManager;
    protected final AnalysisType analysisType;
    protected final NamedXContentRegistry xContentRegistry;
    protected final String configIdFieldName;
    protected final DataAccess dataAccess;

    protected volatile Integer maxOldTaskDocsPerConfig;

    protected final ThreadPool threadPool;
    private final String allResultIndexPattern;
    private final String batchTaskThreadPoolName;
    private volatile boolean deleteResultWhenDeleteConfig;
    private final TaskState stopped;

    public TaskManager(
        TaskCacheManagerType taskCacheManager,
        ClusterService clusterService,
        String stateIndex,
        List<TaskTypeEnum> realTimeTaskTypes,
        List<TaskTypeEnum> historicalTaskTypes,
        List<TaskTypeEnum> runOnceTaskTypes,
        StateManager nodeStateManager,
        AnalysisType analysisType,
        NamedXContentRegistry xContentRegistry,
        String configIdFieldName,
        Setting<Integer> maxOldADTaskDocsPerConfigSetting,
        Settings settings,
        ThreadPool threadPool,
        DataAccess taskSearcher,
        String allResultIndexPattern,
        String batchTaskThreadPoolName,
        Setting<Boolean> deleteResultWhenDeleteConfigSetting,
        TaskState stopped
    ) {
        this.taskCacheManager = taskCacheManager;
        this.clusterService = clusterService;
        this.stateIndex = stateIndex;
        this.realTimeTaskTypes = realTimeTaskTypes;
        this.historicalTaskTypes = historicalTaskTypes;
        this.runOnceTaskTypes = runOnceTaskTypes;
        this.nodeStateManager = nodeStateManager;
        this.analysisType = analysisType;
        this.xContentRegistry = xContentRegistry;
        this.configIdFieldName = configIdFieldName;
        this.dataAccess = Objects.requireNonNull(taskSearcher, "taskSearcher must not be null");

        this.maxOldTaskDocsPerConfig = maxOldADTaskDocsPerConfigSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(maxOldADTaskDocsPerConfigSetting, it -> maxOldTaskDocsPerConfig = it);

        this.threadPool = threadPool;
        this.allResultIndexPattern = allResultIndexPattern;
        this.batchTaskThreadPoolName = batchTaskThreadPoolName;

        this.deleteResultWhenDeleteConfig = deleteResultWhenDeleteConfigSetting.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(deleteResultWhenDeleteConfigSetting, it -> deleteResultWhenDeleteConfig = it);

        this.stopped = stopped;
    }

    public DataAccess getDataAccess() {
        return dataAccess;
    }

    public StateManager getStateManager() {
        return nodeStateManager;
    }

    public boolean skipUpdateRealtimeTask(String configId, String error) {
        RealtimeTaskCache realtimeTaskCache = taskCacheManager.getRealtimeTaskCache(configId);
        return realtimeTaskCache != null
            && realtimeTaskCache.getInitProgress() != null
            && realtimeTaskCache.getInitProgress().floatValue() == 1.0
            && Objects.equals(error, realtimeTaskCache.getError());
    }

    public boolean isRealtimeTaskStartInitializing(String configId) {
        RealtimeTaskCache realtimeTaskCache = taskCacheManager.getRealtimeTaskCache(configId);
        return realtimeTaskCache != null
            && realtimeTaskCache.getInitProgress() != null
            && realtimeTaskCache.getInitProgress().floatValue() > 0;
    }

    /**
     * Maintain running realtime tasks. Check if realtime task cache expires or not. Remove realtime
     * task cache directly if expired.
     */
    public void maintainRunningRealtimeTasks() {
        String[] configIds = taskCacheManager.getConfigIdsInRealtimeTaskCache();
        if (configIds == null || configIds.length == 0) {
            return;
        }
        for (int i = 0; i < configIds.length; i++) {
            String configId = configIds[i];
            RealtimeTaskCache taskCache = taskCacheManager.getRealtimeTaskCache(configId);
            if (taskCache != null && taskCache.expired()) {
                taskCacheManager.removeRealtimeTaskCache(configId);
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
     * @param coordinatingNode whether this function is called on coordinating node or not
     * @param listener action listener
     */
    public void updateLatestRealtimeTask(
        String configId,
        String tenantId,
        String state,
        Long rcfTotalUpdates,
        Long intervalInMinutes,
        String error,
        boolean coordinatingNode,
        Boolean hasResult,
        ActionListener<UpdateResponse> listener
    ) {

        // Check if job is enabled before proceeding
        nodeStateManager.getJob(configId, tenantId, false, ActionListener.wrap(jobOptional -> {
            boolean jobEnabled = jobOptional.isPresent() && jobOptional.get().isEnabled();

            String newState = null;

            // If job is not enabled, set state to STOPPED and
            if (!jobEnabled) {
                newState = TaskState.STOPPED.name();
            } // Check if new state is not null and ignore state calculated from rcf total updates
            else if (state != null) {
                newState = state;
            } else {
                newState = triageState(hasResult, error, rcfTotalUpdates);
            }

            // update error if necessary
            String finalError = Optional.ofNullable(error).orElse("");
            // calculate init progress and task state with RCF total updates
            Float initProgress = null;

            if (hasResult) {
                initProgress = 1.0f;
            } else if (intervalInMinutes != null && rcfTotalUpdates != null) {
                // progress is fraction of the min-sample requirement, capped at 1.0
                initProgress = Math.min((float) rcfTotalUpdates / TimeSeriesSettings.NUM_MIN_SAMPLES, 1.0f);
            }

            RealtimeTaskCache realtimeTaskCache = taskCacheManager.getRealtimeTaskCache(configId);
            String oldState = null;
            if (realtimeTaskCache != null) {
                oldState = realtimeTaskCache.getState();
            }

            // We don't want to change state from running to init.
            // Also, if task not changed at all, no need to update, just return.
            if (!taskCacheManager.isRealtimeTaskChangeNeeded(configId, newState, initProgress, finalError)
                || forbidOverrideChange(configId, newState, oldState)) {
                listener.onResponse(null);
                return;
            }
            Map<String, Object> updatedFields = new HashMap<>();

            if (coordinatingNode) {
                updatedFields.put(TimeSeriesTask.COORDINATING_NODE_FIELD, clusterService.localNode().getId());
            }

            // no need to update init progress if state is STOPPED
            if (initProgress != null && newState != TaskState.STOPPED.name()) {
                if (Boolean.TRUE.equals(hasResult)) {
                    updatedFields.put(TimeSeriesTask.INIT_PROGRESS_FIELD, initProgress);
                    updatedFields.put(TimeSeriesTask.ESTIMATED_MINUTES_LEFT_FIELD, 0);
                } else if (rcfTotalUpdates != null && rcfTotalUpdates > 0) {
                    updatedFields.put(TimeSeriesTask.INIT_PROGRESS_FIELD, initProgress);
                    updatedFields
                        .put(
                            TimeSeriesTask.ESTIMATED_MINUTES_LEFT_FIELD,
                            Math.max(0, TimeSeriesSettings.NUM_MIN_SAMPLES - rcfTotalUpdates) * intervalInMinutes
                        );
                }
            }
            if (newState != null) {
                updatedFields.put(TimeSeriesTask.STATE_FIELD, newState);
            }
            if (finalError != null) {
                updatedFields.put(TimeSeriesTask.ERROR_FIELD, finalError);
            }
            Float finalInitProgress = initProgress;
            String finalNewState = newState;
            updateLatestRealtimeTaskWithRetry(
                configId,
                tenantId,
                updatedFields,
                MAX_UPDATE_LATEST_REALTIME_TASK_RETRIES,
                ActionListener.wrap(r -> {
                    logger.debug("Updated latest realtime AD task successfully for config {}", configId);
                    taskCacheManager.updateRealtimeTaskCache(configId, finalNewState, finalInitProgress, finalError);
                    listener.onResponse(r);
                }, e -> {
                    logger.error("Failed to update realtime task for config " + configId, e);
                    listener.onFailure(e);
                })
            );
        }, e -> {
            logger.error("Failed to get job for config " + configId, e);
            listener.onFailure(e);
        }));
    }

    /**
     * Retry updating latest realtime task to handle a short visibility gap in coordinator mode.
     *
     * In multi-tenant coordinator mode, some start paths such as EventBridge-backed start do not
     * create a realtime task document immediately.
     * On first execution, the analysis-type-specific task manager may recreate the missing realtime task.
     * Immediately after recreation, backend visibility can lag, so searching the latest task in
     * {@link #updateLatestTask(String, String, List, Map, ActionListener)} can briefly return empty and
     * raise {@code can't find latest task}. We retry that specific transient case.
     *
     * @param configId config id
     * @param tenantId tenant id
     * @param updatedFields updated fields
     * @param remainingRetries remaining retries
     * @param listener action listener
     */
    private void updateLatestRealtimeTaskWithRetry(
        String configId,
        String tenantId,
        Map<String, Object> updatedFields,
        int remainingRetries,
        ActionListener<UpdateResponse> listener
    ) {
        updateLatestTask(configId, tenantId, realTimeTaskTypes, updatedFields, ActionListener.wrap(listener::onResponse, e -> {
            boolean missingLatestTask = (e instanceof ResourceNotFoundException)
                && e.getMessage() != null
                && e.getMessage().contains(CommonMessages.CAN_NOT_FIND_LATEST_TASK);

            if (!missingLatestTask || remainingRetries <= 0) {
                listener.onFailure(e);
                return;
            }

            logger.debug("Latest realtime task is not visible yet for config {}, retries left: {}", configId, remainingRetries);
            threadPool
                .schedule(
                    () -> updateLatestRealtimeTaskWithRetry(configId, tenantId, updatedFields, remainingRetries - 1, listener),
                    UPDATE_LATEST_REALTIME_TASK_RETRY_INTERVAL,
                    ThreadPool.Names.GENERIC
                );
        }));
    }

    public void updateLatestRealtimeTaskOnCoordinatingNode(
        String configId,
        String tenantId,
        String state,
        Long rcfTotalUpdates,
        Long intervalInMinutes,
        String error,
        Boolean hasResult,
        ActionListener<UpdateResponse> listener
    ) {
        updateLatestRealtimeTask(configId, tenantId, state, rcfTotalUpdates, intervalInMinutes, error, true, hasResult, listener);
    }

    public void updateLatestTask(
        String configId,
        String tenantId,
        List<TaskTypeEnum> taskTypes,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        logger
            .info(
                "updateLatestTask enter config={} tenant={} taskTypes={} updatedFieldKeys={}",
                configId,
                tenantId,
                taskTypes == null ? List.of() : taskTypeToString(taskTypes),
                updatedFields.keySet()
            );

        getAndExecuteOnLatestConfigLevelTask(configId, tenantId, taskTypes, (task) -> {
            if (task.isPresent()) {
                logger.info("found latest realtime task for update config={} taskId={}", configId, task.get().getTaskId());
                updateTask(task.get().getTaskId(), updatedFields, tenantId, listener);
            } else {
                logger.info("latest realtime task missing during update config={} tenant={}", configId, tenantId);
                listener.onFailure(new ResourceNotFoundException(configId, CommonMessages.CAN_NOT_FIND_LATEST_TASK));
            }
        }, null, false, listener);
    }

    public void getAndExecuteOnLatestConfigLevelTask(
        Config config,
        String tenantId,
        DateRange dateRange,
        boolean runOnce,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        getAndExecuteOnLatestConfigLevelTask(config.getId(), tenantId, getTaskTypes(dateRange), (task) -> {
            if (!task.isPresent() || task.get().isDone()) {
                updateLatestFlagOfOldTasksAndCreateNewTask(config, dateRange, runOnce, user, tenantId, TaskState.CREATED, listener);
            } else {
                listener.onFailure(new OpenSearchStatusException(CONFIG_IS_RUNNING, RestStatus.BAD_REQUEST));
            }
        }, transportService, true, listener);
    }

    public <T> void updateLatestFlagOfOldTasksAndCreateNewTask(
        Config config,
        DateRange dateRange,
        boolean runOnce,
        User user,
        String tenantId,
        TaskState initialState,
        ActionListener<T> listener
    ) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(stateIndex);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(configIdFieldName, config.getId()));
        query.filter(new TermQueryBuilder(TimeSeriesTask.IS_LATEST_FIELD, true));
        // make sure we reset all latest task as false when user switch from single entity to HC, vice versa.
        // Ensures that only the latest flags of the same analysis type are reset:
        // Real-time analysis will only reset the latest flag of previous real-time analyses.
        // Historical analysis will only reset the latest flag of previous historical analyses.
        query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(getTaskTypes(dateRange, runOnce))));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        String script = String.format(Locale.ROOT, "ctx._source.%s=%s;", TimeSeriesTask.IS_LATEST_FIELD, false);
        updateByQueryRequest.setScript(new Script(script));

        dataAccess.updateByQuery(updateByQueryRequest, TenantContext.user(tenantId), ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (bulkFailures.isEmpty()) {
                // Realtime AD coordinating node is chosen by job scheduler, we won't know it until realtime AD job
                // runs. Just set realtime AD coordinating node as null here, and AD job runner will reset correct
                // coordinating node once realtime job starts.
                // For historical analysis, this method will be called on coordinating node, so we can set coordinating
                // node as local node.
                String coordinatingNode = dateRange == null ? null : clusterService.localNode().getId();
                createNewTask(config, dateRange, runOnce, user, coordinatingNode, initialState, listener);
            } else {
                logger.error("Failed to update old task's state for config: {}, response: {} ", config.getId(), r.toString());
                listener.onFailure(bulkFailures.get(0).getCause());
            }
        }, e -> {
            logger.error("Failed to reset old tasks as not latest for config " + config.getId(), e);
            listener.onFailure(e);
        }));
    }

    public <T> void getAndExecuteOnLatestConfigLevelTask(
        String configId,
        String tenantId,
        List<TaskTypeEnum> taskTypes,
        Consumer<Optional<TaskClass>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestConfigTask(configId, null, null, tenantId, taskTypes, function, transportService, resetTaskState, listener);
    }

    public <T> void getAndExecuteOnLatestConfigTask(
        String configId,
        String parentTaskId,
        Entity entity,
        String tenantId,
        List<TaskTypeEnum> taskTypes,
        Consumer<Optional<TaskClass>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestTasks(configId, parentTaskId, entity, tenantId, taskTypes, (taskList) -> {
            Optional<TaskClass> latestTask = taskList != null && taskList.size() > 0
                ? Optional.ofNullable(taskList.get(0))
                : Optional.empty();
            logger
                .info(
                    "Latest config task callback enter config={} tenant={} resetTaskState={} taskPresent={} firstTaskId={}",
                    configId,
                    tenantId,
                    resetTaskState,
                    latestTask.isPresent(),
                    latestTask.map(TimeSeriesTask::getTaskId).orElse("none")
                );
            try {
                function.accept(latestTask);
                logger
                    .info(
                        "Latest config task callback exit config={} tenant={} resetTaskState={} taskPresent={}",
                        configId,
                        tenantId,
                        resetTaskState,
                        latestTask.isPresent()
                    );
            } catch (RuntimeException e) {
                logger
                    .error(
                        "Latest config task callback failed config={} tenant={} resetTaskState={} taskPresent={}",
                        configId,
                        tenantId,
                        resetTaskState,
                        latestTask.isPresent(),
                        e
                    );
                throw e;
            }
        }, transportService, resetTaskState, 1, listener);
    }

    public List<TaskTypeEnum> getTaskTypes(DateRange dateRange) {
        return getTaskTypes(dateRange, false);
    }

    /**
     * Update latest realtime task.
     *
     * @param configId config id
     * @param state task state
     * @param error error
     * @param transportService transport service
     * @param listener action listener
     */
    public void stopLatestRealtimeTask(
        String configId,
        String tenantId,
        TaskState state,
        Exception error,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        stopLatestRealtimeTaskWithRetry(
            configId,
            tenantId,
            state,
            error,
            transportService,
            MAX_UPDATE_LATEST_REALTIME_TASK_RETRIES,
            listener
        );
    }

    private void stopLatestRealtimeTaskWithRetry(
        String configId,
        String tenantId,
        TaskState state,
        Exception error,
        TransportService transportService,
        int remainingRetries,
        ActionListener<JobResponse> listener
    ) {
        getAndExecuteOnLatestConfigLevelTask(configId, tenantId, realTimeTaskTypes, (adTask) -> {
            if (!adTask.isPresent()) {
                if (remainingRetries > 0) {
                    logger
                        .debug(
                            "Latest realtime task is not visible yet when stopping config {}, retries left: {}",
                            configId,
                            remainingRetries
                        );
                    threadPool
                        .schedule(
                            () -> stopLatestRealtimeTaskWithRetry(
                                configId,
                                tenantId,
                                state,
                                error,
                                transportService,
                                remainingRetries - 1,
                                listener
                            ),
                            UPDATE_LATEST_REALTIME_TASK_RETRY_INTERVAL,
                            ThreadPool.Names.GENERIC
                        );
                } else {
                    listener.onFailure(stopLatestRealtimeTaskTerminalError(configId, error));
                }
                return;
            }

            if (adTask.get().isDone()) {
                listener.onFailure(stopLatestRealtimeTaskTerminalError(configId, error));
                return;
            }

            Map<String, Object> updatedFields = new HashMap<>();
            updatedFields.put(TimeSeriesTask.STATE_FIELD, state.name());
            if (error != null) {
                updatedFields.put(TimeSeriesTask.ERROR_FIELD, ExceptionUtil.getErrorMessage(error));
            }
            ExecutorFunction function = () -> updateTask(adTask.get().getTaskId(), updatedFields, tenantId, ActionListener.wrap(r -> {
                if (error == null) {
                    listener.onResponse(new JobResponse(configId));
                } else {
                    listener.onFailure(error);
                }
            }, e -> { listener.onFailure(e); }));

            String coordinatingNode = adTask.get().getCoordinatingNode();
            if (coordinatingNode != null && transportService != null) {
                cleanConfigCache(adTask.get(), transportService, function, listener);
            } else {
                function.execute();
            }
        }, null, false, listener);
    }

    private Exception stopLatestRealtimeTaskTerminalError(String configId, Exception error) {
        return error != null ? error : new OpenSearchStatusException("job is already stopped: " + configId, RestStatus.OK);
    }

    protected <T> void resetTaskStateAsStopped(
        TimeSeriesTask task,
        ExecutorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        cleanConfigCache(task, transportService, () -> {
            String taskId = task.getTaskId();
            Map<String, Object> updatedFields = ImmutableMap.of(TimeSeriesTask.STATE_FIELD, stopped.name());
            updateTask(taskId, updatedFields, task.getTenantId(), ActionListener.wrap(r -> {
                task.setState(stopped.name());
                if (function != null) {
                    function.execute();
                }
                // For realtime anomaly detection, we only create config level task, no entity level realtime task.
                if (isHistoricalHCTask(task)) {
                    // Reset running entity tasks as STOPPED
                    resetEntityTasksAsStopped(taskId, task.getTenantId());
                }
            }, e -> {
                logger.error("Failed to update task state as stopped for task " + taskId, e);
                listener.onFailure(e);
            }));
        }, listener);
    }

    public <T> void getAndExecuteOnLatestTasks(
        String configId,
        String parentTaskId,
        Entity entity,
        String tenantId,
        List<TaskTypeEnum> taskTypes,
        Consumer<List<TaskClass>> function,
        TransportService transportService,
        boolean resetTaskState,
        int size,
        ActionListener<T> listener
    ) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(configIdFieldName, configId));
        query.filter(new TermQueryBuilder(TimeSeriesTask.IS_LATEST_FIELD, true));
        if (parentTaskId != null) {
            query.filter(new TermQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, parentTaskId));
        }
        if (taskTypes != null && taskTypes.size() > 0) {
            query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, TaskType.taskTypeToString(taskTypes)));
        }
        if (entity != null && !ParseUtils.isNullOrEmpty(entity.getAttributes())) {
            String path = "entity";
            String entityKeyFieldName = path + ".name";
            String entityValueFieldName = path + ".value";

            for (Map.Entry<String, String> attribute : entity.getAttributes().entrySet()) {
                BoolQueryBuilder entityBoolQuery = new BoolQueryBuilder();
                TermQueryBuilder entityKeyFilterQuery = QueryBuilders.termQuery(entityKeyFieldName, attribute.getKey());
                TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValueFieldName, attribute.getValue());

                entityBoolQuery.filter(entityKeyFilterQuery).filter(entityValueFilterQuery);
                NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, entityBoolQuery, ScoreMode.None);
                query.filter(nestedQueryBuilder);
            }
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query).sort(TimeSeriesTask.EXECUTION_START_TIME_FIELD, SortOrder.DESC).size(size);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(stateIndex);
        dataAccess.search(searchRequest, TenantContext.user(tenantId), ActionListener.wrap(r -> {
            // https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359#discussion_r558653132
            // getTotalHits will be null when we track_total_hits is false in the query request.
            // Add more checking here to cover some unknown cases.
            List<TaskClass> tsTasks = new ArrayList<>();
            long totalHits = (r == null || r.getHits().getTotalHits() == null) ? -1 : r.getHits().getTotalHits().value();
            logger
                .info(
                    "Latest task search completed config={} tenant={} resetTaskState={} requestedSize={} taskTypes={} totalHits={}",
                    configId,
                    tenantId,
                    resetTaskState,
                    size,
                    taskTypes == null ? "[]" : TaskType.taskTypeToString(taskTypes),
                    totalHits
                );
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value() == 0) {
                // don't throw exception here as consumer functions need to handle missing task
                // in different way.
                logger
                    .info(
                        "Latest task search returned no tasks config={} tenant={} resetTaskState={} taskTypes={}",
                        configId,
                        tenantId,
                        resetTaskState,
                        taskTypes == null ? "[]" : TaskType.taskTypeToString(taskTypes)
                    );
                function.accept(tsTasks);
                return;
            }
            BiCheckedFunction<XContentParser, String, TaskClass, IOException> parserMethod = getTaskParser();
            Iterator<SearchHit> iterator = r.getHits().iterator();
            while (iterator.hasNext()) {
                SearchHit searchHit = iterator.next();
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    TaskClass tsTask = parserMethod.apply(parser, searchHit.getId());
                    tsTasks.add(tsTask);
                } catch (Exception e) {
                    String message = "Failed to parse task for config " + configId + ", task id " + searchHit.getId();
                    logger.error(message, e);
                    listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }
            logger
                .info(
                    "Latest task search parsed tasks config={} tenant={} resetTaskState={} parsedCount={}",
                    configId,
                    tenantId,
                    resetTaskState,
                    tsTasks.size()
                );
            if (resetTaskState) {
                logger.info("Latest task search delegating to resetLatestConfigTaskState config={} tenant={}", configId, tenantId);
                resetLatestConfigTaskState(tsTasks, function, transportService, listener);
            } else {
                logger.info("Latest task search invoking callback directly config={} tenant={}", configId, tenantId);
                function.accept(tsTasks);
            }
        }, e -> {
            if (e instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(e)) {
                logger.info("Latest task search treated missing state index as empty result config={} tenant={}", configId, tenantId);
                function.accept(new ArrayList<>());
            } else if (e instanceof SearchPhaseExecutionException) {
                logger.info("Failed to search task for config " + configId, e);
                // e.getMessage(): "No mapping found for" or "all shards failed" likely due to state index hasn't finished initialization
                logger.info("Latest task search treated search phase failure as empty result config={} tenant={}", configId, tenantId);
                function.accept(new ArrayList<>());
            } else {
                // unknown exceptions
                logger.error("Failed to search task for config " + configId, e);
                listener.onFailure(e);
            }
        }));
    }

    protected <T> void resetRealtimeConfigTaskState(
        List<TimeSeriesTask> runningRealtimeTasks,
        ExecutorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        if (ParseUtils.isNullOrEmpty(runningRealtimeTasks)) {
            function.execute();
            return;
        }
        TimeSeriesTask tsTask = runningRealtimeTasks.get(0);
        String configId = tsTask.getConfigId();
        String tenantId = tsTask.getTenantId();
        nodeStateManager.getJob(configId, tenantId, false, ActionListener.wrap(jobOptional -> {
            if (jobOptional.isPresent()) {
                Job job = jobOptional.get();
                if (!job.isEnabled()) {
                    logger.debug("job is disabled, reset realtime task as stopped for config {}", configId);
                    resetTaskStateAsStopped(tsTask, function, transportService, listener);
                } else {
                    function.execute();
                }
            } else {
                logger.debug("job is not found, reset realtime task as stopped for config {}", configId);
                resetTaskStateAsStopped(tsTask, function, transportService, listener);
            }
        }, e -> {
            if (e instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(e)) {
                logger.debug("job is not found, reset realtime task as stopped for config {}", configId);
                resetTaskStateAsStopped(tsTask, function, transportService, listener);
            } else {
                logger.error("Fail to get realtime job for config " + configId, e);
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Handle exceptions for task. Update task state and record error message.
     *
     * @param task AD task
     * @param e exception
     */
    public void handleTaskException(TaskClass task, Exception e) {
        // TODO: handle timeout exception
        String state = TaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (e instanceof DuplicateTaskException) {
            // If user send multiple start detector request, we will meet race condition.
            // Cache manager will put first request in cache and throw DuplicateTaskException
            // for the second request. We will delete the second task.
            logger
                .warn(
                    "There is already one running task for config, configId:"
                        + task.getConfigId()
                        + ". Will delete task "
                        + task.getTaskId()
                );
            deleteTask(task.getTaskId(), task.getTenantId());
            return;
        }
        if (e instanceof TaskCancelledException) {
            logger.info("task cancelled, taskId: {}, configId: {}", task.getTaskId(), task.getConfigId());
            state = stopped.name();
            String stoppedBy = ((TaskCancelledException) e).getCancelledBy();
            if (stoppedBy != null) {
                updatedFields.put(TimeSeriesTask.STOPPED_BY_FIELD, stoppedBy);
            }
        } else {
            logger.error("Failed to execute batch task, task id: " + task.getTaskId() + ", config id: " + task.getConfigId(), e);
        }
        updatedFields.put(TimeSeriesTask.ERROR_FIELD, ExceptionUtil.getErrorMessage(e));
        updatedFields.put(TimeSeriesTask.STATE_FIELD, state);
        updatedFields.put(TimeSeriesTask.EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
        updateTask(task.getTaskId(), updatedFields, task.getTenantId());
    }

    /**
     * Update task with specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param tenantId tenant id
     */
    public void updateTask(String taskId, Map<String, Object> updatedFields, String tenantId) {
        updateTask(taskId, updatedFields, tenantId, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated task successfully: {}, task id: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task: " + taskId, e); }));
    }

    /**
     * Update task for specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param tenantId tenant id
     * @param listener action listener
     */
    public void updateTask(String taskId, Map<String, Object> updatedFields, String tenantId, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(stateIndex, taskId);
        Map<String, Object> updatedContent = new HashMap<>();
        updatedContent.putAll(updatedFields);
        updatedContent.put(TimeSeriesTask.LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
        updateRequest.doc(updatedContent);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // OpenSearch will transparently re‑read the doc and retry up to 2 times.
        updateRequest.retryOnConflict(2);
        dataAccess.update(updateRequest, TenantContext.user(tenantId), listener);
    }

    /**
     * Delete task with task id and tenant.
     *
     * @param taskId task id
     * @param tenantId tenant id
     */
    public void deleteTask(String taskId, String tenantId) {
        deleteTask(
            taskId,
            tenantId,
            ActionListener.wrap(r -> { logger.info("Deleted task {} with status: {}", taskId, r.status()); }, e -> {
                logger.error("Failed to delete task " + taskId, e);
            })
        );
    }

    /**
     * Delete task with task id.
     *
     * @param taskId task id
     * @param tenantId tenant id
     * @param listener action listener
     */
    public void deleteTask(String taskId, String tenantId, ActionListener<DeleteResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(stateIndex, taskId);
        dataAccess.delete(deleteRequest, TenantContext.user(tenantId), listener);
    }

    /**
     * Create config task directly without checking index exists of not.
     * [Important!] Make sure listener returns in function
     *
     * @param tsTask Time series task
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void createTaskDirectly(TaskClass tsTask, Consumer<IndexResponse> function, ActionListener<T> listener) {
        IndexRequest request = new IndexRequest(stateIndex);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                .source(tsTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            String tenantId = tsTask.getTenantId();
            dataAccess.index(request, TenantContext.user(tenantId), ActionListener.wrap(r -> function.accept(r), e -> {
                logger.error("Failed to create task for config " + tsTask.getConfigId(), e);
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            logger.error("Failed to create task for config " + tsTask.getConfigId(), e);
            listener.onFailure(e);
        }
    }

    protected <T> void cleanOldConfigTaskDocs(
        IndexResponse response,
        TaskClass tsTask,
        ResponseTransformer<IndexResponse, T> responseTransformer,
        ActionListener<T> delegatedListener
    ) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(configIdFieldName, tsTask.getConfigId()));
        query.filter(new TermQueryBuilder(TimeSeriesTask.IS_LATEST_FIELD, false));

        if (tsTask.isHistoricalTask()) {
            // If historical task, only delete detector level task. It may take longer time to delete entity tasks.
            // We will delete child task (entity task) of config level task in hourly cron job.
            query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(historicalTaskTypes)));
        } else if (tsTask.isRunOnceTask()) {
            // We don't have entity level task for run once detection, so will delete all tasks.
            query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(runOnceTaskTypes)));
        } else {
            // We don't have entity level task for realtime detection, so will delete all tasks.
            query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(realTimeTaskTypes)));
        }

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
            .query(query)
            .sort(TimeSeriesTask.EXECUTION_START_TIME_FIELD, SortOrder.DESC)
            // Search query "from" starts from maxOldTaskDocsPerConfig.
            .from(maxOldTaskDocsPerConfig)
            .size(MAX_OLD_AD_TASK_DOCS);
        searchRequest.source(sourceBuilder).indices(stateIndex);
        String configId = tsTask.getConfigId();
        deleteTaskDocs(configId, searchRequest, () -> {
            if (tsTask.isHistoricalTask()) {
                // run batch result action for historical analysis
                runBatchResultAction(response, tsTask, responseTransformer, delegatedListener);
            } else {
                // use the responseTransformer to transform the response
                T transformedResponse = responseTransformer.transform(response);
                delegatedListener.onResponse(transformedResponse);
            }
        }, tsTask.getTenantId(), delegatedListener);
    }

    public <T> void deleteTaskDocs(
        String configId,
        SearchRequest searchRequest,
        ExecutorFunction function,
        String tenantId,
        ActionListener<T> listener
    ) {
        deleteTaskDocs(configId, searchRequest, tenantId, function, listener);
    }

    public <T> void deleteTaskDocs(
        String configId,
        SearchRequest searchRequest,
        String tenantId,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        ActionListener<SearchResponse> searchListener = ActionListener.wrap(r -> {
            Iterator<SearchHit> iterator = r.getHits().iterator();
            if (iterator.hasNext()) {
                BulkRequest bulkRequest = new BulkRequest();
                while (iterator.hasNext()) {
                    SearchHit searchHit = iterator.next();
                    try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        TimeSeriesTask task = null;
                        if (analysisType.isAD()) {
                            task = ADTask.parse(parser, searchHit.getId());
                        } else {
                            task = ForecastTask.parse(parser, searchHit.getId());
                        }

                        logger.debug("Delete old task: {} of config: {}", task.getTaskId(), task.getConfigId());
                        bulkRequest.add(new DeleteRequest(stateIndex).id(task.getTaskId()));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
                dataAccess.bulk(bulkRequest, TenantContext.user(tenantId), ActionListener.wrap(res -> {
                    logger.info("Old tasks deleted for config {}", configId);
                    BulkItemResponse[] bulkItemResponses = res.getItems();
                    if (bulkItemResponses != null && bulkItemResponses.length > 0) {
                        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                            if (!bulkItemResponse.isFailed()) {
                                logger.debug("Add config task into cache. Task id: {}", bulkItemResponse.getId());
                                // add deleted task in cache and delete its child tasks and results
                                taskCacheManager.addDeletedTask(bulkItemResponse.getId(), tenantId);
                            }
                        }
                    }
                    // delete child tasks and results of this task
                    cleanChildTasksAndResultsOfDeletedTask();
                    function.execute();
                }, e -> {
                    logger.warn("Failed to clean tasks for config " + configId, e);
                    listener.onFailure(e);
                }));
            } else {
                function.execute();
            }
        }, e -> {
            if (e instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(e)) {
                function.execute();
            } else {
                listener.onFailure(e);
            }
        });

        dataAccess.search(searchRequest, TenantContext.user(tenantId), searchListener);
    }

    /**
     * Poll deleted config task from cache and delete its child tasks and results.
     */
    public void cleanChildTasksAndResultsOfDeletedTask() {
        if (!taskCacheManager.hasDeletedTask()) {
            return;
        }
        threadPool.schedule(() -> {
            Pair<String, String> deletedTask = taskCacheManager.pollDeletedTask();
            if (deletedTask == null) {
                return;
            }
            String taskId = deletedTask.getLeft();
            String tenantId = deletedTask.getRight();
            if (taskId == null) {
                return;
            }
            DeleteByQueryRequest deleteResultsRequest = new DeleteByQueryRequest(allResultIndexPattern);
            deleteResultsRequest.setQuery(new TermsQueryBuilder(CommonName.TASK_ID_FIELD, taskId));
            dataAccess.deleteByQuery(deleteResultsRequest, TenantContext.user(tenantId), ActionListener.wrap(res -> {
                logger.debug("Successfully deleted {} results of task {}", res.getDeleted(), taskId);
                DeleteByQueryRequest deleteChildTasksRequest = new DeleteByQueryRequest(stateIndex);
                deleteChildTasksRequest.setQuery(new TermsQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, taskId));

                dataAccess.deleteByQuery(deleteChildTasksRequest, TenantContext.user(tenantId), ActionListener.wrap(r -> {
                    logger.debug("Successfully deleted {} child tasks of task {}", r.getDeleted(), taskId);
                    cleanChildTasksAndResultsOfDeletedTask();
                }, e -> { logger.error("Failed to delete child tasks of task " + taskId, e); }));
            }, ex -> { logger.error("Failed to delete results for task " + taskId, ex); }));
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), batchTaskThreadPoolName);
    }

    protected void resetEntityTasksAsStopped(String configTaskId, String tenantId) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(stateIndex);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, configTaskId));
        query.filter(new TermQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, ADTaskType.HISTORICAL_HC_ENTITY.name()));
        query.filter(new TermsQueryBuilder(TimeSeriesTask.STATE_FIELD, NOT_ENDED_STATES));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        String script = String.format(Locale.ROOT, "ctx._source.%s='%s';", TimeSeriesTask.STATE_FIELD, TaskState.INACTIVE.name());
        updateByQueryRequest.setScript(new Script(script));

        dataAccess.updateByQuery(updateByQueryRequest, TenantContext.user(tenantId), ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (ParseUtils.isNullOrEmpty(bulkFailures)) {
                logger.debug("Updated {} child entity tasks state for config task {}", r.getUpdated(), configTaskId);
            } else {
                logger.error("Failed to update child entity task's state for config task {} ", configTaskId);
            }
        }, e -> logger.error("Exception happened when update child entity task's state for config task " + configTaskId, e)));
    }

    /**
     * Set old task's latest flag as false.
     * @param tasks list of tasks
     */
    public void resetLatestFlagAsFalse(List<TaskClass> tasks, String tenantId) {
        if (tasks == null || tasks.size() == 0) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest();
        tasks.forEach(task -> {
            try {
                task.setLatest(false);
                task.setLastUpdateTime(Instant.now());
                IndexRequest indexRequest = new IndexRequest(stateIndex)
                    .id(task.getTaskId())
                    .source(task.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE));
                bulkRequest.add(indexRequest);
            } catch (Exception e) {
                logger.error("Fail to parse task task to XContent, task id " + task.getTaskId(), e);
            }
        });

        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        dataAccess.bulk(bulkRequest, TenantContext.user(tenantId), ActionListener.wrap(res -> {
            BulkItemResponse[] bulkItemResponses = res.getItems();
            if (bulkItemResponses != null && bulkItemResponses.length > 0) {
                for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                    if (!bulkItemResponse.isFailed()) {
                        logger.warn("Reset tasks latest flag as false Successfully. Task id: {}", bulkItemResponse.getId());
                    } else {
                        logger.warn("Failed to reset tasks latest flag as false. Task id: " + bulkItemResponse.getId());
                    }
                }
            }
        }, e -> { logger.warn("Failed to reset AD tasks latest flag as false", e); }));
    }

    /**
     * Delete tasks docs.
     * [Important!] Make sure listener returns in function
     *
     * @param configId config id
     * @param function time series function
     * @param listener action listener
     */
    public void deleteTasks(String configId, ExecutorFunction function, String tenantId, ActionListener<DeleteResponse> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(stateIndex);

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(configIdFieldName, configId));

        request.setQuery(query);
        dataAccess.deleteByQuery(request, TenantContext.user(tenantId), ActionListener.wrap(r -> {
            if (r.getBulkFailures() == null || r.getBulkFailures().size() == 0) {
                logger.info("{} tasks deleted for config {}", r.getDeleted(), configId);
                deleteResultOfConfig(configId, tenantId);
                function.execute();
            } else {
                listener.onFailure(new OpenSearchStatusException("Failed to delete all tasks", RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            logger.info("Failed to delete tasks for " + configId, e);
            if (e instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(e)) {
                deleteResultOfConfig(configId, tenantId);
                function.execute();
            } else {
                logger.error("Failed to delete tasks for " + configId, e);
                listener.onFailure(e);
            }
        }));
    }

    public void deleteResultOfConfig(String configId, String tenantId) {
        if (!deleteResultWhenDeleteConfig) {
            logger.info("Won't delete result for {} as delete result setting is disabled", configId);
            return;
        }
        logger.info("Start to delete results of config {}", configId);
        DeleteByQueryRequest deleteADResultsRequest = new DeleteByQueryRequest(allResultIndexPattern);
        deleteADResultsRequest.setQuery(new TermQueryBuilder(configIdFieldName, configId));
        dataAccess.deleteByQuery(deleteADResultsRequest, TenantContext.user(tenantId), ActionListener.wrap(response -> {
            logger.debug("Successfully deleted {} results of config {}", response.getDeleted(), configId);
        }, exception -> {
            logger.error("Failed to delete results of config " + configId, exception);
            taskCacheManager.addDeletedConfig(configId, tenantId);
        }));
    }

    /**
     * Get task with task id and execute listener.
     * @param taskId task id
     * @param tenantId tenant id
     * @param listener action listener
     */
    public void getTask(String taskId, String tenantId, ActionListener<Optional<TaskClass>> listener) {
        GetRequest request = new GetRequest(stateIndex, taskId);
        dataAccess.get(request, TenantContext.user(tenantId), ActionListener.wrap(r -> {
            if (r != null && r.isExists()) {
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    BiCheckedFunction<XContentParser, String, TaskClass, IOException> parserMethod = getTaskParser();
                    TaskClass tsTask = parserMethod.apply(parser, r.getId());
                    listener.onResponse(Optional.ofNullable(tsTask));
                } catch (Exception e) {
                    logger.error("Failed to parse task " + r.getId(), e);
                    listener.onFailure(e);
                }
            } else {
                listener.onResponse(Optional.empty());
            }
        }, e -> {
            if (e instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(e)) {
                listener.onResponse(Optional.empty());
            } else {
                logger.error("Failed to get task " + taskId, e);
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Clean results of deleted config.
     */
    public void cleanResultOfDeletedConfig() {
        Pair<String, String> deletedConfig = taskCacheManager.pollDeletedConfig();
        if (deletedConfig != null) {
            String configId = deletedConfig.getLeft();
            String tenantId = deletedConfig.getRight();
            deleteResultOfConfig(configId, tenantId);
        }
    }

    public abstract void startHistorical(
        Config config,
        DateRange dateRange,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    );

    protected abstract TaskType getTaskType(Config config, DateRange dateRange, boolean runOnce);

    protected abstract <T> void createNewTask(
        Config config,
        DateRange dateRange,
        boolean runOnce,
        User user,
        String coordinatingNode,
        TaskState initialState,
        ActionListener<T> listener
    );

    public abstract <T> void cleanConfigCache(
        TimeSeriesTask task,
        TransportService transportService,
        ExecutorFunction function,
        ActionListener<T> listener
    );

    protected abstract boolean isHistoricalHCTask(TimeSeriesTask task);

    protected abstract <T> void resetLatestConfigTaskState(
        List<TaskClass> tasks,
        Consumer<List<TaskClass>> function,
        TransportService transportService,
        ActionListener<T> listener
    );

    protected abstract <T> void onIndexConfigTaskResponse(
        IndexResponse response,
        TaskClass adTask,
        BiConsumer<IndexResponse, ActionListener<T>> function,
        ActionListener<T> listener
    );

    protected abstract <T> void runBatchResultAction(
        IndexResponse response,
        TaskClass tsTask,
        ResponseTransformer<IndexResponse, T> responseTransformer,
        ActionListener<T> listener
    );

    protected abstract BiCheckedFunction<XContentParser, String, TaskClass, IOException> getTaskParser();

    /**
     * the function initializes the real time cache and only performs cleanup if it is deemed necessary.
     * @param configId config id
     * @param config config accessor
     * @param transportService Transport service
     * @param listener listener to return back init success or not
     */
    public abstract void initRealtimeTaskCacheAndCleanupStaleCache(
        String configId,
        Config config,
        TransportService transportService,
        ActionListener<Boolean> listener
    );

    public abstract void createRunOnceTaskAndCleanupStaleTasks(
        String configId,
        Config config,
        TransportService transportService,
        ActionListener<TaskClass> listener
    );

    public abstract List<TaskTypeEnum> getTaskTypes(DateRange dateRange, boolean runOnce);

    protected abstract String triageState(Boolean hasResult, String error, Long rcfTotalUpdates);

    /**
     *
     * @param configId Config id
     * @param newState new state
     * @return Whether we should forbid overriding changes
     */
    protected abstract boolean forbidOverrideChange(String configId, String newState, String oldState);
}
