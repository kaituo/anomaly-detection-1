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

import static org.opensearch.ad.constant.ADCommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.REALTIME_TASK_TYPES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.constant.CommonMessages.CONFIG_IS_RUNNING;
import static org.opensearch.timeseries.model.TaskState.NOT_ENDED_STATES;
import static org.opensearch.timeseries.model.TaskType.taskTypeToString;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.common.exception.DuplicateTaskException;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TaskCancelledException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;

public abstract class TaskManager<TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>> {
    protected static int DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS = 5;

    private final Logger logger = LogManager.getLogger(TaskManager.class);

    protected final TaskCacheManagerType taskCacheManager;
    protected final ClusterService clusterService;
    protected final Client client;
    protected final String stateIndex;
    private final List<TaskTypeEnum> realTimeTaskTypes;
    protected final IndexManagementType indexManagement;
    protected final NodeStateManager nodeStateManager;
    protected final AnalysisType analysisType;
    protected final NamedXContentRegistry xContentRegistry;
    protected final String configIdFieldName;

    protected volatile Integer maxOldAdTaskDocsPerConfig;

    protected final ThreadPool threadPool;
    private final String allResultIndexPattern;
    private final String batchTaskThreadPoolName;

    public TaskManager(
        TaskCacheManagerType taskCacheManager,
        ClusterService clusterService,
        Client client,
        String stateIndex,
        List<TaskTypeEnum> realTimeTaskTypes,
        IndexManagementType indexManagement,
        NodeStateManager nodeStateManager,
        AnalysisType analysisType,
        NamedXContentRegistry xContentRegistry,
        String configIdFieldName,
        Setting<Integer> maxOldADTaskDocsPerConfig,
        Settings settings,
        ThreadPool threadPool,
        String allResultIndexPattern,
        String batchTaskThreadPoolName
    ) {
        this.taskCacheManager = taskCacheManager;
        this.clusterService = clusterService;
        this.client = client;
        this.stateIndex = stateIndex;
        this.realTimeTaskTypes = realTimeTaskTypes;
        this.indexManagement = indexManagement;
        this.nodeStateManager = nodeStateManager;
        this.analysisType = analysisType;
        this.xContentRegistry = xContentRegistry;
        this.configIdFieldName = configIdFieldName;

        this.maxOldAdTaskDocsPerConfig = maxOldADTaskDocsPerConfig.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(maxOldADTaskDocsPerConfig, it -> maxOldAdTaskDocsPerConfig = it);

        this.threadPool = threadPool;
        this.allResultIndexPattern = allResultIndexPattern;
        this.batchTaskThreadPoolName = batchTaskThreadPoolName;
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

    public <T> void getAndExecuteOnLatestConfigLevelTask(
        Config config,
        DateRange dateRange,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        getAndExecuteOnLatestConfigLevelTask(config.getId(), getTaskTypes(dateRange), (adTask) -> {
            if (!adTask.isPresent() || adTask.get().isDone()) {
                updateLatestFlagOfOldTasksAndCreateNewTask(config, dateRange, user, listener);
            } else {
                listener.onFailure(new OpenSearchStatusException(CONFIG_IS_RUNNING, RestStatus.BAD_REQUEST));
            }
        }, transportService, true, listener);
    }

    public void updateLatestFlagOfOldTasksAndCreateNewTask(
        Config config,
        DateRange dateRange,
        User user,
        ActionListener<JobResponse> listener
    ) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(stateIndex);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(configIdFieldName, config.getId()));
        query.filter(new TermQueryBuilder(TimeSeriesTask.IS_LATEST_FIELD, true));
        // make sure we reset all latest task as false when user switch from single entity to HC, vice versa.
        query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(getTaskTypes(dateRange, true))));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        String script = String.format(Locale.ROOT, "ctx._source.%s=%s;", TimeSeriesTask.IS_LATEST_FIELD, false);
        updateByQueryRequest.setScript(new Script(script));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (bulkFailures.isEmpty()) {
                // Realtime AD coordinating node is chosen by job scheduler, we won't know it until realtime AD job
                // runs. Just set realtime AD coordinating node as null here, and AD job runner will reset correct
                // coordinating node once realtime job starts.
                // For historical analysis, this method will be called on coordinating node, so we can set coordinating
                // node as local node.
                String coordinatingNode = dateRange == null ? null : clusterService.localNode().getId();
                createNewTask(config, dateRange, user, coordinatingNode, listener);
            } else {
                logger.error("Failed to update old task's state for detector: {}, response: {} ", config.getId(), r.toString());
                listener.onFailure(bulkFailures.get(0).getCause());
            }
        }, e -> {
            logger.error("Failed to reset old tasks as not latest for detector " + config.getId(), e);
            listener.onFailure(e);
        }));
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
        TaskState state,
        Exception error,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        getAndExecuteOnLatestConfigLevelTask(configId, getRealTimeTaskTypes(), (adTask) -> {
            if (adTask.isPresent() && !adTask.get().isDone()) {
                Map<String, Object> updatedFields = new HashMap<>();
                updatedFields.put(TimeSeriesTask.STATE_FIELD, state.name());
                if (error != null) {
                    updatedFields.put(TimeSeriesTask.ERROR_FIELD, error.getMessage());
                }
                ExecutorFunction function = () -> updateTask(adTask.get().getTaskId(), updatedFields, ActionListener.wrap(r -> {
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
            } else {
                listener.onFailure(new OpenSearchStatusException("job is already stopped: " + configId, RestStatus.OK));
            }
        }, null, false, listener);
    }

    protected <T> void resetTaskStateAsStopped(
        TimeSeriesTask task,
        ExecutorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        cleanConfigCache(task, transportService, () -> {
            String taskId = task.getTaskId();
            Map<String, Object> updatedFields = ImmutableMap.of(TimeSeriesTask.STATE_FIELD, TaskState.STOPPED.name());
            updateTask(taskId, updatedFields, ActionListener.wrap(r -> {
                task.setState(TaskState.STOPPED.name());
                if (function != null) {
                    function.execute();
                }
                // For realtime anomaly detection, we only create config level task, no entity level realtime task.
                if (isHistoricalHCTask(task)) {
                    // Reset running entity tasks as STOPPED
                    resetEntityTasksAsStopped(taskId);
                }
            }, e -> {
                logger.error("Failed to update task state as STOPPED for task " + taskId, e);
                listener.onFailure(e);
            }));
        }, listener);
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

    /**
     * Get latest config tasks and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param detectorId detector id
     * @param parentTaskId parent task id
     * @param entity entity value
     * @param adTaskTypes AD task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param size return how many AD tasks
     * @param listener action listener
     * @param <T> response type of action listener
     */
    public <T> void getAndExecuteOnLatestTasks(
        String configId,
        String parentTaskId,
        Entity entity,
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

        client.search(searchRequest, ActionListener.wrap(r -> {
            // https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359#discussion_r558653132
            // getTotalHits will be null when we track_total_hits is false in the query request.
            // Add more checking here to cover some unknown cases.
            List<TaskClass> tsTasks = new ArrayList<>();
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                // don't throw exception here as consumer functions need to handle missing task
                // in different way.
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
            if (resetTaskState) {
                resetLatestConfigTaskState(tsTasks, function, transportService, listener);
            } else {
                function.accept(tsTasks);
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(new ArrayList<>());
            } else {
                logger.error("Failed to search task for config " + configId, e);
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Reset latest config task state. Will reset both historical and realtime tasks.
     * [Important!] Make sure listener returns in function
     *
     * @param adTasks ad tasks
     * @param function consumer function
     * @param transportService transport service
     * @param listener action listener
     * @param <T> response type of action listener
     */
    protected <T> void resetLatestConfigTaskState(
        List<TaskClass> adTasks,
        Consumer<List<TaskClass>> function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        List<TimeSeriesTask> runningHistoricalTasks = new ArrayList<>();
        List<TimeSeriesTask> runningRealtimeTasks = new ArrayList<>();
        for (TimeSeriesTask adTask : adTasks) {
            if (!adTask.isEntityTask() && !adTask.isDone()) {
                if (!adTask.isHistoricalTask()) {
                    // try to reset task state if realtime task is not ended
                    runningRealtimeTasks.add(adTask);
                } else {
                    // try to reset task state if historical task not updated for 2 piece intervals
                    runningHistoricalTasks.add(adTask);
                }
            }
        }

        resetHistoricalConfigTaskState(
            runningHistoricalTasks,
            () -> resetRealtimeConfigTaskState(runningRealtimeTasks, () -> function.accept(adTasks), transportService, listener),
            transportService,
            listener
        );
    }

    private <T> void resetRealtimeConfigTaskState(
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
        GetRequest getJobRequest = new GetRequest(CommonName.JOB_INDEX).id(configId);
        client.get(getJobRequest, ActionListener.wrap(r -> {
            if (r.isExists()) {
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job job = Job.parse(parser);
                    if (!job.isEnabled()) {
                        logger.debug("job is disabled, reset realtime task as stopped for config {}", configId);
                        resetTaskStateAsStopped(tsTask, function, transportService, listener);
                    } else {
                        function.execute();
                    }
                } catch (IOException e) {
                    logger.error(" Failed to parse job " + configId, e);
                    listener.onFailure(e);
                }
            } else {
                logger.debug("job is not found, reset realtime task as stopped for config {}", configId);
                resetTaskStateAsStopped(tsTask, function, transportService, listener);
            }
        }, e -> {
            logger.error("Fail to get realtime job for config " + configId, e);
            listener.onFailure(e);
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
            deleteTask(task.getTaskId());
            return;
        }
        if (e instanceof TaskCancelledException) {
            logger.info("task cancelled, taskId: {}, configId: {}", task.getTaskId(), task.getConfigId());
            state = TaskState.STOPPED.name();
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
        updateTask(task.getTaskId(), updatedFields);
    }

    /**
     * Update task with specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     */
    public void updateTask(String taskId, Map<String, Object> updatedFields) {
        updateTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated task successfully: {}, task id: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task: " + taskId, e); }));
    }

    /**
     * Update AD task for specific fields.
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
     * Delete task with task id.
     *
     * @param taskId task id
     */
    public void deleteTask(String taskId) {
        deleteTask(
            taskId,
            ActionListener
                .wrap(
                    r -> { logger.info("Deleted task {} with status: {}", taskId, r.status()); },
                    e -> { logger.error("Failed to delete task " + taskId, e); }
                )
        );
    }

    /**
     * Delete task with task id.
     *
     * @param taskId task id
     * @param listener action listener
     */
    public void deleteTask(String taskId, ActionListener<DeleteResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(stateIndex, taskId);
        client.delete(deleteRequest, listener);
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
            client.index(request, ActionListener.wrap(r -> function.accept(r), e -> {
                logger.error("Failed to create task for config " + tsTask.getConfigId(), e);
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            logger.error("Failed to create task for config " + tsTask.getConfigId(), e);
            listener.onFailure(e);
        }
    }

    protected void cleanOldConfigTaskDocs(IndexResponse response, TaskClass tsTask, ActionListener<JobResponse> delegatedListener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(configIdFieldName, tsTask.getConfigId()));
        query.filter(new TermQueryBuilder(TimeSeriesTask.IS_LATEST_FIELD, false));

        if (tsTask.isHistoricalTask()) {
            // If historical task, only delete detector level task. It may take longer time to delete entity tasks.
            // We will delete child task (entity task) of detector level task in hourly cron job.
            query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(HISTORICAL_DETECTOR_TASK_TYPES)));
        } else {
            // We don't have entity level task for realtime detection, so will delete all tasks.
            query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(REALTIME_TASK_TYPES)));
        }

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
            .query(query)
            .sort(TimeSeriesTask.EXECUTION_START_TIME_FIELD, SortOrder.DESC)
            // Search query "from" starts from 0.
            .from(maxOldAdTaskDocsPerConfig)
            .size(MAX_OLD_AD_TASK_DOCS);
        searchRequest.source(sourceBuilder).indices(DETECTION_STATE_INDEX);
        String detectorId = tsTask.getConfigId();

        deleteTaskDocs(detectorId, searchRequest, () -> {
            if (tsTask.isHistoricalTask()) {
                // run batch result action for historical detection
                runBatchResultAction(response, tsTask, delegatedListener);
            } else {
                // return response directly for realtime detection
                JobResponse anomalyDetectorJobResponse = new JobResponse(response.getId());
                delegatedListener.onResponse(anomalyDetectorJobResponse);
            }
        }, delegatedListener);
    }

    protected <T> void deleteTaskDocs(
        String detectorId,
        SearchRequest searchRequest,
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
                        ADTask adTask = ADTask.parse(parser, searchHit.getId());
                        logger.debug("Delete old task: {} of detector: {}", adTask.getTaskId(), adTask.getConfigId());
                        bulkRequest.add(new DeleteRequest(DETECTION_STATE_INDEX).id(adTask.getTaskId()));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
                client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.wrap(res -> {
                    logger.info("Old AD tasks deleted for detector {}", detectorId);
                    BulkItemResponse[] bulkItemResponses = res.getItems();
                    if (bulkItemResponses != null && bulkItemResponses.length > 0) {
                        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                            if (!bulkItemResponse.isFailed()) {
                                logger.debug("Add detector task into cache. Task id: {}", bulkItemResponse.getId());
                                // add deleted task in cache and delete its child tasks and AD results
                                taskCacheManager.addDeletedTask(bulkItemResponse.getId());
                            }
                        }
                    }
                    // delete child tasks and results of this task
                    cleanChildTasksAndADResultsOfDeletedTask();

                    function.execute();
                }, e -> {
                    logger.warn("Failed to clean tasks for config " + detectorId, e);
                    listener.onFailure(e);
                }));
            } else {
                function.execute();
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.execute();
            } else {
                listener.onFailure(e);
            }
        });

        client.search(searchRequest, searchListener);
    }

    /**
     * Poll deleted detector task from cache and delete its child tasks and AD results.
     */
    public void cleanChildTasksAndADResultsOfDeletedTask() {
        if (!taskCacheManager.hasDeletedTask()) {
            return;
        }
        threadPool.schedule(() -> {
            String taskId = taskCacheManager.pollDeletedTask();
            if (taskId == null) {
                return;
            }
            DeleteByQueryRequest deleteADResultsRequest = new DeleteByQueryRequest(allResultIndexPattern);
            deleteADResultsRequest.setQuery(new TermsQueryBuilder(CommonName.TASK_ID_FIELD, taskId));
            client.execute(DeleteByQueryAction.INSTANCE, deleteADResultsRequest, ActionListener.wrap(res -> {
                logger.debug("Successfully deleted results of task " + taskId);
                DeleteByQueryRequest deleteChildTasksRequest = new DeleteByQueryRequest(DETECTION_STATE_INDEX);
                deleteChildTasksRequest.setQuery(new TermsQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, taskId));

                client.execute(DeleteByQueryAction.INSTANCE, deleteChildTasksRequest, ActionListener.wrap(r -> {
                    logger.debug("Successfully deleted child tasks of task " + taskId);
                    cleanChildTasksAndADResultsOfDeletedTask();
                }, e -> { logger.error("Failed to delete child tasks of task " + taskId, e); }));
            }, ex -> { logger.error("Failed to delete results for task " + taskId, ex); }));
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), batchTaskThreadPoolName);
    }

    protected void resetEntityTasksAsStopped(String configTaskId) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(stateIndex);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, configTaskId));
        query.filter(new TermQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, ADTaskType.AD_HISTORICAL_HC_ENTITY.name()));
        query.filter(new TermsQueryBuilder(TimeSeriesTask.STATE_FIELD, NOT_ENDED_STATES));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        String script = String.format(Locale.ROOT, "ctx._source.%s='%s';", TimeSeriesTask.STATE_FIELD, TaskState.STOPPED.name());
        updateByQueryRequest.setScript(new Script(script));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (ParseUtils.isNullOrEmpty(bulkFailures)) {
                logger.debug("Updated {} child entity tasks state for config task {}", r.getUpdated(), configTaskId);
            } else {
                logger.error("Failed to update child entity task's state for config task {} ", configTaskId);
            }
        }, e -> logger.error("Exception happened when update child entity task's state for config task " + configTaskId, e)));
    }

    public abstract void startHistorical(
        Config config,
        DateRange dateRange,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    );

    protected abstract List<TaskTypeEnum> getTaskTypes(DateRange dateRange, boolean resetLatestTaskStateFlag);

    protected abstract TaskType getTaskType(Config config, DateRange dateRange);

    protected abstract void createNewTask(
        Config config,
        DateRange dateRange,
        User user,
        String coordinatingNode,
        ActionListener<JobResponse> listener
    );

    protected abstract List<TaskTypeEnum> getRealTimeTaskTypes();

    public abstract <T> void cleanConfigCache(
        TimeSeriesTask task,
        TransportService transportService,
        ExecutorFunction function,
        ActionListener<T> listener
    );

    protected abstract boolean isHistoricalHCTask(TimeSeriesTask task);

    public abstract void stopHistoricalAnalysis(
        String detectorId,
        Optional<TaskClass> adTask,
        User user,
        ActionListener<JobResponse> listener
    );

    protected abstract <T> void resetHistoricalConfigTaskState(
        List<TimeSeriesTask> runningHistoricalTasks,
        ExecutorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    );

    protected abstract void onIndexConfigTaskResponse(
        IndexResponse response,
        TaskClass adTask,
        BiConsumer<IndexResponse, ActionListener<JobResponse>> function,
        ActionListener<JobResponse> listener
    );

    protected abstract void runBatchResultAction(IndexResponse response, TaskClass tsTask, ActionListener<JobResponse> listener);

    protected abstract BiCheckedFunction<XContentParser, String, TaskClass, IOException> getTaskParser();
}
