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

package org.opensearch.forecast.task;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.forecast.constant.ForecastCommonMessages.FORECASTER_IS_RUNNING;
import static org.opensearch.forecast.indices.ForecastIndexManagement.ALL_FORECAST_RESULTS_INDEX_PATTERN;
import static org.opensearch.forecast.model.ForecastTask.EXECUTION_START_TIME_FIELD;
import static org.opensearch.forecast.model.ForecastTask.FORECASTER_ID_FIELD;
import static org.opensearch.forecast.model.ForecastTask.IS_LATEST_FIELD;
import static org.opensearch.forecast.model.ForecastTask.PARENT_TASK_ID_FIELD;
import static org.opensearch.forecast.model.ForecastTask.TASK_TYPE_FIELD;
import static org.opensearch.forecast.model.ForecastTaskType.REALTIME_TASK_TYPES;
import static org.opensearch.forecast.settings.ForecastSettings.DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_OLD_TASK_DOCS_PER_FORECASTER;
import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME;
import static org.opensearch.timeseries.model.TimeSeriesTask.TASK_ID_FIELD;
import static org.opensearch.timeseries.util.RestHandlerUtils.createXContentParserFromRegistry;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.common.exception.DuplicateTaskException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.transport.TransportService;

public class ForecastTaskManager extends
    TaskManager<TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement> {
    private final Logger logger = LogManager.getLogger(ForecastTaskManager.class);

    public ForecastTaskManager(
        TaskCacheManager forecastTaskCacheManager,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ForecastIndexManagement forecastIndices,
        ClusterService clusterService,
        Settings settings,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager
    ) {
        super(
            forecastTaskCacheManager,
            clusterService,
            client,
            ForecastIndex.STATE.getIndexName(),
            ForecastTaskType.REALTIME_TASK_TYPES,
            forecastIndices,
            nodeStateManager,
            AnalysisType.FORECAST,
            xContentRegistry,
            FORECASTER_ID_FIELD,
            MAX_OLD_TASK_DOCS_PER_FORECASTER,
            settings,
            threadPool,
            ALL_FORECAST_RESULTS_INDEX_PATTERN,
            FORECAST_THREAD_POOL_NAME,
            DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER
        );
    }

    /**
     * Init realtime task cache Realtime forecast depending on job scheduler to choose node (job coordinating node)
     * to run forecast job. Nodes have primary or replica shard of the job index are candidate to run forecast job.
     * Job scheduler will build hash ring on these candidate nodes and choose one to run forecast job.
     * If forecast job index shard relocated, for example new node added into cluster, then job scheduler will
     * rebuild hash ring and may choose different node to run forecast job. So we need to init realtime task cache
     * on new forecast job coordinating node.
     *
     * If realtime task cache inited for the first time on this node, listener will return true; otherwise
     * listener will return false.
     *
     * We don't clean up realtime task cache on old coordinating node as HourlyCron will clear cache on old coordinating node.
     *
     * @param forecasterId forecaster id
     * @param forecaster forecaster
     * @param transportService transport service
     * @param listener listener
     */
    @Override
    public void initCacheWithCleanupIfRequired(
        String forecasterId,
        Config forecaster,
        TransportService transportService,
        ActionListener<Boolean> listener
    ) {
        try {
            if (taskCacheManager.getRealtimeTaskCache(forecasterId) != null) {
                listener.onResponse(false);
                return;
            }

            getAndExecuteOnLatestForecasterLevelTask(forecasterId, REALTIME_TASK_TYPES, (forecastTaskOptional) -> {
                if (forecastTaskOptional.isEmpty()) {
                    logger.debug("Can't find realtime task for forecaster {}, init realtime task cache directly", forecasterId);
                    ExecutorFunction function = () -> createNewTask(
                        forecaster,
                        null,
                        forecaster.getUser(),
                        clusterService.localNode().getId(),
                        ActionListener.wrap(r -> {
                            logger.info("Recreate realtime task successfully for forecaster {}", forecasterId);
                            taskCacheManager.initRealtimeTaskCache(forecasterId, forecaster.getIntervalInMilliseconds());
                            listener.onResponse(true);
                        }, e -> {
                            logger.error("Failed to recreate realtime task for forecaster " + forecasterId, e);
                            listener.onFailure(e);
                        })
                    );
                    recreateRealtimeTaskBeforeExecuting(function, listener);
                    return;
                }

                logger.info("Init realtime task cache for forecaster {}", forecasterId);
                taskCacheManager.initRealtimeTaskCache(forecasterId, forecaster.getIntervalInMilliseconds());
                listener.onResponse(true);
            }, transportService, false, listener);
        } catch (Exception e) {
            logger.error("Failed to init realtime task cache for " + forecasterId, e);
            listener.onFailure(e);
        }
    }

    @Override
    protected <T> void deleteTaskDocs(
        String forecasterId,
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
                        ForecastTask forecastTask = ForecastTask.parse(parser, searchHit.getId());
                        logger.debug("Delete old task: {} of forecaster: {}", forecastTask.getTaskId(), forecastTask.getConfigId());
                        bulkRequest.add(new DeleteRequest(ForecastIndex.STATE.getIndexName()).id(forecastTask.getTaskId()));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
                client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.wrap(res -> {
                    logger.info("Old forecast tasks deleted for forecaster {}", forecasterId);
                    BulkItemResponse[] bulkItemResponses = res.getItems();
                    if (bulkItemResponses != null && bulkItemResponses.length > 0) {
                        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                            if (!bulkItemResponse.isFailed()) {
                                logger.debug("Add detector task into cache. Task id: {}", bulkItemResponse.getId());
                                // add deleted task in cache and delete its child tasks and forecast results
                                taskCacheManager.addDeletedTask(bulkItemResponse.getId());
                            }
                        }
                    }
                    // delete child tasks and forecast results of this task
                    cleanChildTasksAndResultsOfDeletedTask();

                    function.execute();
                }, e -> {
                    logger.warn("Failed to clean forecast tasks for forecaster " + forecasterId, e);
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
     * Update forecast task with specific fields.
     *
     * @param taskId forecast task id
     * @param updatedFields updated fields, key: filed name, value: new value
     */
    public void updateForecastTask(String taskId, Map<String, Object> updatedFields) {
        updateForecastTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated forecast task successfully: {}, task id: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update forecast task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task: " + taskId, e); }));
    }

    /**
     * Update forecast task for specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateForecastTask(String taskId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(ForecastIndex.STATE.getIndexName(), taskId);
        Map<String, Object> updatedContent = new HashMap<>();
        updatedContent.putAll(updatedFields);
        updatedContent.put(TimeSeriesTask.LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
        updateRequest.doc(updatedContent);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.update(updateRequest, listener);
    }

    /**
     * Get latest forecast task and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param forecasterId detector id
     * @param forecastTaskTypes forecast task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAndExecuteOnLatestForecasterLevelTask(
        String forecasterId,
        List<ForecastTaskType> forecastTaskTypes,
        Consumer<Optional<ForecastTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestForecastTask(
            forecasterId,
            null,
            null,
            forecastTaskTypes,
            function,
            transportService,
            resetTaskState,
            listener
        );
    }

    /**
     * Get one latest forecast task and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param forecasterId forecaster id
     * @param parentTaskId parent task id
     * @param entity entity value
     * @param forecastTaskTypes forecast task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAndExecuteOnLatestForecastTask(
        String forecasterId,
        String parentTaskId,
        Entity entity,
        List<ForecastTaskType> forecastTaskTypes,
        Consumer<Optional<ForecastTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestForecastTasks(forecasterId, parentTaskId, entity, forecastTaskTypes, (taskList) -> {
            if (taskList != null && taskList.size() > 0) {
                function.accept(Optional.ofNullable(taskList.get(0)));
            } else {
                function.accept(Optional.empty());
            }
        }, transportService, resetTaskState, 1, listener);
    }

    /**
     * Get latest forecast tasks and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param forecasterId forecaster id
     * @param parentTaskId parent task id
     * @param entity entity value
     * @param forecastTaskTypes forecast task types
     * @param function consumer function
     * @param transportService transport service
     * @param size return how many AD tasks
     * @param listener action listener
     * @param <T> response type of action listener
     */
    public <T> void getAndExecuteOnLatestForecastTasks(
        String forecasterId,
        String parentTaskId,
        Entity entity,
        List<ForecastTaskType> forecastTaskTypes,
        Consumer<List<ForecastTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        int size,
        ActionListener<T> listener
    ) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(FORECASTER_ID_FIELD, forecasterId));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        if (parentTaskId != null) {
            query.filter(new TermQueryBuilder(PARENT_TASK_ID_FIELD, parentTaskId));
        }
        if (forecastTaskTypes != null && forecastTaskTypes.size() > 0) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, TaskType.taskTypeToString(forecastTaskTypes)));
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
        sourceBuilder.query(query).sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC).size(size);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(ForecastIndex.STATE.getIndexName());

        client.search(searchRequest, ActionListener.wrap(r -> {
            // https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359#discussion_r558653132
            // getTotalHits will be null when we track_total_hits is false in the query request.
            // Add more checking here to cover some unknown cases.
            List<ForecastTask> forecastTasks = new ArrayList<>();
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                // don't throw exception here as consumer functions need to handle missing task
                // in different way.
                function.accept(forecastTasks);
                return;
            }

            Iterator<SearchHit> iterator = r.getHits().iterator();
            while (iterator.hasNext()) {
                SearchHit searchHit = iterator.next();
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ForecastTask forecastTask = ForecastTask.parse(parser, searchHit.getId());
                    forecastTasks.add(forecastTask);
                } catch (Exception e) {
                    String message = "Failed to parse forecast task for forecaster " + forecasterId + ", task id " + searchHit.getId();
                    logger.error(message, e);
                    listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }

            if (resetTaskState) {
                resetLatestForecasterTaskState(forecastTasks, function, transportService, listener);
            } else {
                function.accept(forecastTasks);
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(new ArrayList<>());
            } else {
                logger.error("Failed to search forecast task for forecaster " + forecasterId, e);
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Reset latest forecaster task state. Will reset both historical and realtime tasks.
     * [Important!] Make sure listener returns in function
     *
     * @param forecastTasks ad tasks
     * @param function consumer function
     * @param transportService transport service
     * @param listener action listener
     * @param <T> response type of action listener
     */
    private <T> void resetLatestForecasterTaskState(
        List<ForecastTask> forecastTasks,
        Consumer<List<ForecastTask>> function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        List<ForecastTask> runningHistoricalTasks = new ArrayList<>();
        List<ForecastTask> runningRealtimeTasks = new ArrayList<>();
        for (ForecastTask forecastTask : forecastTasks) {
            if (!forecastTask.isEntityTask() && !forecastTask.isDone()) {
                if (!forecastTask.isHistoricalTask()) {
                    // try to reset task state if realtime task is not ended
                    runningRealtimeTasks.add(forecastTask);
                } else {
                    // try to reset task state if historical task not updated for 2 piece intervals
                    runningHistoricalTasks.add(forecastTask);
                }
            }
        }

        // TODO: reset real time and historical tasks
    }

    private void recreateRealtimeTaskBeforeExecuting(ExecutorFunction function, ActionListener<Boolean> listener) {
        if (indexManagement.doesStateIndexExist()) {
            function.execute();
        } else {
            // If forecast state index doesn't exist, create index and execute function.
            indexManagement.initStateIndex(ActionListener.wrap(r -> {
                if (r.isAcknowledged()) {
                    logger.info("Created {} with mappings.", ForecastIndex.STATE.getIndexName());
                    function.execute();
                } else {
                    String error = String
                        .format(Locale.ROOT, CommonMessages.CREATE_INDEX_NOT_ACKNOWLEDGED, ForecastIndex.STATE.getIndexName());
                    logger.warn(error);
                    listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }, e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                    function.execute();
                } else {
                    logger.error("Failed to init anomaly detection state index", e);
                    listener.onFailure(e);
                }
            }));
        }
    }

    /**
     * Poll deleted detector task from cache and delete its child tasks and AD results.
     */
    @Override
    public void cleanChildTasksAndResultsOfDeletedTask() {
        if (!taskCacheManager.hasDeletedTask()) {
            return;
        }
        threadPool.schedule(() -> {
            String taskId = taskCacheManager.pollDeletedTask();
            if (taskId == null) {
                return;
            }
            DeleteByQueryRequest deleteForecastResultsRequest = new DeleteByQueryRequest(ALL_FORECAST_RESULTS_INDEX_PATTERN);
            deleteForecastResultsRequest.setQuery(new TermsQueryBuilder(TASK_ID_FIELD, taskId));
            client.execute(DeleteByQueryAction.INSTANCE, deleteForecastResultsRequest, ActionListener.wrap(res -> {
                logger.debug("Successfully deleted forecast results of task " + taskId);
                DeleteByQueryRequest deleteChildTasksRequest = new DeleteByQueryRequest(ForecastIndex.STATE.getIndexName());
                deleteChildTasksRequest.setQuery(new TermsQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, taskId));

                client.execute(DeleteByQueryAction.INSTANCE, deleteChildTasksRequest, ActionListener.wrap(r -> {
                    logger.debug("Successfully deleted child tasks of task " + taskId);
                    cleanChildTasksAndResultsOfDeletedTask();
                }, e -> { logger.error("Failed to delete child tasks of task " + taskId, e); }));
            }, ex -> { logger.error("Failed to delete forecast results for task " + taskId, ex); }));
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), AD_BATCH_TASK_THREAD_POOL_NAME);
    }

    @Override
    public void startHistorical(
        Config config,
        DateRange dateRange,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        // TODO Auto-generated method stub

    }

    @Override
    protected List<ForecastTaskType> getTaskTypes(DateRange dateRange, boolean resetLatestTaskStateFlag) {
        if (dateRange == null) {
            return ForecastTaskType.REALTIME_TASK_TYPES;
        } else if (resetLatestTaskStateFlag) {
            return ForecastTaskType.ALL_HISTORICAL_TASK_TYPES;
        } else {
            return ForecastTaskType.HISTORICAL_FORECASTER_TASK_TYPES;
        }
    }

    @Override
    protected TaskType getTaskType(Config config, DateRange dateRange) {
        if (dateRange == null) {
            return config.isHighCardinality()
                ? ForecastTaskType.FORECAST_REALTIME_HC_FORECASTER
                : ForecastTaskType.FORECAST_REALTIME_SINGLE_STREAM;
        } else {
            return config.isHighCardinality()
                ? ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER
                : ForecastTaskType.FORECAST_HISTORICAL_SINGLE_STREAM;
        }
    }

    @Override
    protected void createNewTask(
        Config config,
        DateRange dateRange,
        User user,
        String coordinatingNode,
        ActionListener<JobResponse> listener
    ) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        String taskType = getTaskType(config, dateRange).name();
        ForecastTask forecastTask = new ForecastTask.Builder()
            .configId(config.getId())
            .forecaster((Forecaster) config)
            .isLatest(true)
            .taskType(taskType)
            .executionStartTime(now)
            .taskProgress(0.0f)
            .initProgress(0.0f)
            .state(TaskState.CREATED.name())
            .lastUpdateTime(now)
            .startedBy(userName)
            .coordinatingNode(coordinatingNode)
            .dateRange(dateRange)
            .user(user)
            .build();

        createTaskDirectly(
            forecastTask,
            r -> onIndexConfigTaskResponse(
                r,
                forecastTask,
                (response, delegatedListener) -> cleanOldConfigTaskDocs(response, forecastTask, delegatedListener),
                listener
            ),
            listener
        );

    }

    @Override
    public <T> void cleanConfigCache(
        TimeSeriesTask task,
        TransportService transportService,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        // no op for forecaster as we rely on state ttl to auto clean it
        listener.onResponse(null);
    }

    @Override
    protected boolean isHistoricalHCTask(TimeSeriesTask task) {
        return ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER.name().equals(task.getTaskType());
    }

    @Override
    public void stopHistoricalAnalysis(String detectorId, Optional<ForecastTask> adTask, User user, ActionListener<JobResponse> listener) {
        // TODO Auto-generated method stub

    }

    @Override
    protected <T> void resetHistoricalConfigTaskState(
        List<TimeSeriesTask> runningHistoricalTasks,
        ExecutorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        // TODO complete reset historical part; now only execute function
        function.execute();
    }

    @Override
    protected void onIndexConfigTaskResponse(
        IndexResponse response,
        ForecastTask forecastTask,
        BiConsumer<IndexResponse, ActionListener<JobResponse>> function,
        ActionListener<JobResponse> listener
    ) {
        if (response == null || response.getResult() != CREATED) {
            String errorMsg = ExceptionUtil.getShardsFailure(response);
            listener.onFailure(new OpenSearchStatusException(errorMsg, response.status()));
            return;
        }
        forecastTask.setTaskId(response.getId());
        ActionListener<JobResponse> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            handleTaskException(forecastTask, e);
            if (e instanceof DuplicateTaskException) {
                listener.onFailure(new OpenSearchStatusException(FORECASTER_IS_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                // TODO: For historical forecast task, what to do if any other exception happened?
                // For realtime forecast, task cache will be inited when realtime job starts, check
                // ForecastTaskManager#initRealtimeTaskCache for details. Here the
                // realtime task cache not inited yet when create AD task, so no need to cleanup.
                listener.onFailure(e);
            }
        });
        // TODO: what to do if this is a historical task?
        if (function != null) {
            function.accept(response, delegatedListener);
        }
    }

    @Override
    protected void runBatchResultAction(IndexResponse response, ForecastTask tsTask, ActionListener<JobResponse> listener) {
        // TODO Auto-generated method stub

    }

    @Override
    protected BiCheckedFunction<XContentParser, String, ForecastTask, IOException> getTaskParser() {
        return ForecastTask::parse;
    }
}
