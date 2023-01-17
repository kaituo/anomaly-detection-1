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

import static org.opensearch.forecast.model.ForecastTask.FORECASTER_ID_FIELD;
import static org.opensearch.forecast.model.ForecastTask.EXECUTION_START_TIME_FIELD;
import static org.opensearch.forecast.model.ForecastTask.IS_LATEST_FIELD;
import static org.opensearch.forecast.model.ForecastTask.PARENT_TASK_ID_FIELD;
import static org.opensearch.forecast.model.ForecastTask.TASK_TYPE_FIELD;
import static org.opensearch.forecast.model.ForecastTaskType.REALTIME_TASK_TYPES;
import static org.opensearch.forecast.model.ForecastTaskType.taskTypeToString;
import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.forecast.constant.ForecastCommonMessages.FORECASTER_IS_RUNNING;
import static org.opensearch.forecast.indices.ForecastIndices.ALL_FORECAST_RESULTS_INDEX_PATTERN;
import static org.opensearch.forecast.model.ForecastTaskType.HISTORICAL_FORECASTER_TASK_TYPES;
import static org.opensearch.timeseries.model.TimeSeriesTask.TASK_ID_FIELD;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_OLD_FORECAST_TASK_DOCS;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_OLD_TASK_DOCS_PER_FORECASTER;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static org.opensearch.timeseries.util.RestHandlerUtils.createXContentParserFromRegistry;

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
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.forecast.indices.ForecastIndices;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.common.exception.TaskCancelledException;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.transport.ForecastJobResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.DuplicateTaskException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.TimeSeriesFunction;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

public class ForecastTaskManager {
    private final Logger logger = LogManager.getLogger(ForecastTaskManager.class);

    private static int DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS = 5;

    private final ForecastTaskCacheManager forecastTaskCacheManager;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final ForecastIndices forecastIndices;
    private final ClusterService clusterService;
    private volatile Integer maxOldTaskDocsPerForecaster;
    private final ThreadPool threadPool;

    public ForecastTaskManager(ForecastTaskCacheManager forecastTaskCacheManager,
            Client client,
            NamedXContentRegistry xContentRegistry,
            ForecastIndices forecastIndices,
            ClusterService clusterService,
            Settings settings,
            ThreadPool threadPool) {
        this.forecastTaskCacheManager = forecastTaskCacheManager;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.forecastIndices = forecastIndices;
        this.clusterService = clusterService;

        this.maxOldTaskDocsPerForecaster = MAX_OLD_TASK_DOCS_PER_FORECASTER.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_OLD_TASK_DOCS_PER_FORECASTER, it -> maxOldTaskDocsPerForecaster = it);

        this.threadPool = threadPool;
    }

    /**
     * Init realtime task cache and clean up realtime task cache on old coordinating node. Realtime forecast
     * depends on job scheduler to choose node (job coordinating node) to run forecast job. Nodes have primary
     * or replica shard of the job index are candidate to run forecast job. Job scheduler will build hash ring
     * on these candidate nodes and choose one to run forecast job. If forecast job index shard relocated, for example
     * new node added into cluster, then job scheduler will rebuild hash ring and may choose different
     * node to run forecast job. So we need to init realtime task cache on new forecast job coordinating node.
     * HourlyCron will clear cache on old coordinating node.
     *
     * If realtime task cache inited for the first time on this node, listener will return true; otherwise
     * listener will return false.
     *
     * @param forecasterId forecaster id
     * @param forecaster forecaster
     * @param transportService transport service
     * @param listener listener
     */
    public void initRealtimeTaskCache(
        String forecasterId,
        Forecaster forecaster,
        TransportService transportService,
        ActionListener<Boolean> listener
    ) {
        try {
            if (forecastTaskCacheManager.getRealtimeTaskCache(forecasterId) != null) {
                listener.onResponse(false);
                return;
            }

            getAndExecuteOnLatestForecasterLevelTask(forecasterId, REALTIME_TASK_TYPES, (forecastTaskOptional) -> {
                if (!forecastTaskOptional.isPresent()) {
                    logger.debug("Can't find realtime task for forecaster {}, init realtime task cache directly", forecasterId);
                    TimeSeriesFunction function = () -> createNewForecasterTask(
                            forecaster,
                        null,
                        forecaster.getUser(),
                        clusterService.localNode().getId(),
                        ActionListener.wrap(r -> {
                            logger.info("Recreate realtime task successfully for forecaster {}", forecasterId);
                            forecastTaskCacheManager.initRealtimeTaskCache(forecasterId, forecaster.getIntervalInMilliseconds());
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
                    forecastTaskCacheManager.initRealtimeTaskCache(forecasterId, forecaster.getIntervalInMilliseconds());
                    listener.onResponse(true);
            }, transportService, false, listener);
        } catch (Exception e) {
            logger.error("Failed to init realtime task cache for " + forecasterId, e);
            listener.onFailure(e);
        }
    }

    private void createNewForecasterTask(
            Forecaster forecaster,
            DateRange dateRange,
            User user,
            String coordinatingNode,
            ActionListener<ForecastJobResponse> listener
        ) {
            String userName = user == null ? null : user.getName();
            Instant now = Instant.now();
            String taskType = getForecasterTaskType(forecaster, dateRange).name();
            ForecastTask forecastTask = new ForecastTask.Builder()
                .forecasterId(forecaster.getId())
                .forecaster(forecaster)
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

            createForecastTaskDirectly(
                forecastTask,
                r -> onIndexForecastTaskResponse(
                    r,
                    forecastTask,
                    (response, delegatedListener) -> cleanOldForecastTaskDocs(response, forecastTask, delegatedListener),
                    listener
                ),
                listener
            );
        }

    private void cleanOldForecastTaskDocs(IndexResponse response, ForecastTask forecastTask, ActionListener<ForecastJobResponse> delegatedListener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(FORECASTER_ID_FIELD, forecastTask.getForecasterId()));
        query.filter(new TermQueryBuilder(TimeSeriesTask.IS_LATEST_FIELD, false));

        if (forecastTask.isHistoricalTask()) {
            // If historical task, only delete detector level task. It may take longer time to delete entity tasks.
            // We will delete child task (entity task) of detector level task in hourly cron job.
            query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(HISTORICAL_FORECASTER_TASK_TYPES)));
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
            .from(maxOldTaskDocsPerForecaster)
            .size(MAX_OLD_FORECAST_TASK_DOCS);
        searchRequest.source(sourceBuilder).indices(ForecastIndex.STATE.getIndexName());
        String detectorId = forecastTask.getForecasterId();

        deleteTaskDocs(detectorId, searchRequest, () -> {
            if (forecastTask.isHistoricalTask()) {
                // TODO: run batch result action for historical forecasting

            } else {
                // return response directly for realtime detection
                ForecastJobResponse forecasterJobResponse = new ForecastJobResponse(
                    response.getId(),
                    response.getVersion(),
                    response.getSeqNo(),
                    response.getPrimaryTerm(),
                    RestStatus.OK
                );
                delegatedListener.onResponse(forecasterJobResponse);
            }
        }, delegatedListener);
    }

    protected <T> void deleteTaskDocs(
            String forecasterId,
            SearchRequest searchRequest,
            TimeSeriesFunction function,
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
                            logger.debug("Delete old task: {} of forecaster: {}", forecastTask.getTaskId(), forecastTask.getForecasterId());
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
                                    // add deleted task in cache and delete its child tasks and AD results
                                    forecastTaskCacheManager.addDeletedDetectorTask(bulkItemResponse.getId());
                                }
                            }
                        }
                        // delete child tasks and AD results of this task
                        cleanChildTasksAndResultsOfDeletedTask();

                        function.execute();
                    }, e -> {
                        logger.warn("Failed to clean AD tasks for detector " + forecasterId, e);
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

    private void onIndexForecastTaskResponse(
            IndexResponse response,
            ForecastTask forecastTask,
            BiConsumer<IndexResponse, ActionListener<ForecastJobResponse>> function,
            ActionListener<ForecastJobResponse> listener
        ) {
            if (response == null || response.getResult() != CREATED) {
                String errorMsg = ExceptionUtil.getShardsFailure(response);
                listener.onFailure(new OpenSearchStatusException(errorMsg, response.status()));
                return;
            }
            forecastTask.setTaskId(response.getId());
            ActionListener<ForecastJobResponse> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
                handleForecastTaskException(forecastTask, e);
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

    /**
     * Handle exceptions for forecast task. Update task state and record error message.
     *
     * @param forecastTask forecast task
     * @param e exception
     */
    public void handleForecastTaskException(ForecastTask forecastTask, Exception e) {
        // TODO: handle timeout exception
        String state = TaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (e instanceof DuplicateTaskException) {
            // If user send multiple start detector request, we will meet race condition.
            // Cache manager will put first request in cache and throw DuplicateTaskException
            // for the second request. We will delete the second task.
            logger
                .warn(
                    "There is already one running task for forecaster, forecasterId:"
                        + forecastTask.getForecasterId()
                        + ". Will delete task "
                        + forecastTask.getTaskId()
                );
            deleteForecastTask(forecastTask.getTaskId());
            return;
        }
        if (e instanceof TaskCancelledException) {
            logger.info("Forecast task cancelled, taskId: {}, forecasterId: {}", forecastTask.getTaskId(), forecastTask.getForecasterId());
            state = TaskState.STOPPED.name();
            String stoppedBy = ((TaskCancelledException) e).getCancelledBy();
            if (stoppedBy != null) {
                updatedFields.put(TimeSeriesTask.STOPPED_BY_FIELD, stoppedBy);
            }
        } else {
            logger.error("Failed to execute forecast batch task, task id: " + forecastTask.getTaskId() + ", forecaster id: " + forecastTask.getForecasterId(), e);
        }
        updatedFields.put(TimeSeriesTask.ERROR_FIELD, ExceptionUtil.getErrorMessage(e));
        updatedFields.put(TimeSeriesTask.STATE_FIELD, state);
        updatedFields.put(TimeSeriesTask.EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
        updateForecastTask(forecastTask.getTaskId(), updatedFields);
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
     * Delete forecast task with task id.
     *
     * @param taskId forecast task id
     */
    public void deleteForecastTask(String taskId) {
        deleteForecastTask(
            taskId,
            ActionListener
                .wrap(
                    r -> { logger.info("Deleted forecast task {} with status: {}", taskId, r.status()); },
                    e -> { logger.error("Failed to delete forecast task " + taskId, e); }
                )
        );
    }

    /**
     * Delete forecast task with task id.
     *
     * @param taskId forecast task id
     * @param listener action listener
     */
    public void deleteForecastTask(String taskId, ActionListener<DeleteResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(ForecastIndex.STATE.getIndexName(), taskId);
        client.delete(deleteRequest, listener);
    }

    /**
     * Create forecast task directly without checking index exists of not.
     * [Important!] Make sure listener returns in function
     *
     * @param forecastTask forecast task
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void createForecastTaskDirectly(ForecastTask forecastTask, Consumer<IndexResponse> function, ActionListener<T> listener) {
        IndexRequest request = new IndexRequest(ForecastIndex.STATE.getIndexName());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                .source(forecastTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client.index(request, ActionListener.wrap(r -> function.accept(r), e -> {
                logger.error("Failed to create forecast task for forecaster " + forecastTask.getForecasterId(), e);
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            logger.error("Failed to create forecast task for forecaster " + forecastTask.getForecasterId(), e);
            listener.onFailure(e);
        }
    }

    private ForecastTaskType getForecasterTaskType(Forecaster forecaster, DateRange dateRange) {
        if (dateRange == null) {
            return forecaster.isHC() ? ForecastTaskType.REALTIME_HC_DETECTOR : ForecastTaskType.REALTIME_SINGLE_STREAM;
        } else {
            return forecaster.isHC() ? ForecastTaskType.HISTORICAL_HC_FORECASTER : ForecastTaskType.HISTORICAL_SINGLE_STREAM;
        }
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
        getAndExecuteOnLatestForecastTask(forecasterId, null, null, forecastTaskTypes, function, transportService, resetTaskState, listener);
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
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(forecastTaskTypes)));
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

    private void recreateRealtimeTaskBeforeExecuting(TimeSeriesFunction function, ActionListener<Boolean> listener) {
        if (forecastIndices.doesForecasterStateIndexExist()) {
            function.execute();
        } else {
            // If forecast state index doesn't exist, create index and execute function.
            forecastIndices.initForecastStateIndex(ActionListener.wrap(r -> {
                if (r.isAcknowledged()) {
                    logger.info("Created {} with mappings.", ForecastIndex.STATE.getIndexName());
                    function.execute();
                } else {
                    String error = String.format(Locale.ROOT, CommonMessages.CREATE_INDEX_NOT_ACKNOWLEDGED, ForecastIndex.STATE.getIndexName());
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
    public void cleanChildTasksAndResultsOfDeletedTask() {
        if (!forecastTaskCacheManager.hasDeletedDetectorTask()) {
            return;
        }
        threadPool.schedule(() -> {
            String taskId = forecastTaskCacheManager.pollDeletedDetectorTask();
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
}
