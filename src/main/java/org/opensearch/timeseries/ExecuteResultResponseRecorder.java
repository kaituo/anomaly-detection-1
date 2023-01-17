/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.ad.transport.ProfileAction;
import org.opensearch.ad.transport.ProfileRequest;
import org.opensearch.ad.transport.RCFPollingAction;
import org.opensearch.ad.transport.RCFPollingRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.search.SearchHits;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.transport.handler.ResultIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.ExceptionUtil;

public abstract class ExecuteResultResponseRecorder<IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass>> {

    private static final Logger log = LogManager.getLogger(ExecuteResultResponseRecorder.class);

    protected IndexManagementType indexManagement;
    private ResultIndexingHandler<IndexableResult, IndexType, IndexManagementType> resultHandler;
    private TaskManagerType taskManager;
    private DiscoveryNodeFilterer nodeFilter;
    private ThreadPool threadPool;
    private Client client;
    private NodeStateManager nodeStateManager;
    private TaskCacheManager taskCacheManager;
    private int rcfMinSamples;
    protected IndexType resultIndex;
    private AnalysisType analysisType;

    public ExecuteResultResponseRecorder(
        IndexManagementType indexManagement,
        ResultIndexingHandler<IndexableResult, IndexType, IndexManagementType> resultHandler,
        TaskManagerType taskManager,
        DiscoveryNodeFilterer nodeFilter,
        ThreadPool threadPool,
        Client client,
        NodeStateManager nodeStateManager,
        TaskCacheManager taskCacheManager,
        int rcfMinSamples,
        IndexType resultIndex,
        AnalysisType analysisType
    ) {
        this.indexManagement = indexManagement;
        this.resultHandler = resultHandler;
        this.taskManager = taskManager;
        this.nodeFilter = nodeFilter;
        this.threadPool = threadPool;
        this.client = client;
        this.nodeStateManager = nodeStateManager;
        this.taskCacheManager = taskCacheManager;
        this.rcfMinSamples = rcfMinSamples;
        this.resultIndex = resultIndex;
        this.analysisType = analysisType;
    }

    public void indexResult(Instant detectionStartTime, Instant executionStartTime, ResultResponse response, Config config) {
        String configId = config.getId();
        try {

            if (!response.shouldSave()) {
                updateRealtimeTask(response, configId);
                return;
            }
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) config.getWindowDelay();
            Instant dataStartTime = detectionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            User user = config.getUser();

            if (response.getError() != null) {
                log.info("Anomaly result action run successfully for {} with error {}", configId, response.getError());
            }

            IndexableResult analysisResult = response
                .toIndexableResult(
                    configId,
                    dataStartTime,
                    dataEndTime,
                    executionStartTime,
                    Instant.now(),
                    indexManagement.getSchemaVersion(resultIndex),
                    user,
                    response.getError()
                );

            String resultIndex = config.getCustomResultIndex();
            resultHandler.index(analysisResult, configId, resultIndex);
            updateRealtimeTask(response, configId);
        } catch (EndRunException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + configId, e);
        }
    }

    /**
     * Update real time task (one document per detector in state index). If the real-time task has no changes compared with local cache,
     * the task won't update. Task only updates when the state changed, or any error happened, or job stopped. Task is mainly consumed
     * by the front-end to track analysis status. For single-stream analyses, we embed model total updates in ResultResponse and
     * update state accordingly. For HC analysis, we won't wait for model finishing updating before returning a response to the job scheduler
     * since it might be long before all entities finish execution. So we don't embed model total updates in AnomalyResultResponse.
     * Instead, we issue a profile request to poll each model node and get the maximum total updates among all models.
     * @param response response returned from executing AnomalyResultAction
     * @param configId config Id
     */
    private void updateRealtimeTask(ResultResponse response, String configId) {
        if (response.isHC() != null && response.isHC()) {
            if (taskManager.skipUpdateHCRealtimeTask(configId, response.getError())) {
                return;
            }
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            Set<DetectorProfileName> profiles = new HashSet<>();
            profiles.add(DetectorProfileName.INIT_PROGRESS);
            ProfileRequest profileRequest = new ProfileRequest(configId, profiles, true, dataNodes);
            Runnable profileHCInitProgress = () -> {
                client.execute(ProfileAction.INSTANCE, profileRequest, ActionListener.wrap(r -> {
                    log.debug("Update latest realtime task for HC detector {}, total updates: {}", configId, r.getTotalUpdates());
                    updateLatestRealtimeTask(
                        configId,
                        null,
                        r.getTotalUpdates(),
                        response.getConfigIntervalInMinutes(),
                        response.getError()
                    );
                }, e -> { log.error("Failed to update latest realtime task for " + configId, e); }));
            };
            if (!taskManager.isHCRealtimeTaskStartInitializing(configId)) {
                // real time init progress is 0 may mean this is a newly started detector
                // Delay real time cache update by one minute. If we are in init status, the delay may give the model training time to
                // finish. We can change the detector running immediately instead of waiting for the next interval.
                threadPool
                    .schedule(profileHCInitProgress, new TimeValue(60, TimeUnit.SECONDS), TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME);
            } else {
                profileHCInitProgress.run();
            }

        } else {
            log
                .debug(
                    "Update latest realtime task for single stream detector {}, total updates: {}",
                    configId,
                    response.getRcfTotalUpdates()
                );
            updateLatestRealtimeTask(
                configId,
                null,
                response.getRcfTotalUpdates(),
                response.getConfigIntervalInMinutes(),
                response.getError()
            );
        }
    }

    private void updateLatestRealtimeTask(
        String configId,
        String taskState,
        Long rcfTotalUpdates,
        Long configIntervalInMinutes,
        String error
    ) {
        // Don't need info as this will be printed repeatedly in each interval
        ActionListener<UpdateResponse> listener = ActionListener.wrap(r -> {
            if (r != null) {
                log.debug("Updated latest realtime task successfully for config {}, taskState: {}", configId, taskState);
            }
        }, e -> {
            if ((e instanceof ResourceNotFoundException) && e.getMessage().contains(CommonMessages.CAN_NOT_FIND_LATEST_TASK)) {
                // Clear realtime task cache, will recreate task in next run, check ADResultProcessor.
                log.error("Can't find latest realtime task of config " + configId);
                taskManager.removeRealtimeTaskCache(configId);
            } else {
                log.error("Failed to update latest realtime task for config " + configId, e);
            }
        });

        // rcfTotalUpdates is null when we save exception messages
        if (!taskCacheManager.hasQueriedResultIndex(configId) && rcfTotalUpdates != null && rcfTotalUpdates < rcfMinSamples) {
            // confirm the total updates number since it is possible that we have already had results after job enabling time
            // If yes, total updates should be at least rcfMinSamples so that the init progress reaches 100%.
            confirmTotalRCFUpdatesFound(
                configId,
                taskState,
                rcfTotalUpdates,
                configIntervalInMinutes,
                error,
                ActionListener
                    .wrap(
                        r -> taskManager
                            .updateLatestRealtimeTaskOnCoordinatingNode(configId, taskState, r, configIntervalInMinutes, error, listener),
                        e -> {
                            log.error("Fail to confirm rcf update", e);
                            taskManager
                                .updateLatestRealtimeTaskOnCoordinatingNode(
                                    configId,
                                    taskState,
                                    rcfTotalUpdates,
                                    configIntervalInMinutes,
                                    error,
                                    listener
                                );
                        }
                    )
            );
        } else {
            taskManager
                .updateLatestRealtimeTaskOnCoordinatingNode(configId, taskState, rcfTotalUpdates, configIntervalInMinutes, error, listener);
        }
    }

    /**
     * The function is not only indexing the result with the exception, but also updating the task state after
     * 60s if the exception is related to cold start (index not found exceptions) for a single stream detector.
     *
     * @param executeStartTime execution start time
     * @param executeEndTime execution end time
     * @param errorMessage Error message to record
     * @param taskState task state (e.g., stopped)
     * @param config config accessor
     */
    public void indexResultException(
        Instant executeStartTime,
        Instant executeEndTime,
        String errorMessage,
        String taskState,
        Config config
    ) {
        String configId = config.getId();
        try {
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) config.getWindowDelay();
            Instant dataStartTime = executeStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executeEndTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            User user = config.getUser();

            IndexableResult resultToSave = createErrorResult(configId, dataStartTime, dataEndTime, executeEndTime, errorMessage, user);
            String resultIndex = config.getCustomResultIndex();
            if (resultIndex != null && !indexManagement.doesIndexExist(resultIndex)) {
                // Set result index as null, will write exception to default result index.
                resultHandler.index(resultToSave, configId, null);
            } else {
                resultHandler.index(resultToSave, configId, resultIndex);
            }

            if (errorMessage.contains(ADCommonMessages.NO_MODEL_ERR_MSG) && !config.isHighCardinality()) {
                // single stream detector raises ResourceNotFoundException containing ADCommonMessages.NO_CHECKPOINT_ERR_MSG
                // when there is no checkpoint.
                // Delay real time cache update by one minute so we will have trained models by then and update the state
                // document accordingly.
                threadPool.schedule(() -> {
                    RCFPollingRequest request = new RCFPollingRequest(configId);
                    client.execute(RCFPollingAction.INSTANCE, request, ActionListener.wrap(rcfPollResponse -> {
                        long totalUpdates = rcfPollResponse.getTotalUpdates();
                        // if there are updates, don't record failures
                        updateLatestRealtimeTask(
                            configId,
                            taskState,
                            totalUpdates,
                            config.getIntervalInMinutes(),
                            totalUpdates > 0 ? "" : errorMessage
                        );
                    }, e -> {
                        log.error("Fail to execute RCFRollingAction", e);
                        updateLatestRealtimeTask(configId, taskState, null, null, errorMessage);
                    }));
                }, new TimeValue(60, TimeUnit.SECONDS), TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME);
            } else {
                updateLatestRealtimeTask(configId, taskState, null, null, errorMessage);
            }

        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + configId, e);
        }
    }

    private void confirmTotalRCFUpdatesFound(
        String configId,
        String taskState,
        Long rcfTotalUpdates,
        Long configIntervalInMinutes,
        String error,
        ActionListener<Long> listener
    ) {
        nodeStateManager.getConfig(configId, analysisType, ActionListener.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new TimeSeriesException(configId, "fail to get detector"));
                return;
            }
            nodeStateManager.getJob(configId, ActionListener.wrap(jobOptional -> {
                if (!jobOptional.isPresent()) {
                    listener.onFailure(new TimeSeriesException(configId, "fail to get job"));
                    return;
                }

                ProfileUtil
                    .confirmRealtimeInitStatus(
                        configOptional.get(),
                        jobOptional.get().getEnabledTime().toEpochMilli(),
                        client,
                        analysisType,
                        ActionListener.wrap(searchResponse -> {
                            ActionListener.completeWith(listener, () -> {
                                SearchHits hits = searchResponse.getHits();
                                Long correctedTotalUpdates = rcfTotalUpdates;
                                if (hits.getTotalHits().value > 0L) {
                                    // correct the number if we have already had results after job enabling time
                                    // so that the detector won't stay initialized
                                    correctedTotalUpdates = Long.valueOf(rcfMinSamples);
                                }
                                taskCacheManager.markResultIndexQueried(configId);
                                return correctedTotalUpdates;
                            });
                        }, exception -> {
                            if (ExceptionUtil.isIndexNotAvailable(exception)) {
                                // anomaly result index is not created yet
                                taskCacheManager.markResultIndexQueried(configId);
                                listener.onResponse(0L);
                            } else {
                                listener.onFailure(exception);
                            }
                        })
                    );
            }, e -> listener.onFailure(new TimeSeriesException(configId, "fail to get job"))));
        }, e -> listener.onFailure(new TimeSeriesException(configId, "fail to get config"))));
    }

    protected abstract IndexableResult createErrorResult(
        String configId,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executeEndTime,
        String errorMessage,
        User user
    );
}
