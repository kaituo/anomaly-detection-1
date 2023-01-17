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

package org.opensearch.timeseries;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.XCONTENT_WITH_TYPE;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.transport.AnomalyResultTransportAction;
import org.opensearch.ad.util.SecurityUtil;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.ResultRequest;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.util.SecurityUtil;

import com.google.common.base.Throwables;

/**
 * JobScheduler will call job runner to get time series analysis result periodically
 */
public abstract class JobProcessor<IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass>, ExecuteResultResponseRecorderType extends ExecuteResultResponseRecorder<IndexType, IndexManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType>> {

    private static final Logger log = LogManager.getLogger(JobProcessor.class);

    private Settings settings;
    private int maxRetryForEndRunException;
    private Client client;
    private ThreadPool threadPool;
    private ConcurrentHashMap<String, Integer> endRunExceptionCount;
    private IndexManagementType indexManagement;
    private TaskManagerType taskManager;
    private NodeStateManager nodeStateManager;
    private ExecuteResultResponseRecorderType recorder;
    private AnalysisType analysisType;
    private String threadPoolName;
    private ActionType<? extends ResultResponse> resultAction;

    protected JobProcessor(AnalysisType analysisType, String threadPoolName, ActionType<? extends ResultResponse> resultAction) {
        // Singleton class, use getJobRunnerInstance method instead of constructor
        this.endRunExceptionCount = new ConcurrentHashMap<>();
        this.analysisType = analysisType;
        this.threadPoolName = threadPoolName;
        this.resultAction = resultAction;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    protected void registerSettings(Settings settings, Setting<Integer> maxRetryForEndRunExceptionSetting) {
        this.settings = settings;
        this.maxRetryForEndRunException = maxRetryForEndRunExceptionSetting.get(settings);
    }

    public void setTaskManager(TaskManagerType adTaskManager) {
        this.taskManager = adTaskManager;
    }

    public void setIndexManagement(IndexManagementType anomalyDetectionIndices) {
        this.indexManagement = anomalyDetectionIndices;
    }

    public void setNodeStateManager(NodeStateManager nodeStateManager) {
        this.nodeStateManager = nodeStateManager;
    }

    public void setExecuteResultResponseRecorder(ExecuteResultResponseRecorderType recorder) {
        this.recorder = recorder;
    }

    public void process(Job jobParameter, JobExecutionContext context) {
        String configId = jobParameter.getName();

        taskManager.refreshRealtimeJobRunTime(configId);

        Instant executionStartTime = Instant.now();
        IntervalSchedule schedule = (IntervalSchedule) jobParameter.getSchedule();
        Instant analysisStartTime = executionStartTime.minus(schedule.getInterval(), schedule.getUnit());

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            try {
                nodeStateManager.getConfig(configId, analysisType, ActionListener.wrap(configOptional -> {
                    if (!configOptional.isPresent()) {
                        log.error(new ParameterizedMessage("fail to get config [{}]", configId));
                        return;
                    }
                    Config config = configOptional.get();

                    if (jobParameter.getLockDurationSeconds() != null) {
                        lockService
                            .acquireLock(
                                jobParameter,
                                context,
                                ActionListener
                                    .wrap(
                                        lock -> runJob(
                                            jobParameter,
                                            lockService,
                                            lock,
                                            analysisStartTime,
                                            executionStartTime,
                                            recorder,
                                            config
                                        ),
                                        exception -> {
                                            indexResultException(
                                                jobParameter,
                                                lockService,
                                                null,
                                                analysisStartTime,
                                                executionStartTime,
                                                exception,
                                                false,
                                                recorder,
                                                config
                                            );
                                            throw new IllegalStateException("Failed to acquire lock for job: " + configId);
                                        }
                                    )
                            );
                    } else {
                        log.warn("Can't get lock for job: " + configId);
                    }

                }, e -> log.error(new ParameterizedMessage("fail to get config [{}]", configId), e)));
            } catch (Exception e) {
                // os log won't show anything if there is an exception happens (maybe due to running on a ExecutorService)
                // we at least log the error.
                log.error("Can't start job: " + configId, e);
                throw e;
            }
        };

        ExecutorService executor = threadPool.executor(threadPoolName);
        executor.submit(runnable);
    }

    /**
     * Get analysis result, index result or handle exception if failed.
     *
     * @param jobParameter scheduled job parameter
     * @param lockService lock service
     * @param lock lock to run job
     * @param analysisStartTime analysis start time
     * @param analysisEndTime detection end time
     * @param recorder utility to record job execution result
     * @param detector associated detector accessor
     */
    protected void runJob(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant analysisStartTime,
        Instant analysisEndTime,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        String detectorId = jobParameter.getName();
        if (lock == null) {
            indexResultException(
                jobParameter,
                lockService,
                lock,
                analysisStartTime,
                analysisEndTime,
                "Can't run AD job due to null lock",
                false,
                recorder,
                detector
            );
            return;
        }
        indexManagement.update();

        User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);

        String user = userInfo.getName();
        List<String> roles = userInfo.getRoles();

        String resultIndex = jobParameter.getCustomResultIndex();
        if (resultIndex == null) {
            runJob(jobParameter, lockService, lock, analysisStartTime, analysisEndTime, detectorId, user, roles, recorder, detector);
            return;
        }
        ActionListener<Boolean> listener = ActionListener.wrap(r -> { log.debug("Custom index is valid"); }, e -> {
            Exception exception = new EndRunException(detectorId, e.getMessage(), true);
            handleException(jobParameter, lockService, lock, analysisStartTime, analysisEndTime, exception, recorder, detector);
        });
        indexManagement.validateCustomIndexForBackendJob(resultIndex, detectorId, user, roles, () -> {
            listener.onResponse(true);
            runJob(jobParameter, lockService, lock, analysisStartTime, analysisEndTime, detectorId, user, roles, recorder, detector);
        }, listener);
    }

    private void runJob(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String configId,
        String user,
        List<String> roles,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        // using one thread in the write threadpool
        try (InjectSecurity injectSecurity = new InjectSecurity(configId, settings, client.threadPool().getThreadContext())) {
            // Injecting user role to verify if the user has permissions for our API.
            injectSecurity.inject(user, roles);

            ResultRequest request = createResultRequest(configId, detectionStartTime.toEpochMilli(), executionStartTime.toEpochMilli());
            client
                .execute(
                    resultAction,
                    request,
                    ActionListener
                        .wrap(
                            response -> {
                                indexResult(
                                    jobParameter,
                                    lockService,
                                    lock,
                                    detectionStartTime,
                                    executionStartTime,
                                    response,
                                    recorder,
                                    detector
                                );
                            },
                            exception -> {
                                handleException(
                                    jobParameter,
                                    lockService,
                                    lock,
                                    detectionStartTime,
                                    executionStartTime,
                                    exception,
                                    recorder,
                                    detector
                                );
                            }
                        )
                );
        } catch (Exception e) {
            indexResultException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, e, true, recorder, detector);
            log.error("Failed to execute AD job " + configId, e);
        }
    }

    /**
     * Handle exception from anomaly result action.
     *
     * 1. If exception is {@link EndRunException}
     *   a). if isEndNow == true, stop AD job and store exception in anomaly result
     *   b). if isEndNow == false, record count of {@link EndRunException} for this
     *       detector. If count of {@link EndRunException} exceeds upper limit, will
     *       stop AD job and store exception in anomaly result; otherwise, just
     *       store exception in anomaly result, not stop AD job for the detector.
     *
     * 2. If exception is not {@link EndRunException}, decrease count of
     *    {@link EndRunException} for the detector and index eception in Anomaly
     *    result. If exception is {@link InternalFailure}, will not log exception
     *    stack trace as already logged in {@link AnomalyResultTransportAction}.
     *
     * TODO: Handle finer granularity exception such as some exception may be
     *       transient and retry in current job may succeed. Currently, we don't
     *       know which exception is transient and retryable in
     *       {@link AnomalyResultTransportAction}. So we don't add backoff retry
     *       now to avoid bring extra load to cluster, expecially the code start
     *       process is relatively heavy by sending out 24 queries, initializing
     *       models, and saving checkpoints.
     *       Sometimes missing anomaly and notification is not acceptable. For example,
     *       current detection interval is 1hour, and there should be anomaly in
     *       current interval, some transient exception may fail current AD job,
     *       so no anomaly found and user never know it. Then we start next AD job,
     *       maybe there is no anomaly in next 1hour, user will never know something
     *       wrong happened. In one word, this is some tradeoff between protecting
     *       our performance, user experience and what we can do currently.
     *
     * @param jobParameter scheduled job parameter
     * @param lockService lock service
     * @param lock lock to run job
     * @param detectionStartTime detection start time
     * @param executionStartTime detection end time
     * @param exception exception
     * @param recorder utility to record job execution result
     * @param config associated config accessor
     */
    protected void handleException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        Exception exception,
        ExecuteResultResponseRecorderType recorder,
        Config config
    ) {
        String detectorId = jobParameter.getName();
        if (exception instanceof EndRunException) {
            log.error("EndRunException happened when executing anomaly result action for " + detectorId, exception);

            if (((EndRunException) exception).isEndNow()) {
                // Stop AD job if EndRunException shows we should end job now.
                log.info("JobRunner will stop AD job due to EndRunException for {}", detectorId);
                stopJobForEndRunException(
                    jobParameter,
                    lockService,
                    lock,
                    detectionStartTime,
                    executionStartTime,
                    (EndRunException) exception,
                    recorder,
                    config
                );
            } else {
                endRunExceptionCount.compute(detectorId, (k, v) -> {
                    if (v == null) {
                        return 1;
                    } else {
                        return v + 1;
                    }
                });
                log.info("EndRunException happened for {}", detectorId);
                // if AD job failed consecutively due to EndRunException and failed times exceeds upper limit, will stop AD job
                if (endRunExceptionCount.get(detectorId) > maxRetryForEndRunException) {
                    log
                        .info(
                            "JobRunner will stop AD job due to EndRunException retry exceeds upper limit {} for {}",
                            maxRetryForEndRunException,
                            detectorId
                        );
                    stopJobForEndRunException(
                        jobParameter,
                        lockService,
                        lock,
                        detectionStartTime,
                        executionStartTime,
                        (EndRunException) exception,
                        recorder,
                        config
                    );
                    return;
                }
                indexResultException(
                    jobParameter,
                    lockService,
                    lock,
                    detectionStartTime,
                    executionStartTime,
                    exception.getMessage(),
                    true,
                    recorder,
                    config
                );
            }
        } else {
            endRunExceptionCount.remove(detectorId);
            if (exception instanceof InternalFailure) {
                log.error("InternalFailure happened when executing anomaly result action for " + detectorId, exception);
            } else {
                log.error("Failed to execute anomaly result action for " + detectorId, exception);
            }
            indexResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                exception,
                true,
                recorder,
                config
            );
        }
    }

    private void stopJobForEndRunException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        EndRunException exception,
        ExecuteResultResponseRecorderType recorder,
        Config config
    ) {
        String configId = jobParameter.getName();
        endRunExceptionCount.remove(configId);
        String errorPrefix = exception.isEndNow()
            ? "Stopped analysis: "
            : "Stopped analysis as job failed consecutively for more than " + this.maxRetryForEndRunException + " times: ";
        String error = errorPrefix + exception.getMessage();
        stopJob(
            configId,
            () -> indexResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                error,
                true,
                TaskState.STOPPED.name(),
                recorder,
                config
            )
        );
    }

    private void stopJob(String detectorId, ExecutorFunction function) {
        GetRequest getRequest = new GetRequest(CommonName.JOB_INDEX).id(detectorId);
        ActionListener<GetResponse> listener = ActionListener.wrap(response -> {
            if (response.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job job = Job.parse(parser);
                    if (job.isEnabled()) {
                        Job newJob = new Job(
                            job.getName(),
                            job.getSchedule(),
                            job.getWindowDelay(),
                            false,
                            job.getEnabledTime(),
                            Instant.now(),
                            Instant.now(),
                            job.getLockDurationSeconds(),
                            job.getUser(),
                            job.getCustomResultIndex(),
                            job.getAnalysisType()
                        );
                        IndexRequest indexRequest = new IndexRequest(CommonName.JOB_INDEX)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                            .source(newJob.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE))
                            .id(detectorId);

                        client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                            if (indexResponse != null && (indexResponse.getResult() == CREATED || indexResponse.getResult() == UPDATED)) {
                                log.info("Job was disabled by JobRunner for " + detectorId);
                                // function.execute();
                            } else {
                                log.warn("Failed to disable job for " + detectorId);
                            }
                        }, exception -> { log.error("JobRunner failed to update job as disabled for " + detectorId, exception); }));
                    } else {
                        log.info("Job was disabled for " + detectorId);
                    }
                } catch (IOException e) {
                    log.error("JobRunner failed to stop detector job " + detectorId, e);
                }
            } else {
                log.info("AD Job was not found for " + detectorId);
            }
        }, exception -> log.error("JobRunner failed to get detector job " + detectorId, exception));

        client.get(getRequest, ActionListener.runAfter(listener, () -> function.execute()));
    }

    private void indexResult(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        ResultResponse response,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        String detectorId = jobParameter.getName();
        endRunExceptionCount.remove(detectorId);
        try {
            recorder.indexResult(detectionStartTime, executionStartTime, response, detector);
        } catch (EndRunException e) {
            handleException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, e, recorder, detector);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + detectorId, e);
        } finally {
            releaseLock(jobParameter, lockService, lock);
        }

    }

    private void indexResultException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        Exception exception,
        boolean releaseLock,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        try {
            String errorMessage = exception instanceof TimeSeriesException
                ? exception.getMessage()
                : Throwables.getStackTraceAsString(exception);
            indexResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                errorMessage,
                releaseLock,
                recorder,
                detector
            );
        } catch (Exception e) {
            log.error("Failed to index result for " + jobParameter.getName(), e);
        }
    }

    private void indexResultException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String errorMessage,
        boolean releaseLock,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        indexResultException(
            jobParameter,
            lockService,
            lock,
            detectionStartTime,
            executionStartTime,
            errorMessage,
            releaseLock,
            null,
            recorder,
            detector
        );
    }

    private void indexResultException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String errorMessage,
        boolean releaseLock,
        String taskState,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        try {
            recorder.indexResultException(detectionStartTime, executionStartTime, errorMessage, taskState, detector);
        } finally {
            if (releaseLock) {
                releaseLock(jobParameter, lockService, lock);
            }
        }
    }

    private void releaseLock(Job jobParameter, LockService lockService, LockModel lock) {
        lockService
            .release(
                lock,
                ActionListener
                    .wrap(
                        released -> { log.info("Released lock for AD job {}", jobParameter.getName()); },
                        exception -> { log.error("Failed to release lock for AD job: " + jobParameter.getName(), exception); }
                    )
            );
    }

    protected abstract ResultRequest createResultRequest(String configID, long start, long end);
}
