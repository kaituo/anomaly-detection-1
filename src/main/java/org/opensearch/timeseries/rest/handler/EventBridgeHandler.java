/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.timeseries.rest.handler;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.StringUtil;
import org.opensearch.transport.TransportService;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.scheduler.SchedulerAsyncClient;
import software.amazon.awssdk.services.scheduler.model.ConflictException;
import software.amazon.awssdk.services.scheduler.model.CreateScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.DeleteScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindow;
import software.amazon.awssdk.services.scheduler.model.FlexibleTimeWindowMode;
import software.amazon.awssdk.services.scheduler.model.GetScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.GetScheduleResponse;
import software.amazon.awssdk.services.scheduler.model.ResourceNotFoundException;
import software.amazon.awssdk.services.scheduler.model.ScheduleState;
import software.amazon.awssdk.services.scheduler.model.SqsParameters;
import software.amazon.awssdk.services.scheduler.model.Target;
import software.amazon.awssdk.services.scheduler.model.UpdateScheduleRequest;

/**
 * EventBridge handler to process job events via AWS EventBridge Scheduler API.
 */
public abstract class EventBridgeHandler {

    private static final Logger logger = LogManager.getLogger(EventBridgeHandler.class);
    private static final String SCHEDULE_TIMEZONE = "UTC";
    private static final int FLEXIBLE_WINDOW_MINUTES = 1;
    private static final String DEFAULT_MAINTENANCE_SCHEDULE_GROUP = "timeseries";
    private static final String HOURLY_CRON_SCHEDULE_NAME = "HourlyCron";
    private static final String HOURLY_CRON_DESCRIPTION = "Periodic maintenance and cleanup";
    private static final String DAILY_S3_CLEANUP_SCHEDULE_NAME = "DailyS3CheckpointCleanup";
    private static final String DAILY_S3_CLEANUP_DESCRIPTION = "Daily S3 checkpoint cleanup";
    private static final int TENANT_HASH_SEED = 0;
    private final SchedulerAsyncClient schedulerClient;
    private final String sqsQueueArn;
    private final String schedulerRoleArn;
    private final AnalysisType analysisType;
    private final String scheduleGroup;
    private final String maintenanceScheduleGroup;
    private final Clock clock;
    private volatile Duration maintenanceScheduleInterval;
    private volatile Duration dailyS3CleanupInterval;

    /**
     * Constructor function.
     *
     * @param region AWS region for EventBridge Scheduler
     * @param sqsQueueArn ARN of the FIFO SQS queue that receives scheduled job triggers
     * @param schedulerRoleArn IAM role ARN assumed by EventBridge Scheduler to publish to SQS
     * @param configuredScheduleGroup optional AWS Scheduler group name override
     * @param analysisType Analysis type (AD or FORECAST)
     * @param checkpointSavingFreq checkpoint saving frequency used to derive the maintenance schedule interval
     * @param dailyS3CleanupInterval interval used to trigger daily S3 checkpoint cleanup
     * @param clock Clock to get current time
     */
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "java.security.AccessController usage: required for privileged AWS client initialization.")
    public EventBridgeHandler(
        String region,
        String sqsQueueArn,
        String schedulerRoleArn,
        String configuredScheduleGroup,
        AnalysisType analysisType,
        Duration checkpointSavingFreq,
        Duration dailyS3CleanupInterval,
        Clock clock
    ) {
        this(
            createSchedulerClient(region),
            sqsQueueArn,
            schedulerRoleArn,
            configuredScheduleGroup,
            analysisType,
            checkpointSavingFreq,
            dailyS3CleanupInterval,
            clock
        );
    }

    EventBridgeHandler(
        SchedulerAsyncClient schedulerClient,
        String sqsQueueArn,
        String schedulerRoleArn,
        String configuredScheduleGroup,
        AnalysisType analysisType,
        Duration checkpointSavingFreq,
        Duration dailyS3CleanupInterval,
        Clock clock
    ) {
        this.schedulerClient = Objects.requireNonNull(schedulerClient, "schedulerClient must not be null");
        this.sqsQueueArn = Objects.requireNonNull(sqsQueueArn, "sqsQueueArn must not be null");
        this.schedulerRoleArn = Objects.requireNonNull(schedulerRoleArn, "schedulerRoleArn must not be null");
        if (this.sqsQueueArn.isBlank()) {
            throw new IllegalArgumentException("sqsQueueArn must not be blank");
        }
        if (this.schedulerRoleArn.isBlank()) {
            throw new IllegalArgumentException("schedulerRoleArn must not be blank");
        }
        this.analysisType = analysisType;
        this.scheduleGroup = resolveConfigScheduleGroup(configuredScheduleGroup, analysisType);
        this.maintenanceScheduleGroup = resolveMaintenanceScheduleGroup(configuredScheduleGroup);
        this.clock = clock;
        this.maintenanceScheduleInterval = resolveMaintenanceScheduleInterval(checkpointSavingFreq);
        this.dailyS3CleanupInterval = resolveDailyS3CleanupInterval(dailyS3CleanupInterval);
    }

    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "java.security.AccessController usage: required for privileged AWS client initialization.")
    private static SchedulerAsyncClient createSchedulerClient(String region) {
        return AccessController
            .doPrivileged(
                (PrivilegedAction<SchedulerAsyncClient>) () -> SchedulerAsyncClient
                    .builder()
                    .region(Region.of(region))
                    .credentialsProvider(SecurityUtil.createCredentialsProvider())
                    .build()
            );
    }

    public static String resolveConfigScheduleGroup(String configuredScheduleGroup, AnalysisType analysisType) {
        if (Strings.isBlank(configuredScheduleGroup)) {
            return analysisType.name().toLowerCase(Locale.ROOT);
        }
        return configuredScheduleGroup.trim();
    }

    public static String resolveMaintenanceScheduleGroup(String configuredScheduleGroup) {
        if (Strings.isBlank(configuredScheduleGroup)) {
            return DEFAULT_MAINTENANCE_SCHEDULE_GROUP;
        }
        return configuredScheduleGroup.trim();
    }

    static Duration resolveMaintenanceScheduleInterval(Duration checkpointSavingFreq) {
        Objects.requireNonNull(checkpointSavingFreq, "checkpointSavingFreq must not be null");
        Duration normalizedCheckpointInterval = Duration.ofMinutes(toScheduleIntervalMinutes(checkpointSavingFreq));
        return normalizedCheckpointInterval.compareTo(TimeSeriesSettings.HOURLY_MAINTENANCE) < 0
            ? normalizedCheckpointInterval
            : TimeSeriesSettings.HOURLY_MAINTENANCE;
    }

    static String buildMaintenanceScheduleExpression(Duration maintenanceScheduleInterval) {
        return buildRateExpression(maintenanceScheduleInterval);
    }

    static Duration resolveDailyS3CleanupInterval(Duration dailyS3CleanupInterval) {
        Objects.requireNonNull(dailyS3CleanupInterval, "dailyS3CleanupInterval must not be null");
        return Duration.ofMinutes(toScheduleIntervalMinutes(dailyS3CleanupInterval));
    }

    static String buildDailyS3CleanupScheduleExpression(Duration dailyS3CleanupInterval) {
        return buildRateExpression(dailyS3CleanupInterval);
    }

    private static String buildRateExpression(Duration interval) {
        long scheduleIntervalMinutes = toScheduleIntervalMinutes(interval);
        if (scheduleIntervalMinutes % 60 == 0) {
            long scheduleIntervalHours = scheduleIntervalMinutes / 60;
            return scheduleIntervalHours == 1 ? "rate(1 hour)" : "rate(" + scheduleIntervalHours + " hours)";
        }
        return scheduleIntervalMinutes == 1 ? "rate(1 minute)" : "rate(" + scheduleIntervalMinutes + " minutes)";
    }

    public static boolean isMaintenanceScheduleName(String scheduleName) {
        return HOURLY_CRON_SCHEDULE_NAME.equals(scheduleName) || DAILY_S3_CLEANUP_SCHEDULE_NAME.equals(scheduleName);
    }

    public void setMaintenanceScheduleInterval(Duration checkpointSavingFreq) {
        this.maintenanceScheduleInterval = resolveMaintenanceScheduleInterval(checkpointSavingFreq);
    }

    public void setDailyS3CleanupInterval(Duration dailyS3CleanupInterval) {
        this.dailyS3CleanupInterval = resolveDailyS3CleanupInterval(dailyS3CleanupInterval);
    }

    private static int toScheduleIntervalMinutes(Duration interval) {
        long intervalMillis = interval.toMillis();
        long intervalMinutes = Math.max(1L, (intervalMillis + Duration.ofMinutes(1).toMillis() - 1) / Duration.ofMinutes(1).toMillis());
        return Math.toIntExact(intervalMinutes);
    }

    /**
     * Start job via EventBridge Scheduler.
     * 1. If job doesn't exist, create new job schedule.
     * 2. If job exists: a). if job enabled, update job schedule; b). if job disabled, enable job schedule.
     * 
     * We don't need to preactively trigger getting result transport action as EventBridge will send sqs message
     * within the first minute of starting the job, irrespective of the job schedule.
     *
     * @param config config accessor
     * @param transportService transport service
     * @param clock clock to get current time
     * @param listener Listener to send responses
     */
    public void startJob(Config config, TransportService transportService, Clock clock, ActionListener<JobResponse> listener) {
        createOrUpdateScheduleAsync(config).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                Throwable cause = unwrapAsyncException(throwable);
                logger.error("Failed to start job via EventBridge Scheduler for config: " + config.getId(), cause);
                listener.onFailure(ExceptionUtil.asException(cause));
            } else {
                listener.onResponse(new JobResponse(config.getId()));
            }
        });
    }

    /**
     * Stop job via EventBridge Scheduler.
     * @param configId config identifier
     * @param tenantId tenant identifier
     * @param transportService transport service
     * @param listener Listener to send responses
     */
    public void stopJob(String configId, String tenantId, TransportService transportService, ActionListener<JobResponse> listener) {
        disableScheduleAsync(configId, tenantId).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                Throwable cause = unwrapAsyncException(throwable);
                logger.error("Failed to stop job via EventBridge Scheduler for config: " + configId, cause);
                listener.onFailure(ExceptionUtil.asException(cause));
            } else {
                listener.onResponse(new JobResponse(configId));
            }
        });
    }

    /**
     * Close the EventBridge Scheduler client.
     */
    public void close() {
        if (schedulerClient != null) {
            schedulerClient.close();
        }
    }

    /**
     * Start the maintenance schedule via EventBridge Scheduler.
     *
     * @param listener Listener to send responses
     */
    public void startHourlyCron(ActionListener<JobResponse> listener) {
        createOrUpdateHourlyCronAsync().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                Throwable cause = unwrapAsyncException(throwable);
                logger.error("Failed to start hourly cron via EventBridge Scheduler", cause);
                listener.onFailure(ExceptionUtil.asException(cause));
            } else {
                listener.onResponse(new JobResponse(HOURLY_CRON_SCHEDULE_NAME));
            }
        });
    }

    /**
     * Stop the maintenance schedule via EventBridge Scheduler.
     *
     * @param listener Listener to send responses
     */
    public void stopHourlyCron(ActionListener<JobResponse> listener) {
        disableHourlyCronAsync().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                Throwable cause = unwrapAsyncException(throwable);
                logger.error("Failed to stop hourly cron via EventBridge Scheduler", cause);
                listener.onFailure(ExceptionUtil.asException(cause));
            } else {
                listener.onResponse(new JobResponse(HOURLY_CRON_SCHEDULE_NAME));
            }
        });
    }

    /**
     * Start daily S3 checkpoint cleanup via EventBridge Scheduler.
     * Creates or updates the daily S3 checkpoint cleanup schedule using the configured interval.
     *
     * @param listener Listener to send responses
     */
    public void startDailyS3CheckpointCleanup(ActionListener<JobResponse> listener) {
        createOrUpdateDailyS3CleanupAsync().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                Throwable cause = unwrapAsyncException(throwable);
                logger.error("Failed to start daily S3 checkpoint cleanup via EventBridge Scheduler", cause);
                listener.onFailure(ExceptionUtil.asException(cause));
            } else {
                listener.onResponse(new JobResponse(DAILY_S3_CLEANUP_SCHEDULE_NAME));
            }
        });
    }

    /**
     * Stop daily S3 checkpoint cleanup via EventBridge Scheduler.
     *
     * @param listener Listener to send responses
     */
    public void stopDailyS3CheckpointCleanup(ActionListener<JobResponse> listener) {
        disableDailyS3CleanupAsync().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                Throwable cause = unwrapAsyncException(throwable);
                logger.error("Failed to stop daily S3 checkpoint cleanup via EventBridge Scheduler", cause);
                listener.onFailure(ExceptionUtil.asException(cause));
            } else {
                listener.onResponse(new JobResponse(DAILY_S3_CLEANUP_SCHEDULE_NAME));
            }
        });
    }

    private CompletableFuture<Void> createOrUpdateHourlyCronAsync() {
        String maintenanceScheduleExpression = buildMaintenanceScheduleExpression(maintenanceScheduleInterval);
        Target target = buildHourlyCronSqsTarget();
        FlexibleTimeWindow flexibleWindow = FlexibleTimeWindow
            .builder()
            .mode(FlexibleTimeWindowMode.FLEXIBLE)
            .maximumWindowInMinutes(FLEXIBLE_WINDOW_MINUTES)
            .build();

        CreateScheduleRequest createScheduleRequest = CreateScheduleRequest
            .builder()
            .groupName(maintenanceScheduleGroup)
            .name(HOURLY_CRON_SCHEDULE_NAME)
            .scheduleExpression(maintenanceScheduleExpression)
            .scheduleExpressionTimezone(SCHEDULE_TIMEZONE)
            .state(ScheduleState.ENABLED)
            .description(HOURLY_CRON_DESCRIPTION)
            .target(target)
            .flexibleTimeWindow(flexibleWindow)
            .build();

        UpdateScheduleRequest updateScheduleRequest = UpdateScheduleRequest
            .builder()
            .groupName(maintenanceScheduleGroup)
            .name(HOURLY_CRON_SCHEDULE_NAME)
            .scheduleExpression(maintenanceScheduleExpression)
            .scheduleExpressionTimezone(SCHEDULE_TIMEZONE)
            .state(ScheduleState.ENABLED)
            .description(HOURLY_CRON_DESCRIPTION)
            .target(target)
            .flexibleTimeWindow(flexibleWindow)
            .build();

        CompletableFuture<Void> result = new CompletableFuture<>();
        schedulerClient
            .getSchedule(GetScheduleRequest.builder().groupName(maintenanceScheduleGroup).name(HOURLY_CRON_SCHEDULE_NAME).build())
            .whenComplete((existingSchedule, getError) -> {
                if (getError == null) {
                    updateHourlyCronSchedule(updateScheduleRequest, maintenanceScheduleExpression, result);
                    return;
                }

                Throwable cause = unwrapAsyncException(getError);
                if (cause instanceof ResourceNotFoundException) {
                    createHourlyCronSchedule(createScheduleRequest, updateScheduleRequest, maintenanceScheduleExpression, result);
                } else {
                    result.completeExceptionally(cause);
                }
            });

        return result;
    }

    private void createHourlyCronSchedule(
        CreateScheduleRequest createScheduleRequest,
        UpdateScheduleRequest updateScheduleRequest,
        String maintenanceScheduleExpression,
        CompletableFuture<Void> result
    ) {
        schedulerClient.createSchedule(createScheduleRequest).whenComplete((response, throwable) -> {
            if (throwable == null) {
                logger
                    .info(
                        "Created AWS Scheduler maintenance schedule {} with expression {} targeting queue {}",
                        response.scheduleArn(),
                        maintenanceScheduleExpression,
                        sqsQueueArn
                    );
                result.complete(null);
                return;
            }

            Throwable cause = unwrapAsyncException(throwable);
            if (cause instanceof ConflictException) {
                updateHourlyCronSchedule(updateScheduleRequest, maintenanceScheduleExpression, result);
            } else {
                result.completeExceptionally(cause);
            }
        });
    }

    private void updateHourlyCronSchedule(
        UpdateScheduleRequest updateScheduleRequest,
        String maintenanceScheduleExpression,
        CompletableFuture<Void> result
    ) {
        schedulerClient.updateSchedule(updateScheduleRequest).whenComplete((updateResponse, updateError) -> {
            if (updateError == null) {
                logger
                    .info(
                        "Updated AWS Scheduler maintenance schedule {} with expression {} targeting queue {}",
                        updateResponse.scheduleArn(),
                        maintenanceScheduleExpression,
                        sqsQueueArn
                    );
                result.complete(null);
            } else {
                Throwable updateCause = unwrapAsyncException(updateError);
                if (updateCause instanceof ConflictException) {
                    schedulerClient
                        .getSchedule(
                            GetScheduleRequest.builder().groupName(maintenanceScheduleGroup).name(HOURLY_CRON_SCHEDULE_NAME).build()
                        )
                        .whenComplete((existingSchedule, getError) -> {
                            if (getError == null && existingSchedule != null && ScheduleState.ENABLED.equals(existingSchedule.state())) {
                                logger.info("Maintenance schedule already enabled; treating as success.");
                                result.complete(null);
                            } else {
                                result.completeExceptionally(updateCause);
                            }
                        });
                } else {
                    result.completeExceptionally(updateCause);
                }
            }
        });
    }

    private Target buildHourlyCronSqsTarget() {
        return Target
            .builder()
            .arn(sqsQueueArn)
            .roleArn(schedulerRoleArn)
            // FIFO queues require a messageGroupId; use the schedule name as a stable group id.
            .sqsParameters(buildSqsParameters(HOURLY_CRON_SCHEDULE_NAME))
            .input(buildHourlyCronTargetInput())
            .build();
    }

    private String buildHourlyCronTargetInput() {
        int maintenanceScheduleIntervalMinutes = toScheduleIntervalMinutes(maintenanceScheduleInterval);
        Job job = new Job(
            HOURLY_CRON_SCHEDULE_NAME,
            new IntervalSchedule(clock.instant(), maintenanceScheduleIntervalMinutes, java.time.temporal.ChronoUnit.MINUTES),
            null, // no window delay
            true,
            clock.instant(),
            null,
            clock.instant(),
            Duration.ofMinutes(maintenanceScheduleIntervalMinutes).getSeconds(),
            null, // no user
            null, // no tenant ID
            null, // no result index
            AnalysisType.HOURLY_MAINTENANCE
        );

        try {
            XContentBuilder builder = job.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            Map<String, Object> jobMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
            jobMap.put(CommonName.EB_SCHEDULED_TIME_FIELD, CommonName.EB_SCHEDULED_TIME_VALUE);

            XContentBuilder decoratedBuilder = JsonXContent.contentBuilder();
            decoratedBuilder.map(jobMap);
            return BytesReference.bytes(decoratedBuilder).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize hourly cron job payload for EventBridge target", e);
        }
    }

    private CompletableFuture<Void> disableHourlyCronAsync() {
        CompletableFuture<Void> result = new CompletableFuture<>();

        schedulerClient
            .getSchedule(GetScheduleRequest.builder().groupName(maintenanceScheduleGroup).name(HOURLY_CRON_SCHEDULE_NAME).build())
            .whenComplete((existingSchedule, throwable) -> {
                if (throwable != null) {
                    Throwable cause = unwrapAsyncException(throwable);
                    if (cause instanceof ResourceNotFoundException) {
                        logger.info("AWS Scheduler hourly cron schedule {} not found when disabling", HOURLY_CRON_SCHEDULE_NAME);
                        result.complete(null);
                    } else {
                        result.completeExceptionally(cause);
                    }
                    return;
                }

                UpdateScheduleRequest.Builder updateRequestBuilder = UpdateScheduleRequest
                    .builder()
                    .groupName(maintenanceScheduleGroup)
                    .name(HOURLY_CRON_SCHEDULE_NAME)
                    .state(ScheduleState.DISABLED);

                String existingScheduleExpression = existingSchedule.scheduleExpression();
                FlexibleTimeWindow existingFlexibleWindow = existingSchedule.flexibleTimeWindow();
                Target existingTarget = existingSchedule.target();

                if (existingScheduleExpression == null || existingFlexibleWindow == null || existingTarget == null) {
                    result.completeExceptionally(new IllegalStateException("Existing hourly cron schedule is missing required fields"));
                    return;
                }

                updateRequestBuilder.scheduleExpression(existingScheduleExpression);
                updateRequestBuilder.flexibleTimeWindow(existingFlexibleWindow);
                updateRequestBuilder.target(existingTarget);

                if (existingSchedule.scheduleExpressionTimezone() != null) {
                    updateRequestBuilder.scheduleExpressionTimezone(existingSchedule.scheduleExpressionTimezone());
                }

                UpdateScheduleRequest updateScheduleRequest = updateRequestBuilder.build();

                schedulerClient.updateSchedule(updateScheduleRequest).whenComplete((response, updateError) -> {
                    if (updateError != null) {
                        result.completeExceptionally(unwrapAsyncException(updateError));
                        return;
                    }
                    logger.info("Disabled AWS Scheduler hourly cron schedule {}", HOURLY_CRON_SCHEDULE_NAME);
                    result.complete(null);
                });
            });

        return result;
    }

    private CompletableFuture<Void> createOrUpdateDailyS3CleanupAsync() {
        String dailyS3CleanupScheduleExpression = buildDailyS3CleanupScheduleExpression(dailyS3CleanupInterval);
        Target target = buildDailyS3CleanupSqsTarget();
        FlexibleTimeWindow flexibleWindow = FlexibleTimeWindow
            .builder()
            .mode(FlexibleTimeWindowMode.FLEXIBLE)
            .maximumWindowInMinutes(FLEXIBLE_WINDOW_MINUTES)
            .build();

        CreateScheduleRequest createScheduleRequest = CreateScheduleRequest
            .builder()
            .groupName(maintenanceScheduleGroup)
            .name(DAILY_S3_CLEANUP_SCHEDULE_NAME)
            .scheduleExpression(dailyS3CleanupScheduleExpression)
            .scheduleExpressionTimezone(SCHEDULE_TIMEZONE)
            .state(ScheduleState.ENABLED)
            .description(DAILY_S3_CLEANUP_DESCRIPTION)
            .target(target)
            .flexibleTimeWindow(flexibleWindow)
            .build();

        UpdateScheduleRequest updateScheduleRequest = UpdateScheduleRequest
            .builder()
            .groupName(maintenanceScheduleGroup)
            .name(DAILY_S3_CLEANUP_SCHEDULE_NAME)
            .scheduleExpression(dailyS3CleanupScheduleExpression)
            .scheduleExpressionTimezone(SCHEDULE_TIMEZONE)
            .state(ScheduleState.ENABLED)
            .description(DAILY_S3_CLEANUP_DESCRIPTION)
            .target(target)
            .flexibleTimeWindow(flexibleWindow)
            .build();

        CompletableFuture<Void> result = new CompletableFuture<>();
        schedulerClient
            .getSchedule(GetScheduleRequest.builder().groupName(maintenanceScheduleGroup).name(DAILY_S3_CLEANUP_SCHEDULE_NAME).build())
            .whenComplete((existingSchedule, getError) -> {
                if (getError == null) {
                    updateDailyS3CleanupSchedule(updateScheduleRequest, dailyS3CleanupScheduleExpression, result);
                    return;
                }

                Throwable cause = unwrapAsyncException(getError);
                if (cause instanceof ResourceNotFoundException) {
                    createDailyS3CleanupSchedule(
                        createScheduleRequest,
                        updateScheduleRequest,
                        dailyS3CleanupScheduleExpression,
                        result
                    );
                } else {
                    result.completeExceptionally(cause);
                }
            });

        return result;
    }

    private void createDailyS3CleanupSchedule(
        CreateScheduleRequest createScheduleRequest,
        UpdateScheduleRequest updateScheduleRequest,
        String dailyS3CleanupScheduleExpression,
        CompletableFuture<Void> result
    ) {
        schedulerClient.createSchedule(createScheduleRequest).whenComplete((response, throwable) -> {
            if (throwable == null) {
                logger
                    .info(
                        "Created AWS Scheduler daily S3 cleanup schedule {} with expression {} targeting queue {}",
                        response.scheduleArn(),
                        dailyS3CleanupScheduleExpression,
                        sqsQueueArn
                    );
                result.complete(null);
                return;
            }

            Throwable cause = unwrapAsyncException(throwable);
            if (cause instanceof ConflictException) {
                updateDailyS3CleanupSchedule(updateScheduleRequest, dailyS3CleanupScheduleExpression, result);
            } else {
                result.completeExceptionally(cause);
            }
        });
    }

    private void updateDailyS3CleanupSchedule(
        UpdateScheduleRequest updateScheduleRequest,
        String dailyS3CleanupScheduleExpression,
        CompletableFuture<Void> result
    ) {
        schedulerClient.updateSchedule(updateScheduleRequest).whenComplete((updateResponse, updateError) -> {
            if (updateError == null) {
                logger
                    .info(
                        "Updated AWS Scheduler daily S3 cleanup schedule {} with expression {} targeting queue {}",
                        updateResponse.scheduleArn(),
                        dailyS3CleanupScheduleExpression,
                        sqsQueueArn
                    );
                result.complete(null);
            } else {
                Throwable updateCause = unwrapAsyncException(updateError);
                if (updateCause instanceof ConflictException) {
                    schedulerClient
                        .getSchedule(
                            GetScheduleRequest.builder().groupName(maintenanceScheduleGroup).name(DAILY_S3_CLEANUP_SCHEDULE_NAME).build()
                        )
                        .whenComplete((existingSchedule, getError) -> {
                            if (getError == null && existingSchedule != null && ScheduleState.ENABLED.equals(existingSchedule.state())) {
                                logger.info("Daily S3 cleanup schedule already enabled; treating as success.");
                                result.complete(null);
                            } else {
                                result.completeExceptionally(updateCause);
                            }
                        });
                } else {
                    result.completeExceptionally(updateCause);
                }
            }
        });
    }

    private Target buildDailyS3CleanupSqsTarget() {
        return Target
            .builder()
            .arn(sqsQueueArn)
            .roleArn(schedulerRoleArn)
            // FIFO queues require a messageGroupId; use the schedule name as a stable group id.
            .sqsParameters(buildSqsParameters(DAILY_S3_CLEANUP_SCHEDULE_NAME))
            .input(buildDailyS3CleanupTargetInput())
            .build();
    }

    private String buildDailyS3CleanupTargetInput() {
        int dailyS3CleanupIntervalMinutes = toScheduleIntervalMinutes(dailyS3CleanupInterval);
        Job job = new Job(
            DAILY_S3_CLEANUP_SCHEDULE_NAME,
            new IntervalSchedule(clock.instant(), dailyS3CleanupIntervalMinutes, java.time.temporal.ChronoUnit.MINUTES),
            null, // no window delay
            true,
            clock.instant(),
            null,
            clock.instant(),
            Duration.ofMinutes(dailyS3CleanupIntervalMinutes).getSeconds(),
            null, // no user
            null, // no tenant ID
            null, // no result index
            AnalysisType.DAILY_S3_CHECKPOINT_CLEANUP
        );

        try {
            XContentBuilder builder = job.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            Map<String, Object> jobMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
            jobMap.put(CommonName.EB_SCHEDULED_TIME_FIELD, CommonName.EB_SCHEDULED_TIME_VALUE);

            XContentBuilder decoratedBuilder = JsonXContent.contentBuilder();
            decoratedBuilder.map(jobMap);
            return BytesReference.bytes(decoratedBuilder).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize daily S3 cleanup job payload for EventBridge target", e);
        }
    }

    private CompletableFuture<Void> disableDailyS3CleanupAsync() {
        CompletableFuture<Void> result = new CompletableFuture<>();

        schedulerClient
            .getSchedule(GetScheduleRequest.builder().groupName(maintenanceScheduleGroup).name(DAILY_S3_CLEANUP_SCHEDULE_NAME).build())
            .whenComplete((existingSchedule, throwable) -> {
                if (throwable != null) {
                    Throwable cause = unwrapAsyncException(throwable);
                    if (cause instanceof ResourceNotFoundException) {
                        logger.info("AWS Scheduler daily S3 cleanup schedule {} not found when disabling", DAILY_S3_CLEANUP_SCHEDULE_NAME);
                        result.complete(null);
                    } else {
                        result.completeExceptionally(cause);
                    }
                    return;
                }

                UpdateScheduleRequest.Builder updateRequestBuilder = UpdateScheduleRequest
                    .builder()
                    .groupName(maintenanceScheduleGroup)
                    .name(DAILY_S3_CLEANUP_SCHEDULE_NAME)
                    .state(ScheduleState.DISABLED);

                String existingScheduleExpression = existingSchedule.scheduleExpression();
                FlexibleTimeWindow existingFlexibleWindow = existingSchedule.flexibleTimeWindow();
                Target existingTarget = existingSchedule.target();

                if (existingScheduleExpression == null || existingFlexibleWindow == null || existingTarget == null) {
                    result
                        .completeExceptionally(new IllegalStateException("Existing daily S3 cleanup schedule is missing required fields"));
                    return;
                }

                updateRequestBuilder.scheduleExpression(existingScheduleExpression);
                updateRequestBuilder.flexibleTimeWindow(existingFlexibleWindow);
                updateRequestBuilder.target(existingTarget);

                if (existingSchedule.scheduleExpressionTimezone() != null) {
                    updateRequestBuilder.scheduleExpressionTimezone(existingSchedule.scheduleExpressionTimezone());
                }

                UpdateScheduleRequest updateScheduleRequest = updateRequestBuilder.build();

                schedulerClient.updateSchedule(updateScheduleRequest).whenComplete((response, updateError) -> {
                    if (updateError != null) {
                        result.completeExceptionally(unwrapAsyncException(updateError));
                        return;
                    }
                    logger.info("Disabled AWS Scheduler daily S3 cleanup schedule {}", DAILY_S3_CLEANUP_SCHEDULE_NAME);
                    result.complete(null);
                });
            });

        return result;
    }

    private CompletableFuture<Void> createOrUpdateScheduleAsync(Config config) {
        String configId = config.getId();
        String scheduleName = buildScheduleName(analysisType, config.getTenantId(), config.getId());
        long intervalMinutes = config.getInferredFrequencyInMinutes();
        String scheduleExpression = buildScheduleExpression(intervalMinutes);

        Target target = buildSqsTarget(config, intervalMinutes);
        FlexibleTimeWindow flexibleWindow = FlexibleTimeWindow
            .builder()
            .mode(FlexibleTimeWindowMode.FLEXIBLE)
            .maximumWindowInMinutes(FLEXIBLE_WINDOW_MINUTES)
            .build();

        CreateScheduleRequest createScheduleRequest = CreateScheduleRequest
            .builder()
            .groupName(scheduleGroup)
            .name(scheduleName)
            .scheduleExpression(scheduleExpression)
            .scheduleExpressionTimezone(SCHEDULE_TIMEZONE)
            .state(ScheduleState.ENABLED)
            .description(String.format(Locale.ROOT, "%s schedule for config %s", analysisType, configId))
            .target(target)
            .flexibleTimeWindow(flexibleWindow)
            .build();

        UpdateScheduleRequest updateScheduleRequest = UpdateScheduleRequest
            .builder()
            .groupName(scheduleGroup)
            .name(scheduleName)
            .scheduleExpression(scheduleExpression)
            .scheduleExpressionTimezone(SCHEDULE_TIMEZONE)
            .state(ScheduleState.ENABLED)
            .description(String.format(Locale.ROOT, "%s schedule for config %s", analysisType, configId))
            .target(target)
            .flexibleTimeWindow(flexibleWindow)
            .build();

        CompletableFuture<Void> result = new CompletableFuture<>();
        schedulerClient.createSchedule(createScheduleRequest).whenComplete((response, throwable) -> {
            if (throwable == null) {
                logger
                    .info(
                        "Created AWS Scheduler schedule {} with expression {} targeting queue {} for config {}",
                        response.scheduleArn(),
                        scheduleExpression,
                        sqsQueueArn,
                        configId
                    );
                result.complete(null);
                return;
            }

            Throwable cause = unwrapAsyncException(throwable);
            if (cause instanceof ConflictException) {
                schedulerClient.updateSchedule(updateScheduleRequest).whenComplete((updateResponse, updateError) -> {
                    if (updateError == null) {
                        logger
                            .info(
                                "Updated AWS Scheduler schedule {} with expression {} targeting queue {} for config {}",
                                updateResponse.scheduleArn(),
                                scheduleExpression,
                                sqsQueueArn,
                                configId
                            );
                        result.complete(null);
                    } else {
                        result.completeExceptionally(unwrapAsyncException(updateError));
                    }
                });
            } else {
                result.completeExceptionally(cause);
            }
        });

        return result;
    }

    public void deleteSchedule(String tenantId, String configId) {
        String scheduleName = buildScheduleName(analysisType, tenantId, configId);

        DeleteScheduleRequest deleteScheduleRequest = DeleteScheduleRequest.builder().groupName(scheduleGroup).name(scheduleName).build();

        schedulerClient.deleteSchedule(deleteScheduleRequest).whenComplete((response, throwable) -> {
            if (throwable == null) {
                logger.info("Removed AWS Scheduler schedule {} for config {}", scheduleName, configId);
            } else {
                Throwable cause = unwrapAsyncException(throwable);
                if (cause instanceof ResourceNotFoundException) {
                    logger.info("AWS Scheduler schedule {} not found when removing config {}", scheduleName, configId);
                } else {
                    logger.error("Failed to remove AWS Scheduler schedule {} for config {}", scheduleName, configId, cause);
                }
            }
        });
    }

    private Target buildSqsTarget(Config config, long intervalMinutes) {
        String configId = config.getId();

        return Target
            .builder()
            .arn(sqsQueueArn)
            .roleArn(schedulerRoleArn)
            // message group id is the schedule name which is unique for each schedule
            .sqsParameters(buildSqsParameters(buildScheduleName(analysisType, config.getTenantId(), configId)))
            .input(buildTargetInput(config, intervalMinutes))
            .build();
    }

    static SqsParameters buildSqsParameters(String messageGroupId) {
        return SqsParameters.builder().messageGroupId(messageGroupId).build();
    }

    private String buildTargetInput(Config config, long intervalMinutes) {
        IntervalTimeConfiguration frequency = (IntervalTimeConfiguration) config.getInferredFrequency();
        Schedule jobSchedule = new IntervalSchedule(clock.instant(), (int) frequency.getInterval(), frequency.getUnit());
        Duration duration = Duration.of(frequency.getInterval(), frequency.getUnit());

        Job job = new Job(
            config.getId(),
            jobSchedule,
            config.getWindowDelay(),
            true,
            clock.instant(),
            null,
            clock.instant(),
            duration.getSeconds(),
            config.getUser(),
            config.getTenantId(),
            config.getCustomResultIndexOrAlias(),
            analysisType
        );

        try {
            XContentBuilder builder = job.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            Map<String, Object> jobMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
            jobMap.put(CommonName.EB_SCHEDULED_TIME_FIELD, CommonName.EB_SCHEDULED_TIME_VALUE);

            XContentBuilder decoratedBuilder = JsonXContent.contentBuilder();
            decoratedBuilder.map(jobMap);
            return BytesReference.bytes(decoratedBuilder).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize job payload for EventBridge target", e);
        }
    }

    private String buildScheduleExpression(long minutes) {
        String unitText = minutes == 1 ? "minute" : "minutes";
        return String.format(Locale.ROOT, "rate(%d %s)", minutes, unitText);
    }

    /**
     * Get job from EventBridge scheduler schedule.
     * @param tenantId tenant ID
     * @param configId config ID
     * @return Optional Job if schedule exists and contains valid job data
     */
    public Optional<Job> getJobFromSchedule(String tenantId, String configId) {
        String scheduleName = buildScheduleName(analysisType, tenantId, configId);

        try {
            GetScheduleResponse response = schedulerClient
                .getSchedule(GetScheduleRequest.builder().groupName(scheduleGroup).name(scheduleName).build())
                .join();
            return parseJobFromScheduleResponse(response, scheduleName);
        } catch (CompletionException completionException) {
            Throwable cause = unwrapAsyncException(completionException);
            if (cause instanceof ResourceNotFoundException) {
                logger.debug("Schedule {} not found in EventBridge Scheduler", scheduleName);
            } else {
                logger.error("Failed to get schedule {} from EventBridge Scheduler", scheduleName, cause);
            }
            return Optional.empty();
        }
    }

    public Optional<Job> getMaintenanceJobFromSchedule(String scheduleName) {
        if (!isMaintenanceScheduleName(scheduleName)) {
            return Optional.empty();
        }

        try {
            GetScheduleResponse response = schedulerClient
                .getSchedule(GetScheduleRequest.builder().groupName(maintenanceScheduleGroup).name(scheduleName).build())
                .join();
            return parseJobFromScheduleResponse(response, scheduleName);
        } catch (CompletionException completionException) {
            Throwable cause = unwrapAsyncException(completionException);
            if (cause instanceof ResourceNotFoundException) {
                logger.debug("Maintenance schedule {} not found in EventBridge Scheduler", scheduleName);
            } else {
                logger.error("Failed to get maintenance schedule {} from EventBridge Scheduler", scheduleName, cause);
            }
            return Optional.empty();
        }
    }

    static Optional<Job> parseJobFromScheduleResponse(GetScheduleResponse response, String scheduleName) {
        Target target = response.target();
        if (target == null || target.input() == null) {
            logger.warn("Schedule {} exists but has no target input", scheduleName);
            return Optional.empty();
        }

        String targetInput = target.input();
        logger.debug("Fetched schedule target input: {}", targetInput);

        try (
            XContentParser parser = XContentType.JSON
                .xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE,
                    targetInput
                )
        ) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            Job job = Job.parse(parser);
            return Optional.of(mergeLiveScheduleState(job, response));
        } catch (Exception e) {
            logger.error("Failed to parse job from schedule {} target input", scheduleName, e);
            return Optional.empty();
        }
    }

    // Scheduler target input stores the serialized job payload, but the scheduler state is authoritative.
    private static Job mergeLiveScheduleState(Job job, GetScheduleResponse response) {
        ScheduleState scheduleState = response.state();
        boolean enabled = job.isEnabled();
        if (ScheduleState.ENABLED.equals(scheduleState)) {
            enabled = true;
        } else if (ScheduleState.DISABLED.equals(scheduleState)) {
            enabled = false;
        }

        Instant enabledTime = job.getEnabledTime();
        Instant disabledTime = job.getDisabledTime();
        Instant lastUpdateTime = job.getLastUpdateTime();
        Instant creationDate = response.creationDate();
        Instant lastModificationDate = response.lastModificationDate();

        if (ScheduleState.ENABLED.equals(scheduleState)) {
            disabledTime = null;
            if (enabledTime == null) {
                enabledTime = creationDate != null ? creationDate : lastModificationDate;
            }
        } else if (ScheduleState.DISABLED.equals(scheduleState) && lastModificationDate != null) {
            disabledTime = lastModificationDate;
        }

        if (lastModificationDate != null) {
            lastUpdateTime = lastModificationDate;
        } else if (lastUpdateTime == null) {
            lastUpdateTime = creationDate != null ? creationDate : enabledTime;
        }

        return new Job(
            job.getName(),
            job.getSchedule(),
            job.getWindowDelay(),
            enabled,
            enabledTime,
            disabledTime,
            lastUpdateTime,
            job.getLockDurationSeconds(),
            job.getUser(),
            job.getTenantId(),
            job.getCustomResultIndexOrAlias(),
            job.getAnalysisType()
        );
    }

    private CompletableFuture<Void> disableScheduleAsync(String configId, String tenantId) {
        String scheduleName = buildScheduleName(analysisType, tenantId, configId);
        CompletableFuture<Void> result = new CompletableFuture<>();

        schedulerClient
            .getSchedule(GetScheduleRequest.builder().groupName(scheduleGroup).name(scheduleName).build())
            .whenComplete((existingSchedule, throwable) -> {
                if (throwable != null) {
                    Throwable cause = unwrapAsyncException(throwable);
                    if (cause instanceof ResourceNotFoundException) {
                        logger.info("AWS Scheduler schedule {} not found when disabling config {}", scheduleName, configId);
                        result.complete(null);
                    } else {
                        result.completeExceptionally(cause);
                    }
                    return;
                }

                UpdateScheduleRequest.Builder updateRequestBuilder = UpdateScheduleRequest
                    .builder()
                    .groupName(scheduleGroup)
                    .name(scheduleName)
                    .state(ScheduleState.DISABLED);

                String existingScheduleExpression = existingSchedule.scheduleExpression();
                FlexibleTimeWindow existingFlexibleWindow = existingSchedule.flexibleTimeWindow();
                Target existingTarget = existingSchedule.target();

                if (existingScheduleExpression == null || existingFlexibleWindow == null || existingTarget == null) {
                    result
                        .completeExceptionally(
                            new IllegalStateException("Existing schedule is missing required fields for config " + configId)
                        );
                    return;
                }

                updateRequestBuilder.scheduleExpression(existingScheduleExpression);
                updateRequestBuilder.flexibleTimeWindow(existingFlexibleWindow);
                updateRequestBuilder.target(existingTarget);

                if (existingSchedule.scheduleExpressionTimezone() != null) {
                    updateRequestBuilder.scheduleExpressionTimezone(existingSchedule.scheduleExpressionTimezone());
                }

                UpdateScheduleRequest updateScheduleRequest = updateRequestBuilder.build();

                schedulerClient.updateSchedule(updateScheduleRequest).whenComplete((response, updateError) -> {
                    if (updateError != null) {
                        result.completeExceptionally(unwrapAsyncException(updateError));
                        return;
                    }
                    logger.info("Disabled AWS Scheduler schedule {} for config {}", scheduleName, configId);
                    result.complete(null);
                });
            });

        return result;
    }

    private Throwable unwrapAsyncException(Throwable throwable) {
        Throwable current = throwable;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    public static String buildScheduleName(AnalysisType analysisType, String tenantId, String configId) {
        String sanitizedConfigId = StringUtil.sanitizeId(configId);
        String analysisPrefix = analysisType.name().toLowerCase(Locale.ROOT);
        if (Strings.isBlank(tenantId)) {
            return String.format(Locale.ROOT, "%s-%s", analysisPrefix, sanitizedConfigId);
        }
        return String.format(Locale.ROOT, "%s-%s-%s", analysisPrefix, buildShortHash(tenantId), sanitizedConfigId);
    }

    /**
     * Build a short hash of the value using MurmurHash3.
     * 
     * shortHash is 16 hexadecimal characters.
     * 
     * String.format("%016x", hash.h1) formats one 64-bit long as lowercase hex, zero-padded to width 16.
     * Since each hex character represents 4 bits, that is 64 bits total.
     * 
     * Two implications:
     * 1. Different tenant IDs can theoretically collide, though with 64 bits the chance is low for modest scale.
     * 2. It is only suitable for compact identification, not recovery.
     * 
     * Example:
     * 
     * String value = "app-1:data-source-1";
     * String shortHash = buildShortHash(value);
     * System.out.println(shortHash); // Output: "5543123456789012"
     * 
     * @param value the value to hash
     * @return the short hash of the value in hexadecimal format
     */
    private static String buildShortHash(String value) {
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(valueBytes, 0, valueBytes.length, TENANT_HASH_SEED, new MurmurHash3.Hash128());
        return String.format(Locale.ROOT, "%016x", hash.h1);
    }
}
