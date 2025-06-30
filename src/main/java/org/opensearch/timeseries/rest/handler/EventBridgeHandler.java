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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
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
    private static final String HOURLY_CRON_DESCRIPTION = "Hourly maintenance and cleanup";
    private static final String HOURLY_CRON_SCHEDULE_EXPRESSION = "rate(1 hour)";
    private static final String DAILY_S3_CLEANUP_SCHEDULE_NAME = "DailyS3CheckpointCleanup";
    private static final String DAILY_S3_CLEANUP_DESCRIPTION = "Daily S3 checkpoint cleanup";
    private static final String DAILY_S3_CLEANUP_SCHEDULE_EXPRESSION = "rate(24 hours)";
    private final SchedulerAsyncClient schedulerClient;
    private final String sqsQueueArn;
    private final String schedulerRoleArn;
    private final AnalysisType analysisType;
    private final String scheduleGroup;
    private final String maintenanceScheduleGroup;
    private final Clock clock;

    /**
     * Constructor function.
     *
     * @param region AWS region for EventBridge Scheduler
     * @param sqsQueueArn ARN of the FIFO SQS queue that receives scheduled job triggers
     * @param schedulerRoleArn IAM role ARN assumed by EventBridge Scheduler to publish to SQS
     * @param configuredScheduleGroup optional AWS Scheduler group name override
     * @param analysisType Analysis type (AD or FORECAST)
     * @param clock Clock to get current time
     */
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "java.security.AccessController usage: required for privileged AWS client initialization.")
    public EventBridgeHandler(
        String region,
        String sqsQueueArn,
        String schedulerRoleArn,
        String configuredScheduleGroup,
        AnalysisType analysisType,
        Clock clock
    ) {
        this.schedulerClient = AccessController
            .doPrivileged(
                (PrivilegedAction<SchedulerAsyncClient>) () -> SchedulerAsyncClient
                    .builder()
                    .region(Region.of(region))
                    .credentialsProvider(SecurityUtil.createCredentialsProvider())
                    .build()
            );
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
     * Start hourly cron via EventBridge Scheduler.
     * Creates or updates the hourly maintenance schedule that runs every hour.
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
     * Stop hourly cron via EventBridge Scheduler.
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
     * Creates or updates the daily S3 checkpoint cleanup schedule that runs every 24 hours.
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
            .scheduleExpression(HOURLY_CRON_SCHEDULE_EXPRESSION)
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
            .scheduleExpression(HOURLY_CRON_SCHEDULE_EXPRESSION)
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
                    if (existingSchedule != null && ScheduleState.ENABLED.equals(existingSchedule.state())) {
                        logger.info("Hourly cron schedule already enabled; treating as success.");
                        result.complete(null);
                        return;
                    }
                    updateHourlyCronSchedule(updateScheduleRequest, result);
                    return;
                }

                Throwable cause = unwrapAsyncException(getError);
                if (cause instanceof ResourceNotFoundException) {
                    createHourlyCronSchedule(createScheduleRequest, updateScheduleRequest, result);
                } else {
                    result.completeExceptionally(cause);
                }
            });

        return result;
    }

    private void createHourlyCronSchedule(
        CreateScheduleRequest createScheduleRequest,
        UpdateScheduleRequest updateScheduleRequest,
        CompletableFuture<Void> result
    ) {
        schedulerClient.createSchedule(createScheduleRequest).whenComplete((response, throwable) -> {
            if (throwable == null) {
                logger
                    .info(
                        "Created AWS Scheduler hourly cron schedule {} with expression {} targeting queue {}",
                        response.scheduleArn(),
                        HOURLY_CRON_SCHEDULE_EXPRESSION,
                        sqsQueueArn
                    );
                result.complete(null);
                return;
            }

            Throwable cause = unwrapAsyncException(throwable);
            if (cause instanceof ConflictException) {
                updateHourlyCronSchedule(updateScheduleRequest, result);
            } else {
                result.completeExceptionally(cause);
            }
        });
    }

    private void updateHourlyCronSchedule(UpdateScheduleRequest updateScheduleRequest, CompletableFuture<Void> result) {
        schedulerClient.updateSchedule(updateScheduleRequest).whenComplete((updateResponse, updateError) -> {
            if (updateError == null) {
                logger
                    .info(
                        "Updated AWS Scheduler hourly cron schedule {} with expression {} targeting queue {}",
                        updateResponse.scheduleArn(),
                        HOURLY_CRON_SCHEDULE_EXPRESSION,
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
                                logger.info("Hourly cron schedule already enabled; treating as success.");
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
            // No messageGroupId for hourly cron - it doesn't need FIFO ordering per config
            .sqsParameters(SqsParameters.builder().build())
            .input(buildHourlyCronTargetInput())
            .build();
    }

    private String buildHourlyCronTargetInput() {
        Job job = new Job(
            HOURLY_CRON_SCHEDULE_NAME,
            new IntervalSchedule(clock.instant(), 1, java.time.temporal.ChronoUnit.HOURS),
            null, // no window delay
            true,
            clock.instant(),
            null,
            clock.instant(),
            3600L, // 1 hour lock duration
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
            .scheduleExpression(DAILY_S3_CLEANUP_SCHEDULE_EXPRESSION)
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
            .scheduleExpression(DAILY_S3_CLEANUP_SCHEDULE_EXPRESSION)
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
                    if (existingSchedule != null && ScheduleState.ENABLED.equals(existingSchedule.state())) {
                        logger.info("Daily S3 checkpoint cleanup schedule already enabled; treating as success.");
                        result.complete(null);
                        return;
                    }
                    updateDailyS3CleanupSchedule(updateScheduleRequest, result);
                    return;
                }

                Throwable cause = unwrapAsyncException(getError);
                if (cause instanceof ResourceNotFoundException) {
                    createDailyS3CleanupSchedule(createScheduleRequest, updateScheduleRequest, result);
                } else {
                    result.completeExceptionally(cause);
                }
            });

        return result;
    }

    private void createDailyS3CleanupSchedule(
        CreateScheduleRequest createScheduleRequest,
        UpdateScheduleRequest updateScheduleRequest,
        CompletableFuture<Void> result
    ) {
        schedulerClient.createSchedule(createScheduleRequest).whenComplete((response, throwable) -> {
            if (throwable == null) {
                logger
                    .info(
                        "Created AWS Scheduler daily S3 cleanup schedule {} with expression {} targeting queue {}",
                        response.scheduleArn(),
                        DAILY_S3_CLEANUP_SCHEDULE_EXPRESSION,
                        sqsQueueArn
                    );
                result.complete(null);
                return;
            }

            Throwable cause = unwrapAsyncException(throwable);
            if (cause instanceof ConflictException) {
                updateDailyS3CleanupSchedule(updateScheduleRequest, result);
            } else {
                result.completeExceptionally(cause);
            }
        });
    }

    private void updateDailyS3CleanupSchedule(UpdateScheduleRequest updateScheduleRequest, CompletableFuture<Void> result) {
        schedulerClient.updateSchedule(updateScheduleRequest).whenComplete((updateResponse, updateError) -> {
            if (updateError == null) {
                logger
                    .info(
                        "Updated AWS Scheduler daily S3 cleanup schedule {} with expression {} targeting queue {}",
                        updateResponse.scheduleArn(),
                        DAILY_S3_CLEANUP_SCHEDULE_EXPRESSION,
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
            // No messageGroupId for daily S3 cleanup - it doesn't need FIFO ordering per config
            .sqsParameters(SqsParameters.builder().build())
            .input(buildDailyS3CleanupTargetInput())
            .build();
    }

    private String buildDailyS3CleanupTargetInput() {
        Job job = new Job(
            DAILY_S3_CLEANUP_SCHEDULE_NAME,
            new IntervalSchedule(clock.instant(), 24, java.time.temporal.ChronoUnit.HOURS),
            null, // no window delay
            true,
            clock.instant(),
            null,
            clock.instant(),
            86400L, // 24 hour lock duration
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
            .sqsParameters(SqsParameters.builder().messageGroupId(buildScheduleName(analysisType, config.getTenantId(), configId)).build())
            .input(buildTargetInput(config, intervalMinutes))
            .build();
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

            Target target = response.target();
            if (target == null || target.input() == null) {
                logger.warn("Schedule {} exists but has no target input", scheduleName);
                return Optional.empty();
            }

            String targetInput = target.input();
            logger.debug("Fetched schedule target input: {}", targetInput);

            // Parse the job from the target input JSON
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
                return Optional.of(job);
            } catch (Exception e) {
                logger.error("Failed to parse job from schedule {} target input", scheduleName, e);
                return Optional.empty();
            }
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
        return String
            .format(
                Locale.ROOT,
                "%s-%s-%s",
                analysisType.name().toLowerCase(Locale.ROOT),
                StringUtil.sanitizeId(tenantId),
                StringUtil.sanitizeId(configId)
            );
    }
}
