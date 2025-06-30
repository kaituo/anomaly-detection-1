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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.timeseries.util.StringUtil;
import org.opensearch.transport.TransportService;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;

import org.opensearch.timeseries.constant.CommonName;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.scheduler.model.ConflictException;
import software.amazon.awssdk.services.scheduler.model.CreateScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.CreateScheduleResponse;
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
import software.amazon.awssdk.services.scheduler.model.UpdateScheduleResponse;

/**
 * EventBridge handler to process job events via AWS EventBridge Scheduler API.
 */
public abstract class EventBridgeHandler {

    private static final Logger logger = LogManager.getLogger(EventBridgeHandler.class);
    private static final String SCHEDULE_TIMEZONE = "UTC";
    private static final int FLEXIBLE_WINDOW_MINUTES = 1;
    private final SchedulerClient schedulerClient;
    private final String sqsQueueArn;
    private final String schedulerRoleArn;
    private final AnalysisType analysisType;
    private final String scheduleGroup;
    private final Clock clock;
    /**
     * Constructor function.
     *
     * @param region AWS region for EventBridge Scheduler
     * @param sqsQueueArn ARN of the FIFO SQS queue that receives scheduled job triggers
     * @param schedulerRoleArn IAM role ARN assumed by EventBridge Scheduler to publish to SQS
     * @param analysisType Analysis type (AD or FORECAST)
     * @param clock Clock to get current time
     */
    public EventBridgeHandler(String region, String sqsQueueArn, String schedulerRoleArn, AnalysisType analysisType, Clock clock) {
        this.schedulerClient = AccessController.doPrivileged(
            (PrivilegedAction<SchedulerClient>) () -> SchedulerClient
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
        this.scheduleGroup = analysisType.name().toLowerCase(Locale.ROOT);
        this.clock = clock;
    }

    /**
     * Start job via EventBridge Scheduler.
     * 1. If job doesn't exist, create new job schedule.
     * 2. If job exists: a). if job enabled, update job schedule; b). if job disabled, enable job schedule.
     * @param config config accessor
     * @param transportService transport service
     * @param clock clock to get current time
     * @param listener Listener to send responses
     */
    public void startJob(Config config, TransportService transportService, Clock clock, ActionListener<JobResponse> listener) {
        try {
            createOrUpdateSchedule(config);
            listener.onResponse(new JobResponse(config.getId()));
        } catch (Exception ex) {
            logger.error("Failed to start job via EventBridge Scheduler for config: " + config.getId(), ex);
            listener.onFailure(ex);
        }
    }

    /**
     * Stop job via EventBridge Scheduler.
     * @param configId config identifier
     * @param transportService transport service
     * @param listener Listener to send responses
     */
    public void stopJob(String configId, TransportService transportService, ActionListener<JobResponse> listener) {
        try {
            disableSchedule(configId, null);
            listener.onResponse(new JobResponse(configId));
        } catch (Exception ex) {
            logger.error("Failed to stop job via EventBridge Scheduler for config: " + configId, ex);
            listener.onFailure(ex);
        }
    }

    /**
     * Close the EventBridge Scheduler client.
     */
    public void close() {
        if (schedulerClient != null) {
            schedulerClient.close();
        }
    }

    private void createOrUpdateSchedule(Config config) {
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

        try {
            CreateScheduleResponse response = schedulerClient.createSchedule(createScheduleRequest);
            logger.info(
                "Created AWS Scheduler schedule {} with expression {} targeting queue {} for config {}",
                response.scheduleArn(),
                scheduleExpression,
                sqsQueueArn,
                configId
            );
        } catch (ConflictException conflictException) {
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

            UpdateScheduleResponse updateResponse = schedulerClient.updateSchedule(updateScheduleRequest);
            logger.info(
                "Updated AWS Scheduler schedule {} with expression {} targeting queue {} for config {}",
                updateResponse.scheduleArn(),
                scheduleExpression,
                sqsQueueArn,
                configId
            );
        }
    }

    public void deleteSchedule(String tenantId, String configId) {
        String scheduleName = buildScheduleName(analysisType, tenantId, configId);

        DeleteScheduleRequest deleteScheduleRequest = DeleteScheduleRequest
            .builder()
            .groupName(scheduleGroup)
            .name(scheduleName)
            .build();

        try {
            schedulerClient.deleteSchedule(deleteScheduleRequest);
            logger.info("Removed AWS Scheduler schedule {} for config {}", scheduleName, configId);
        } catch (ResourceNotFoundException notFoundException) {
            logger.info("AWS Scheduler schedule {} not found when removing config {}", scheduleName, configId);
        }
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
            Map<String, Object> jobMap = XContentHelper
                    .convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
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
            GetScheduleResponse response = schedulerClient.getSchedule(
                GetScheduleRequest.builder().groupName(scheduleGroup).name(scheduleName).build()
            );

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
        } catch (ResourceNotFoundException notFoundException) {
            logger.debug("Schedule {} not found in EventBridge Scheduler", scheduleName);
            return Optional.empty();
        } catch (Exception e) {
            logger.error("Failed to get schedule {} from EventBridge Scheduler", scheduleName, e);
            return Optional.empty();
        }
    }

    private void disableSchedule(String configId, String tenantId) {
        String scheduleName = buildScheduleName(analysisType, tenantId, configId);

        try {
            GetScheduleResponse existingSchedule = schedulerClient.getSchedule(
                GetScheduleRequest.builder().groupName(scheduleGroup).name(scheduleName).build()
            );

            UpdateScheduleRequest.Builder updateRequestBuilder = UpdateScheduleRequest
                .builder()
                .groupName(scheduleGroup)
                .name(scheduleName)
                .state(ScheduleState.DISABLED);

            String existingScheduleExpression = existingSchedule.scheduleExpression();
            FlexibleTimeWindow existingFlexibleWindow = existingSchedule.flexibleTimeWindow();
            Target existingTarget = existingSchedule.target();

            if (existingScheduleExpression == null || existingFlexibleWindow == null || existingTarget == null) {
                throw new IllegalStateException("Existing schedule is missing required fields for config " + configId);
            }

            updateRequestBuilder.scheduleExpression(existingScheduleExpression);
            updateRequestBuilder.flexibleTimeWindow(existingFlexibleWindow);
            updateRequestBuilder.target(existingTarget);

            if (existingSchedule.scheduleExpressionTimezone() != null) {
                updateRequestBuilder.scheduleExpressionTimezone(existingSchedule.scheduleExpressionTimezone());
            }

            UpdateScheduleRequest updateScheduleRequest = updateRequestBuilder.build();

            schedulerClient.updateSchedule(updateScheduleRequest);
            logger.info("Disabled AWS Scheduler schedule {} for config {}", scheduleName, configId);
        } catch (ResourceNotFoundException notFoundException) {
            logger.info("AWS Scheduler schedule {} not found when disabling config {}", scheduleName, configId);
        }
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
