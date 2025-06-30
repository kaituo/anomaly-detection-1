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

package org.opensearch.timeseries.rest.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.Optional;
import java.util.regex.Pattern;

import org.mockito.ArgumentCaptor;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.JobResponse;

import software.amazon.awssdk.services.scheduler.SchedulerAsyncClient;
import software.amazon.awssdk.services.scheduler.model.GetScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.GetScheduleResponse;
import software.amazon.awssdk.services.scheduler.model.ScheduleState;
import software.amazon.awssdk.services.scheduler.model.SqsParameters;
import software.amazon.awssdk.services.scheduler.model.Target;
import software.amazon.awssdk.services.scheduler.model.UpdateScheduleRequest;
import software.amazon.awssdk.services.scheduler.model.UpdateScheduleResponse;

public class EventBridgeHandlerTests extends AbstractTimeSeriesTest {
    private static final Pattern AD_SCHEDULE_NAME_PATTERN = Pattern.compile("^ad-[0-9a-f]{16}-[A-Za-z0-9_-]+$");

    public void testResolveConfigScheduleGroupFallsBackToAnalysisTypeName() {
        assertEquals("ad", EventBridgeHandler.resolveConfigScheduleGroup(null, AnalysisType.AD));
        assertEquals("forecast", EventBridgeHandler.resolveConfigScheduleGroup("   ", AnalysisType.FORECAST));
    }

    public void testResolveConfigScheduleGroupUsesConfiguredValue() {
        assertEquals("existing-group", EventBridgeHandler.resolveConfigScheduleGroup(" existing-group ", AnalysisType.AD));
    }

    public void testResolveMaintenanceScheduleGroupFallsBackToTimeseries() {
        assertEquals("timeseries", EventBridgeHandler.resolveMaintenanceScheduleGroup(null));
        assertEquals("timeseries", EventBridgeHandler.resolveMaintenanceScheduleGroup("   "));
    }

    public void testResolveMaintenanceScheduleGroupUsesConfiguredValue() {
        assertEquals("existing-group", EventBridgeHandler.resolveMaintenanceScheduleGroup(" existing-group "));
    }

    public void testResolveMaintenanceScheduleIntervalDefaultsToHourly() {
        assertEquals(Duration.ofHours(1), EventBridgeHandler.resolveMaintenanceScheduleInterval(Duration.ofHours(12)));
    }

    public void testResolveMaintenanceScheduleIntervalAllowsMinuteCadenceForTesting() {
        assertEquals(Duration.ofMinutes(1), EventBridgeHandler.resolveMaintenanceScheduleInterval(Duration.ofMinutes(1)));
    }

    public void testBuildMaintenanceScheduleExpressionUsesSingularUnits() {
        assertEquals("rate(1 minute)", EventBridgeHandler.buildMaintenanceScheduleExpression(Duration.ofMinutes(1)));
        assertEquals("rate(1 hour)", EventBridgeHandler.buildMaintenanceScheduleExpression(Duration.ofHours(1)));
    }

    public void testResolveDailyS3CleanupIntervalRoundsUpToWholeMinutes() {
        assertEquals(Duration.ofMinutes(1), EventBridgeHandler.resolveDailyS3CleanupInterval(Duration.ofSeconds(1)));
        assertEquals(Duration.ofMinutes(61), EventBridgeHandler.resolveDailyS3CleanupInterval(Duration.ofMinutes(61)));
    }

    public void testBuildDailyS3CleanupScheduleExpressionSupportsMinuteCadenceForTesting() {
        assertEquals("rate(1 minute)", EventBridgeHandler.buildDailyS3CleanupScheduleExpression(Duration.ofMinutes(1)));
        assertEquals("rate(2 hours)", EventBridgeHandler.buildDailyS3CleanupScheduleExpression(Duration.ofHours(2)));
    }

    public void testStartDailyS3CheckpointCleanupUpdatesEnabledSchedule() {
        SchedulerAsyncClient schedulerClient = mock(SchedulerAsyncClient.class);
        when(schedulerClient.getSchedule(any(GetScheduleRequest.class))).thenReturn(
            CompletableFuture.completedFuture(
                GetScheduleResponse.builder().name("DailyS3CheckpointCleanup").state(ScheduleState.ENABLED).build()
            )
        );

        ArgumentCaptor<UpdateScheduleRequest> updateCaptor = ArgumentCaptor.forClass(UpdateScheduleRequest.class);
        when(schedulerClient.updateSchedule(updateCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(UpdateScheduleResponse.builder().scheduleArn("arn:test").build()));

        EventBridgeHandler handler = new EventBridgeHandler(
            schedulerClient,
            "arn:aws:sqs:us-west-2:123456789012:test.fifo",
            "arn:aws:iam::123456789012:role/TestSchedulerRole",
            "ad",
            AnalysisType.AD,
            Duration.ofHours(1),
            Duration.ofHours(24),
            Clock.systemUTC()
        ) {};
        handler.setDailyS3CleanupInterval(Duration.ofMinutes(1));

        CompletableFuture<JobResponse> future = new CompletableFuture<>();
        handler.startDailyS3CheckpointCleanup(ActionListener.wrap(future::complete, future::completeExceptionally));

        assertEquals("DailyS3CheckpointCleanup", future.join().getId());
        assertEquals("rate(1 minute)", updateCaptor.getValue().scheduleExpression());
        verify(schedulerClient).updateSchedule(any(UpdateScheduleRequest.class));
    }

    public void testBuildSqsParametersUsesScheduleNameAsMessageGroupId() {
        SqsParameters parameters = EventBridgeHandler.buildSqsParameters("HourlyCron");

        assertEquals("HourlyCron", parameters.messageGroupId());
    }

    public void testBuildScheduleNameUsesStableTenantHash() {
        String tenantId = "app-1:data-source-1";
        String configId = "detector-1";

        String first = EventBridgeHandler.buildScheduleName(AnalysisType.AD, tenantId, configId);
        String second = EventBridgeHandler.buildScheduleName(AnalysisType.AD, tenantId, configId);

        assertEquals(first, second);
        assertTrue(AD_SCHEDULE_NAME_PATTERN.matcher(first).matches());
    }

    public void testBuildScheduleNameUsesDifferentHashesForDifferentTenants() {
        String configId = "detector-1";

        String first = EventBridgeHandler.buildScheduleName(AnalysisType.AD, "app-1:data-source-1", configId);
        String second = EventBridgeHandler.buildScheduleName(AnalysisType.AD, "app-2:data-source-1", configId);

        assertNotEquals(first, second);
        assertTrue(first.endsWith("-" + configId));
        assertTrue(second.endsWith("-" + configId));
    }

    public void testBuildScheduleNameSanitizesConfigIdInSuffix() {
        String scheduleName = EventBridgeHandler.buildScheduleName(AnalysisType.AD, "app-1:data-source-1", "detector:id.with spaces");

        assertTrue(scheduleName.endsWith("-detector_id_with_spaces"));
        assertTrue(AD_SCHEDULE_NAME_PATTERN.matcher(scheduleName).matches());
    }

    public void testBuildScheduleNameFallsBackWhenTenantIdMissing() {
        assertEquals("ad-detector_1", EventBridgeHandler.buildScheduleName(AnalysisType.AD, null, "detector:1"));
        assertEquals("forecast-detector_1", EventBridgeHandler.buildScheduleName(AnalysisType.FORECAST, "   ", "detector:1"));
    }

    public void testBuildScheduleNameStaysWithinLimitForLongTenantId() {
        String tenantId = randomAlphaOfLength(80) + ":" + randomAlphaOfLength(80);
        String configId = randomAlphaOfLength(20);

        String scheduleName = EventBridgeHandler.buildScheduleName(AnalysisType.AD, tenantId, configId);

        assertTrue(scheduleName.length() <= 64);
        assertTrue(AD_SCHEDULE_NAME_PATTERN.matcher(scheduleName).matches());
    }

    public void testParseJobFromScheduleResponseKeepsEnabledScheduleEnabled() throws Exception {
        Instant enabledTime = Instant.parse("2026-04-07T01:02:03Z");
        Instant modifiedTime = Instant.parse("2026-04-07T01:07:03Z");
        Job job = createJob(enabledTime);

        GetScheduleResponse response = GetScheduleResponse
            .builder()
            .name(EventBridgeHandler.buildScheduleName(AnalysisType.AD, "tenant-a", "detector-1"))
            .state(ScheduleState.ENABLED)
            .creationDate(enabledTime)
            .lastModificationDate(modifiedTime)
            .target(Target.builder().input(serializeJob(job)).build())
            .build();

        Optional<Job> parsed = EventBridgeHandler.parseJobFromScheduleResponse(response, response.name());

        assertTrue(parsed.isPresent());
        assertEquals("detector-1", parsed.get().getName());
        assertTrue(parsed.get().isEnabled());
        assertEquals(enabledTime, parsed.get().getEnabledTime());
        assertNull(parsed.get().getDisabledTime());
        assertEquals(modifiedTime, parsed.get().getLastUpdateTime());
    }

    public void testParseJobFromScheduleResponseReflectsDisabledScheduleState() throws Exception {
        Instant enabledTime = Instant.parse("2026-04-07T01:02:03Z");
        Instant disabledTime = Instant.parse("2026-04-07T01:17:03Z");
        Job job = createJob(enabledTime);

        GetScheduleResponse response = GetScheduleResponse
            .builder()
            .name(EventBridgeHandler.buildScheduleName(AnalysisType.AD, "tenant-a", "detector-1"))
            .state(ScheduleState.DISABLED)
            .creationDate(enabledTime)
            .lastModificationDate(disabledTime)
            .target(Target.builder().input(serializeJob(job)).build())
            .build();

        Optional<Job> parsed = EventBridgeHandler.parseJobFromScheduleResponse(response, response.name());

        assertTrue(parsed.isPresent());
        assertEquals("detector-1", parsed.get().getName());
        assertFalse(parsed.get().isEnabled());
        assertEquals(enabledTime, parsed.get().getEnabledTime());
        assertEquals(disabledTime, parsed.get().getDisabledTime());
        assertEquals(disabledTime, parsed.get().getLastUpdateTime());
    }

    private Job createJob(Instant enabledTime) {
        return new Job(
            "detector-1",
            new IntervalSchedule(enabledTime, 1, ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(10, ChronoUnit.SECONDS),
            true,
            enabledTime,
            null,
            enabledTime,
            60L,
            null,
            "tenant-a",
            "custom-result-index",
            AnalysisType.AD
        );
    }

    private String serializeJob(Job job) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        job.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return BytesReference.bytes(builder).utf8ToString();
    }
}
