/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.CheckpointDaoInterface;
import org.opensearch.timeseries.model.Job;

public class SQSConsumerTaskTests extends AbstractTimeSeriesTest {

    private static final String DAILY_S3_CLEANUP_SCHEDULE_NAME = "DailyS3CheckpointCleanup";

    private StateManager nodeStateManager;
    private CheckpointDaoInterface<?> checkpointStore;
    private Runnable retentionTask;
    private Clock clock;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        nodeStateManager = mock(StateManager.class);
        checkpointStore = mock(CheckpointDaoInterface.class);
        retentionTask = mock(Runnable.class);
        clock = Clock.fixed(Instant.parse("2026-04-13T18:14:00Z"), ZoneOffset.UTC);
    }

    public void testDailyS3CheckpointCleanupUsesLatestCheckpointTtl() throws Exception {
        AtomicReference<Duration> checkpointTtl = new AtomicReference<>(Duration.ofDays(7));
        Job job = buildDailyCleanupJob(clock.instant());
        String messageBody = buildMessageBody(job, clock.instant());

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Optional<Job>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(job));
            return null;
        }).when(nodeStateManager).getJob(anyString(), any(), anyBoolean(), any());

        when(checkpointStore.createRetentionTask(any(), any())).thenReturn(retentionTask);

        SQSConsumerTask.DefaultSQSMessageHandler handler = new SQSConsumerTask.DefaultSQSMessageHandler(
            nodeStateManager,
            new ConcurrentHashMap<>(),
            org.opensearch.common.settings.Settings.EMPTY,
            clock,
            checkpointStore,
            checkpointTtl::get
        );

        checkpointTtl.set(Duration.ofMinutes(1));

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        handler.processMessage(messageBody, ActionListener.wrap(future::complete, future::completeExceptionally));
        assertTrue(future.get(5, TimeUnit.SECONDS));

        verify(checkpointStore).createRetentionTask(Duration.ofMinutes(1), clock);
        verify(retentionTask).run();
    }

    public void testProcessMessageUsesFreshJobStateAndSkipsDisabledJob() throws Exception {
        Job messageJob = buildDailyCleanupJob(clock.instant());
        Job disabledJob = buildDailyCleanupJob(clock.instant(), false);
        String messageBody = buildMessageBody(messageJob, clock.instant());

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Optional<Job>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(disabledJob));
            return null;
        }).when(nodeStateManager).getJob(anyString(), any(), anyBoolean(), any());

        SQSConsumerTask.DefaultSQSMessageHandler handler = new SQSConsumerTask.DefaultSQSMessageHandler(
            nodeStateManager,
            new ConcurrentHashMap<>(),
            org.opensearch.common.settings.Settings.EMPTY,
            clock,
            checkpointStore,
            () -> Duration.ofDays(7)
        );

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        handler.processMessage(messageBody, ActionListener.wrap(future::complete, future::completeExceptionally));
        assertTrue(future.get(5, TimeUnit.SECONDS));

        verify(nodeStateManager).getJob(eq(DAILY_S3_CLEANUP_SCHEDULE_NAME), any(), eq(false), any());
        verify(checkpointStore, never()).createRetentionTask(any(), any());
    }

    public void testProcessMessageRetriesWhenLatestJobStateLookupFails() throws Exception {
        Job job = buildDailyCleanupJob(clock.instant());
        String messageBody = buildMessageBody(job, clock.instant());

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Optional<Job>> listener = invocation.getArgument(3);
            listener.onFailure(new RuntimeException("state lookup failed"));
            return null;
        }).when(nodeStateManager).getJob(anyString(), any(), anyBoolean(), any());

        SQSConsumerTask.DefaultSQSMessageHandler handler = new SQSConsumerTask.DefaultSQSMessageHandler(
            nodeStateManager,
            new ConcurrentHashMap<>(),
            org.opensearch.common.settings.Settings.EMPTY,
            clock,
            checkpointStore,
            () -> Duration.ofDays(7)
        );

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        handler.processMessage(messageBody, ActionListener.wrap(future::complete, future::completeExceptionally));
        assertFalse(future.get(5, TimeUnit.SECONDS));

        verify(nodeStateManager).getJob(eq(DAILY_S3_CLEANUP_SCHEDULE_NAME), any(), eq(false), any());
        verify(checkpointStore, never()).createRetentionTask(any(), any());
    }

    public void testGroupSemaphoreSafetyReleaseUsesLockDuration() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        ScheduledCancellable cancellable = mock(ScheduledCancellable.class);
        ArgumentCaptor<Runnable> releaseCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<TimeValue> delayCaptor = ArgumentCaptor.forClass(TimeValue.class);
        when(threadPool.schedule(releaseCaptor.capture(), delayCaptor.capture(), eq(CommonName.SQS_CONSUMER_THREAD_POOL_NAME)))
            .thenReturn(cancellable);

        ConcurrentHashMap<String, Semaphore> groupSemaphores = new ConcurrentHashMap<>();
        LockService lockService = createGroupSemaphoreLockService(groupSemaphores, "group-1", threadPool);
        Job job = buildDailyCleanupJob(clock.instant());
        JobExecutionContext context = createJobExecutionContext(lockService, job);

        CompletableFuture<LockModel> lockFuture = new CompletableFuture<>();
        lockService.acquireLock(job, context, ActionListener.wrap(lockFuture::complete, lockFuture::completeExceptionally));

        assertNotNull(lockFuture.get(5, TimeUnit.SECONDS));
        assertEquals(Duration.ofMinutes(1).toMillis(), delayCaptor.getValue().getMillis());
        assertEquals(0, groupSemaphores.get("group-1").availablePermits());

        releaseCaptor.getValue().run();

        assertEquals(1, groupSemaphores.get("group-1").availablePermits());
    }

    public void testExplicitLockReleaseCancelsSafetyReleaseAndDoesNotDoubleRelease() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        ScheduledCancellable cancellable = mock(ScheduledCancellable.class);
        ArgumentCaptor<Runnable> releaseCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(threadPool.schedule(releaseCaptor.capture(), any(), eq(CommonName.SQS_CONSUMER_THREAD_POOL_NAME))).thenReturn(cancellable);

        ConcurrentHashMap<String, Semaphore> groupSemaphores = new ConcurrentHashMap<>();
        LockService lockService = createGroupSemaphoreLockService(groupSemaphores, "group-1", threadPool);
        Job job = buildDailyCleanupJob(clock.instant());
        JobExecutionContext context = createJobExecutionContext(lockService, job);

        CompletableFuture<LockModel> lockFuture = new CompletableFuture<>();
        lockService.acquireLock(job, context, ActionListener.wrap(lockFuture::complete, lockFuture::completeExceptionally));
        LockModel lock = lockFuture.get(5, TimeUnit.SECONDS);
        assertEquals(0, groupSemaphores.get("group-1").availablePermits());

        CompletableFuture<Boolean> releaseFuture = new CompletableFuture<>();
        lockService.release(lock, ActionListener.wrap(releaseFuture::complete, releaseFuture::completeExceptionally));

        assertTrue(releaseFuture.get(5, TimeUnit.SECONDS));
        verify(cancellable).cancel();
        assertEquals(1, groupSemaphores.get("group-1").availablePermits());

        releaseCaptor.getValue().run();

        assertEquals(1, groupSemaphores.get("group-1").availablePermits());
    }

    private Job buildDailyCleanupJob(Instant now) {
        return buildDailyCleanupJob(now, true);
    }

    private Job buildDailyCleanupJob(Instant now, boolean enabled) {
        return new Job(
            DAILY_S3_CLEANUP_SCHEDULE_NAME,
            new IntervalSchedule(now, 1, ChronoUnit.MINUTES),
            null,
            enabled,
            now,
            null,
            now,
            Duration.ofMinutes(1).getSeconds(),
            null,
            null,
            null,
            AnalysisType.DAILY_S3_CHECKPOINT_CLEANUP
        );
    }

    private String buildMessageBody(Job job, Instant scheduledTime) throws IOException {
        XContentBuilder builder = job.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
        Map<String, Object> jobMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, MediaTypeRegistry.JSON).v2();
        jobMap.put(CommonName.EB_SCHEDULED_TIME_FIELD, scheduledTime.toString());

        XContentBuilder decoratedBuilder = JsonXContent.contentBuilder();
        decoratedBuilder.map(jobMap);
        return BytesReference.bytes(decoratedBuilder).utf8ToString();
    }

    private JobExecutionContext createJobExecutionContext(LockService lockService, Job job) {
        return new JobExecutionContext(clock.instant(), new JobDocVersion(1, 1, 1), lockService, CommonName.JOB_INDEX, job.getName());
    }

    private LockService createGroupSemaphoreLockService(
        ConcurrentHashMap<String, Semaphore> groupSemaphores,
        String groupId,
        ThreadPool threadPool
    ) throws Exception {
        Class<?> releaseContextClass = Class.forName(SQSConsumerTask.class.getName() + "$LockReleaseContext");
        Class<?> lockServiceClass = Class.forName(SQSConsumerTask.DefaultSQSMessageHandler.class.getName() + "$GroupSemaphoreLockService");
        Constructor<?> constructor = lockServiceClass
            .getDeclaredConstructor(releaseContextClass, ConcurrentHashMap.class, String.class, ThreadPool.class);
        constructor.setAccessible(true);
        return (LockService) constructor.newInstance(null, groupSemaphores, groupId, threadPool);
    }
}
