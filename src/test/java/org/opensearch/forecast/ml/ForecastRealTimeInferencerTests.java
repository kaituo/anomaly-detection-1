/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastSaveResultStrategy;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.util.ExpiringValue;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastRealTimeInferencerTests extends OpenSearchTestCase {
    private ForecastRealTimeInferencer inferencer;
    private Clock clock;
    private Forecaster config;
    private ModelState<RCFCaster> modelState;
    private Sample sample;
    private ThreadPool threadPool;
    private ForecastModelManager modelManager;
    private Stats stats;
    private TimeSeriesStat timeSeriesStat;
    private ForecastCacheProvider cacheProvider;
    private ForecastPriorityCache cache;
    private SearchFeatureDao searchFeatureDao;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        // Initialize clock to fixed instant
        clock = mock(Clock.class);

        threadPool = mock(ThreadPool.class);
        modelManager = mock(ForecastModelManager.class);

        // Mock the stats.getStat to return a mock TimeSeriesStat
        stats = mock(Stats.class);
        timeSeriesStat = mock(TimeSeriesStat.class);
        when(stats.getStat(anyString())).thenReturn(timeSeriesStat);

        cacheProvider = mock(ForecastCacheProvider.class);
        cache = mock(ForecastPriorityCache.class);
        when(cacheProvider.get()).thenReturn(cache);
        searchFeatureDao = mock(SearchFeatureDao.class);

        // Initialize inferencer with mocks or minimal implementations
        inferencer = new ForecastRealTimeInferencer(
            modelManager,
            stats,
            mock(ForecastCheckpointDao.class),
            mock(ForecastColdStartWorker.class),
            mock(ForecastSaveResultStrategy.class),
            cacheProvider,
            threadPool,
            clock,
            searchFeatureDao
        );

        // Set up the Config object with an interval duration
        config = mock(Forecaster.class);
        when(config.getIntervalDuration()).thenReturn(Duration.ofSeconds(60)); // 60 seconds

        modelState = mock(ModelState.class);
        sample = mock(Sample.class);
    }

    public void testProcessBindsDataSourceRoutingBeforeFetchingGapFeatures() {
        String modelId = "testModelId";
        String tenantId = "account-1:application-1:workspace-1";
        String dataSourceId = "data-source-1";
        Instant sampleStart = Instant.ofEpochSecond(120);
        Instant sampleEnd = Instant.ofEpochSecond(180);
        List<Entry<Long, Long>> sampleRanges = Collections.singletonList(new SimpleEntry<>(60_000L, 120_000L));
        AtomicBoolean routingActive = new AtomicBoolean(false);
        AtomicBoolean featureFetchCalled = new AtomicBoolean(false);

        when(config.getId()).thenReturn("forecaster-1");
        when(config.getTenantId()).thenReturn(tenantId);
        when(config.getDataSourceId()).thenReturn(dataSourceId);
        when(config.getIntervalInSeconds()).thenReturn(60L);
        when(config.getIntervalInMilliseconds()).thenReturn(60_000L);
        when(config.getInterval()).thenReturn(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES));
        when(modelState.getModelId()).thenReturn(modelId);
        when(modelState.getModel()).thenReturn(Optional.of(mock(RCFCaster.class)));
        when(modelState.getEntity()).thenReturn(Optional.empty());
        when(modelState.getSamples())
            .thenReturn(
                new ArrayDeque<>(Collections.singletonList(new Sample(new double[] { 1.0 }, Instant.EPOCH, Instant.ofEpochSecond(60))))
            );
        when(sample.getDataStartTime()).thenReturn(sampleStart);
        when(sample.getDataEndTime()).thenReturn(sampleEnd);
        when(searchFeatureDao.getTrainSampleRanges(any(IntervalTimeConfiguration.class), anyLong(), anyLong(), anyInt()))
            .thenReturn(sampleRanges);
        when(searchFeatureDao.bindRouting(tenantId, dataSourceId)).thenAnswer(invocation -> {
            routingActive.set(true);
            return (Releasable) () -> routingActive.set(false);
        });
        doAnswer(invocation -> {
            assertTrue(routingActive.get());
            featureFetchCalled.set(true);
            return null;
        })
            .when(searchFeatureDao)
            .getColdStartSamplesForPeriods(eq(config), any(), any(), eq(true), eq(true), eq(AnalysisType.FORECAST), any());

        inferencer
            .process(sample, modelState, config, "taskId", ActionListener.wrap(response -> {}, exception -> fail(exception.getMessage())));

        assertTrue(featureFetchCalled.get());
        assertFalse(routingActive.get());
        verify(searchFeatureDao).bindRouting(tenantId, dataSourceId);
    }

    public void testMaintenanceWithNonExpiredEntries() {
        long expirationTimeInMillis = config
            .getIntervalDuration()
            .multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ)
            .toMillis();

        String modelId = "testModelId";

        // Add entries to sampleQueues and modelLocks
        Map<String, ExpiringValue<ConcurrentSkipListSet<Sample>>> sampleQueues = inferencer.getSampleQueues();
        Map<String, ExpiringValue<Lock>> modelLocks = inferencer.getModelLocks();

        // Create a sample queue and add to sampleQueues
        ConcurrentSkipListSet<Sample> sampleQueue = new ConcurrentSkipListSet<>();
        ExpiringValue<ConcurrentSkipListSet<Sample>> expiringSampleQueue = new ExpiringValue<>(sampleQueue, expirationTimeInMillis, clock);

        sampleQueues.put(modelId, expiringSampleQueue);

        // Create a model lock and add to modelLocks
        ReentrantLock lock = new ReentrantLock();
        ExpiringValue<Lock> expiringLock = new ExpiringValue<>(lock, expirationTimeInMillis, clock);

        modelLocks.put(modelId, expiringLock);

        // Verify that entries are present before maintenance
        assertTrue(sampleQueues.containsKey(modelId));
        assertTrue(modelLocks.containsKey(modelId));

        // Call maintenance()
        inferencer.maintenance();

        // Verify that entries are still present after maintenance
        assertTrue(sampleQueues.containsKey(modelId));
        assertTrue(modelLocks.containsKey(modelId));
    }

    public void testMaintenanceWithExpiredEntries() {
        long expirationTimeInMillis = config
            .getIntervalDuration()
            .multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ)
            .toMillis();

        String modelId = "testModelId";

        // Add entries to sampleQueues and modelLocks
        Map<String, ExpiringValue<ConcurrentSkipListSet<Sample>>> sampleQueues = inferencer.getSampleQueues();
        Map<String, ExpiringValue<Lock>> modelLocks = inferencer.getModelLocks();

        // Create a sample queue and add to sampleQueues
        ConcurrentSkipListSet<Sample> sampleQueue = new ConcurrentSkipListSet<>();
        ExpiringValue<ConcurrentSkipListSet<Sample>> expiringSampleQueue = new ExpiringValue<>(sampleQueue, expirationTimeInMillis, clock);

        sampleQueues.put(modelId, expiringSampleQueue);

        // Create a model lock and add to modelLocks
        ReentrantLock lock = new ReentrantLock();
        ExpiringValue<Lock> expiringLock = new ExpiringValue<>(lock, expirationTimeInMillis, clock);

        modelLocks.put(modelId, expiringLock);

        // Verify that entries are present before maintenance
        assertTrue(sampleQueues.containsKey(modelId));
        assertTrue(modelLocks.containsKey(modelId));

        // Advance clock beyond expiration time
        when(clock.millis()).thenReturn(expirationTimeInMillis + 1);

        // Call maintenance()
        inferencer.maintenance();

        // Verify that entries have been removed after maintenance
        assertFalse(sampleQueues.containsKey(modelId));
        assertFalse(modelLocks.containsKey(modelId));
    }

    public void testProcessWithTimeout_LockNotAcquired_TimeoutReached() throws InterruptedException {
        // Set up the Config object
        when(config.getIntervalInMilliseconds()).thenReturn(60000L); // 60 seconds in milliseconds
        when(config.getWindowDelay()).thenReturn(null);

        String modelId = "testModelId";

        // Mock modelState to return the modelId
        when(modelState.getModelId()).thenReturn(modelId);

        // Mock sample to return data end time
        when(sample.getDataEndTime()).thenReturn(Instant.ofEpochMilli(1000L));

        ReentrantLock lock = new ReentrantLock();
        CountDownLatch releaseLock = new CountDownLatch(1);
        Thread lockHolder = holdLock(lock, releaseLock);

        try {
            // Add the lock to modelLocks
            Map<String, ExpiringValue<Lock>> modelLocks = inferencer.getModelLocks();
            ExpiringValue<Lock> expiringLock = new ExpiringValue<>(
                lock,
                config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
                clock
            );
            modelLocks.put(modelId, expiringLock);

            // Set clock time to simulate timeout reached
            long windowDelayMillis = 0L; // Since getWindowDelay() returns null
            long curExecutionEnd = 1000L + windowDelayMillis; // sample data end time + window delay
            long nextExecutionEnd = curExecutionEnd + config.getIntervalInMilliseconds(); // Should be 1000 + 60000 = 61000
            when(clock.millis()).thenReturn(nextExecutionEnd + 1); // Set clock.millis() to 61001 to simulate timeout

            // Call processWithTimeout
            final CountDownLatch inprogress = new CountDownLatch(1);
            AtomicBoolean result = new AtomicBoolean(true);
            inferencer.processWithTimeout(modelState, config, "taskId", sample, ActionListener.wrap(response -> {
                result.set(response);
                inprogress.countDown();
            }, exception -> {
                inprogress.countDown();
                fail("should not have exception");
            }));

            // Verify that the method returns false
            assertTrue(inprogress.await(100, TimeUnit.SECONDS));
            assertFalse(result.get());

            // Verify that threadPool.schedule is NOT called
            verify(threadPool, never()).schedule(any(Runnable.class), any(TimeValue.class), anyString());
        } finally {
            releaseLock.countDown();
            lockHolder.join(30_000L);
        }
    }

    public void testProcessWithTimeout_LockNotAcquired_ScheduleRetry() throws InterruptedException {
        // Set up the Config object
        when(config.getIntervalInMilliseconds()).thenReturn(60000L); // 60 seconds in milliseconds
        when(config.getWindowDelay()).thenReturn(null);

        String modelId = "testModelId";

        // Mock modelState to return the modelId
        when(modelState.getModelId()).thenReturn(modelId);

        // Mock sample to return data end time
        when(sample.getDataEndTime()).thenReturn(Instant.ofEpochMilli(1000L));

        ReentrantLock lock = new ReentrantLock();
        CountDownLatch releaseLock = new CountDownLatch(1);
        Thread lockHolder = holdLock(lock, releaseLock);

        try {
            // Add the lock to modelLocks
            Map<String, ExpiringValue<Lock>> modelLocks = inferencer.getModelLocks();
            ExpiringValue<Lock> expiringLock = new ExpiringValue<>(
                lock,
                config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
                clock
            );
            modelLocks.put(modelId, expiringLock);

            // Set clock time to simulate timeout not reached
            long windowDelayMillis = 0L; // Since getWindowDelay() returns null
            long curExecutionEnd = 1000L + windowDelayMillis; // sample data end time + window delay
            long nextExecutionEnd = curExecutionEnd + config.getIntervalInMilliseconds(); // Should be 1000 + 60000 = 61000
            // when(clock.millis()).thenReturn(nextExecutionEnd - 1); // Set clock.millis() to 60999 to simulate timeout not reached
            when(clock.millis()).thenReturn(
                0L,                // ExpiringValue ctor
                nextExecutionEnd - 1, // first attempt (if condition + log)
                nextExecutionEnd - 1,
                nextExecutionEnd + 1, // second attempt hits timeout branch
                nextExecutionEnd + 1
            );

            // Mock the threadPool.schedule method to capture the Runnable
            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            ArgumentCaptor<TimeValue> timeValueCaptor = ArgumentCaptor.forClass(TimeValue.class);
            ArgumentCaptor<String> threadPoolNameCaptor = ArgumentCaptor.forClass(String.class);

            when(threadPool.schedule(runnableCaptor.capture(), timeValueCaptor.capture(), threadPoolNameCaptor.capture()))
                .thenReturn(mock(ScheduledCancellable.class));

            // Call processWithTimeout
            final CountDownLatch inprogress = new CountDownLatch(1);
            AtomicBoolean result = new AtomicBoolean(true);
            inferencer.processWithTimeout(modelState, config, "taskId", sample, ActionListener.wrap(response -> {
                result.set(response);
                inprogress.countDown();
            }, exception -> {
                inprogress.countDown();
                fail("should not have exception");
            }));

            // Verify that the method returns false
            runnableCaptor.getValue().run();
            assertTrue(inprogress.await(100, TimeUnit.SECONDS));
            // timeout reached, not retrying
            assertFalse(result.get());

            // Verify that threadPool.schedule is called
            verify(threadPool, times(1)).schedule(any(Runnable.class), any(TimeValue.class), anyString());

            // Verify that the scheduled Runnable is correct
            Runnable scheduledRunnable = runnableCaptor.getValue();
            assertNotNull(scheduledRunnable);

            // Verify that the scheduled time is 1 second
            TimeValue scheduledTimeValue = timeValueCaptor.getValue();
            assertEquals(1, scheduledTimeValue.seconds());
        } finally {
            releaseLock.countDown();
            lockHolder.join(30_000L);
        }
    }

    private Thread holdLock(ReentrantLock lock, CountDownLatch releaseLock) throws InterruptedException {
        CountDownLatch lockAcquired = new CountDownLatch(1);
        Thread lockHolder = new Thread(() -> {
            lock.lock();
            try {
                lockAcquired.countDown();
                releaseLock.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }, "forecast-realtime-inferencer-test-lock-holder");
        lockHolder.start();
        assertTrue(lockAcquired.await(30, TimeUnit.SECONDS));
        return lockHolder;
    }
}
