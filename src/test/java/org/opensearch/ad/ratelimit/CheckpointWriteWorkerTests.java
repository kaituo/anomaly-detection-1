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

package org.opensearch.ad.ratelimit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class CheckpointWriteWorkerTests extends AbstractRateLimitingTest {
    CheckpointWriteWorker worker;

    CheckpointDao checkpoint;
    ClusterService clusterService;

    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        checkpoint = mock(CheckpointDao.class);
        Map<String, Object> checkpointMap = new HashMap<>();
        checkpointMap.put(CheckpointDao.FIELD_MODEL, "a");
        when(checkpoint.toIndexSource(any())).thenReturn(checkpointMap);

        // Integer.MAX_VALUE makes a huge heap
        worker = new CheckpointWriteWorker(
            Integer.MAX_VALUE,
            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            mock(ADCircuitBreakerService.class),
            threadPool,
            Settings.EMPTY,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            AnomalyDetectorSettings.QUEUE_MAINTENANCE,
            checkpoint,
            CommonName.CHECKPOINT_INDEX_NAME,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            nodeStateManager,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE
        );
    }

    public void testTriggerSave() {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().build());
        worker.write(state, true, RequestPriority.MEDIUM);

        verify(checkpoint, times(1)).batchWrite(any(), any());
    }

    /**
     * Test that when more requests are coming than concurrency allowed, queues will be
     * auto-flushed given enough time.
     * @throws InterruptedException when thread.sleep gets interrupted
     */
    public void testTriggerAutoFlush() throws InterruptedException {
        final CountDownLatch processingLatch = new CountDownLatch(1);

        ExecutorService executorService = mock(ExecutorService.class);

        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = () -> {
                try {
                    processingLatch.await(100, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.error(e);
                    assertTrue("Unexpected exception", false);
                }
                Runnable toInvoke = invocation.getArgument(0);
                toInvoke.run();
            };
            // start a new thread so it won't block main test thread's execution
            new Thread(runnable).start();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        // make sure permits are released and the next request probe starts
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(checkpoint).batchWrite(any(), any());

        // Integer.MAX_VALUE makes a huge heap
        // create a worker to use mockThreadPool
        worker = new CheckpointWriteWorker(
            Integer.MAX_VALUE,
            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            mock(ADCircuitBreakerService.class),
            mockThreadPool,
            Settings.EMPTY,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            AnomalyDetectorSettings.QUEUE_MAINTENANCE,
            checkpoint,
            CommonName.CHECKPOINT_INDEX_NAME,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            nodeStateManager,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE
        );

        // our concurrency is 2, so first 2 requests cause two batches. And the
        // remaining 1 stays in the queue until the 2 concurrent runs finish.
        // first 2 batch account for one checkpoint.batchWrite; the remaining one
        // calls checkpoint.batchWrite
        // CHECKPOINT_WRITE_QUEUE_BATCH_SIZE is the largest batch size
        int numberOfRequests = 2 * CHECKPOINT_WRITE_QUEUE_BATCH_SIZE.getDefault(Settings.EMPTY) + 1;
        for (int i = 0; i < numberOfRequests; i++) {
            ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().build());
            worker.write(state, true, RequestPriority.MEDIUM);
        }

        // Here, we allow the first 2 pulling batch from queue operations to start.
        processingLatch.countDown();

        // wait until queues get emptied
        int waitIntervals = 20;
        while (!worker.isQueueEmpty() && waitIntervals-- >= 0) {
            Thread.sleep(500);
        }

        assertTrue(worker.isQueueEmpty());
        // of requests cause at least one batch.
        verify(checkpoint, times(3)).batchWrite(any(), any());
    }
}
