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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.ad.ADNodeState;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.ratelimit.CheckPointMaintainRequestAdapter;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointMaintainWorker;


public class ADCheckpointMaintainWorker extends TimeSeriesCheckpointMaintainWorker<ADNodeState> {
    public static final String WORKER_NAME = "ad-checkpoint-maintain";

    public ADCheckpointMaintainWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        TimeSeriesCircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        ADCheckpointWriteWorker checkpointWriteQueue,
        Duration stateTtl,
        ADNodeStateManager nodeStateManager,
        CheckPointMaintainRequestAdapter adapter
    ) {
        super(
            WORKER_NAME,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            checkpointWriteQueue,
            stateTtl,
            nodeStateManager,
            adapter
        );

        this.batchSize = AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE, it -> this.batchSize = it);

        this.expectedExecutionTimeInMilliSecsPerRequest = AnomalyDetectorSettings.AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS
                .get(settings);
            clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(
                    AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                    it -> this.expectedExecutionTimeInMilliSecsPerRequest = it
                );
    }
}
