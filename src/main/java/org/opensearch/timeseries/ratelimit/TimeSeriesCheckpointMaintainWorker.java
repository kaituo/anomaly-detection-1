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

package org.opensearch.timeseries.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;

public abstract class TimeSeriesCheckpointMaintainWorker<NodeState extends ExpiringState> extends ScheduledWorker<CheckpointMaintainRequest, CheckpointWriteRequest, NodeState> {

    private CheckPointMaintainRequestAdapter adapter;

    public TimeSeriesCheckpointMaintainWorker(String workerName, long heapSizeInBytes, int singleRequestSizeInBytes,
            Setting<Float> maxHeapPercentForQueueSetting, ClusterService clusterService, Random random,
            TimeSeriesCircuitBreakerService adCircuitBreakerService, ThreadPool threadPool, Settings settings,
            float maxQueuedTaskRatio, Clock clock, float mediumSegmentPruneRatio, float lowSegmentPruneRatio,
            int maintenanceFreqConstant, RateLimitedRequestWorker<CheckpointWriteRequest, NodeState> targetQueue,
            Duration stateTtl, TimeSeriesNodeStateManager<NodeState> nodeStateManager, CheckPointMaintainRequestAdapter adapter) {
        super(workerName, heapSizeInBytes, singleRequestSizeInBytes, maxHeapPercentForQueueSetting, clusterService, random,
                adCircuitBreakerService, threadPool, settings, maxQueuedTaskRatio, clock, mediumSegmentPruneRatio,
                lowSegmentPruneRatio, maintenanceFreqConstant, targetQueue, stateTtl, nodeStateManager);
        this.adapter = adapter;
    }

    @Override
    protected List<CheckpointWriteRequest> transformRequests(List<CheckpointMaintainRequest> requests) {
        List<CheckpointWriteRequest> allRequests = new ArrayList<>();
        for (CheckpointMaintainRequest request : requests) {
            Optional<CheckpointWriteRequest> converted = adapter.convert(request);
            if (!converted.isEmpty()) {
                allRequests.add(converted.get());
            }
        }
        return allRequests;
    }
}
