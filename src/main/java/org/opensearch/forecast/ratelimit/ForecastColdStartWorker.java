/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_COLD_START_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastColdStartWorker extends
    ColdStartWorker<NodeState, ForecastNodeStateManager, RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastColdStart, ForecastPriorityCache> {
    public static final String WORKER_NAME = "forecast-hc-cold-start";

    public ForecastColdStartWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Duration executionTtl,
        ForecastColdStart coldStarter,
        Duration stateTtl,
        ForecastNodeStateManager nodeStateManager,
        ForecastPriorityCache cacheProvider
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
            FORECAST_COLD_START_QUEUE_CONCURRENCY,
            executionTtl,
            coldStarter,
            stateTtl,
            nodeStateManager,
            cacheProvider
        );
    }

    @Override
    protected ModelState<RCFCaster> createEmptyState(FeatureRequest coldStartRequest, String modelId, String configId) {
        return new ModelState<RCFCaster>(
            null,
            modelId,
            configId,
            ModelManager.ModelType.RCFCASTER.getName(),
            clock,
            0,
            new Sample(),
            coldStartRequest.getEntity(),
            new ArrayDeque<>()
        );
    }
}
