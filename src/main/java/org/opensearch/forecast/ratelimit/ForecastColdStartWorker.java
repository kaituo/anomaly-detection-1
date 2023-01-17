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


package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_COLD_START_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.ml.ForecastColdStarter;
import org.opensearch.forecast.ml.ForecastModelState;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.ratelimit.TimeSeriesColdStartWorker;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastColdStartWorker extends TimeSeriesColdStartWorker<ForecastNodeState, RCFCaster, ForecastModelState<EntityModel<RCFCaster>>> {
    private static final Logger LOG = LogManager.getLogger(ForecastColdStartWorker.class);
    public static final String WORKER_NAME = "forecast-cold-start";

    public ForecastColdStartWorker(
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
        Duration executionTtl,
        ForecastColdStarter coldStarter,
        Duration stateTtl,
        ForecastNodeStateManager nodeStateManager,
        CacheProvider<RCFCaster, TimeSeriesModelState<EntityModel<RCFCaster>>> cacheProvider
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
    protected ForecastModelState<EntityModel<RCFCaster>> createEmptyState(Entity entity, String modelId, String configId) {
        return new ForecastModelState<EntityModel<RCFCaster>>(
                new EntityModel<>(entity, new ArrayDeque<>(), null),
                modelId,
                configId,
                ModelManager.ModelType.ENTITY.getName(),
                clock,
                0,
                new Sample()
            );
    }
}
