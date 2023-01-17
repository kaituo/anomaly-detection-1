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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Random;

import org.opensearch.ad.ADNodeState;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.ad.ml.ADEntityColdStarter;
import org.opensearch.ad.ml.ADModelState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.EntityCacheProvider;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.TimeSeriesColdStartWorker;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A queue for HCAD model training (a.k.a. cold start). As model training is a
 * pretty expensive operation, we pull cold start requests from the queue in a
 * serial fashion. Each detector has an equal chance of being pulled. The equal
 * probability is achieved by putting model training requests for different
 * detectors into different segments and pulling requests from segments in a
 * round-robin fashion.
 *
 */

// suppress warning due to the use of generic type ADModelState
public class ADEntityColdStartWorker extends TimeSeriesColdStartWorker<ADNodeState, ThresholdedRandomCutForest, ADModelState<EntityModel<ThresholdedRandomCutForest>>> {
    public static final String WORKER_NAME = "ad-cold-start";

    public ADEntityColdStartWorker(
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
        ADEntityColdStarter entityColdStarter,
        Duration stateTtl,
        ADNodeStateManager nodeStateManager,
        EntityCacheProvider<ThresholdedRandomCutForest, TimeSeriesModelState<EntityModel<ThresholdedRandomCutForest>>> cacheProvider
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
            ENTITY_COLD_START_QUEUE_CONCURRENCY,
            executionTtl,
            entityColdStarter,
            stateTtl,
            nodeStateManager,
            cacheProvider
        );
    }

    @Override
    protected ADModelState<EntityModel<ThresholdedRandomCutForest>> createEmptyState(Entity entity, String modelId, String configId) {
        return new ADModelState<EntityModel<ThresholdedRandomCutForest>>(
                new EntityModel<>(entity, new ArrayDeque<>(), null),
                modelId,
                configId,
                ModelManager.ModelType.ENTITY.getName(),
                clock,
                0
            );
    }
}
