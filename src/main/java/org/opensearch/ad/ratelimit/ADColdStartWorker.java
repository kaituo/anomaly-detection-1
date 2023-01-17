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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Random;

import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADEntityColdStart;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;

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
public class ADColdStartWorker extends
    ColdStartWorker<ThresholdedRandomCutForest, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADEntityColdStart, ADPriorityCache> {
    public static final String WORKER_NAME = "ad-cold-start";

    public ADColdStartWorker(
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
        ADEntityColdStart entityColdStarter,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        ADPriorityCache cacheProvider
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
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            AD_ENTITY_COLD_START_QUEUE_CONCURRENCY,
            executionTtl,
            entityColdStarter,
            stateTtl,
            nodeStateManager,
            cacheProvider,
            AnalysisType.AD
        );
    }

    @Override
    protected ModelState<ThresholdedRandomCutForest> createEmptyState(FeatureRequest request, String modelId, String configId) {
        return new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            configId,
            ModelManager.ModelType.RCFCASTER.getName(),
            clock,
            0,
            new Sample(),
            request.getEntity(),
            new ArrayDeque<>()
        );
    }
}
