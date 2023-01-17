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

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastModelState;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.transport.ForecastResultBulkRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ratelimit.TimeSeriesColdEntityWorker;

import com.amazon.randomcutforest.parkservices.RCFCaster;

/**
 * A queue slowly releasing low-priority requests to CheckpointReadQueue
 *
 * ColdEntityQueue is a queue to absorb cold entities. Like hot entities, we load a cold
 * entity's model checkpoint from disk, train models if the checkpoint is not found,
 * query for missed features to complete a shingle, use the models to check whether
 * the incoming feature is normal, update models, and save the detection results to disks. 
 * Implementation-wise, we reuse the queues we have developed for hot entities.
 * The differences are: we process hot entities as long as resources (e.g., AD
 * thread pool has availability) are available, while we release cold entity requests
 * to other queues at a slow controlled pace. Also, cold entity requests' priority is low.
 * So only when there are no hot entity requests to process are we going to process cold
 * entity requests. 
 *
 */
public class ForecastColdEntityWorker extends TimeSeriesColdEntityWorker<ForecastNodeState, RCFCaster, ForecastResult, ForecastResultWriteRequest, ForecastResultBulkRequest, ForecastModelState<EntityModel<RCFCaster>>, RCFCasterResult, ForecastModelManager, ForecastCheckpointDao> {
    public static final String WORKER_NAME = "forecast-cold-entity";

    public ForecastColdEntityWorker(
            long heapSizeInBytes,
            int singleRequestSizeInBytes,
            Setting<Float> maxHeapPercentForQueueSetting,
            ClusterService clusterService,
            Random random,
            TimeSeriesCircuitBreakerService forecastCircuitBreakerService,
            ThreadPool threadPool,
            Settings settings,
            float maxQueuedTaskRatio,
            Clock clock,
            float mediumSegmentPruneRatio,
            float lowSegmentPruneRatio,
            int maintenanceFreqConstant,
            ForecastCheckpointReadWorker checkpointReadQueue,
            Duration stateTtl,
            ForecastNodeStateManager nodeStateManager
        ) {
            super(
                WORKER_NAME,
                heapSizeInBytes,
                singleRequestSizeInBytes,
                maxHeapPercentForQueueSetting,
                clusterService,
                random,
                forecastCircuitBreakerService,
                threadPool,
                settings,
                maxQueuedTaskRatio,
                clock,
                mediumSegmentPruneRatio,
                lowSegmentPruneRatio,
                maintenanceFreqConstant,
                checkpointReadQueue,
                stateTtl,
                nodeStateManager,
                FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
                FORECAST_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS
            );
        }
}
