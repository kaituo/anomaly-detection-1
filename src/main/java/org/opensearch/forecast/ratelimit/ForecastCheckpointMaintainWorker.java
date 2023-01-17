package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.ratelimit.CheckPointMaintainRequestAdapter;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteRequest;
import org.opensearch.timeseries.ratelimit.RateLimitedRequestWorker;

public class ForecastCheckpointMaintainWorker extends TimeSeriesCheckpointMaintainWorker<ForecastNodeState> {
    public static final String WORKER_NAME = "forecast-checkpoint-maintain";

    public ForecastCheckpointMaintainWorker(String workerName, long heapSizeInBytes, int singleRequestSizeInBytes,
            Setting<Float> maxHeapPercentForQueueSetting, ClusterService clusterService, Random random,
            TimeSeriesCircuitBreakerService adCircuitBreakerService, ThreadPool threadPool, Settings settings,
            float maxQueuedTaskRatio, Clock clock, float mediumSegmentPruneRatio, float lowSegmentPruneRatio,
            int maintenanceFreqConstant,
            RateLimitedRequestWorker<CheckpointWriteRequest, ForecastNodeState> targetQueue, Duration stateTtl,
            TimeSeriesNodeStateManager<ForecastNodeState> nodeStateManager, CheckPointMaintainRequestAdapter adapter) {
        super(workerName, heapSizeInBytes, singleRequestSizeInBytes, maxHeapPercentForQueueSetting, clusterService, random,
                adCircuitBreakerService, threadPool, settings, maxQueuedTaskRatio, clock, mediumSegmentPruneRatio,
                lowSegmentPruneRatio, maintenanceFreqConstant, targetQueue, stateTtl, nodeStateManager, adapter);

        this.batchSize = FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE, it -> this.batchSize = it);

        this.expectedExecutionTimeInMilliSecsPerRequest = ForecastSettings.FORECAST_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS
                .get(settings);
            clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(
                        FORECAST_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                    it -> this.expectedExecutionTimeInMilliSecsPerRequest = it
                );
    }

}
