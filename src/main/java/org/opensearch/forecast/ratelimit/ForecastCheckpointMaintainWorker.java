package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.Context;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainRequest;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteRequest;
import org.opensearch.timeseries.ratelimit.RateLimitedRequestWorker;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastCheckpointMaintainWorker extends
    CheckpointMaintainWorker<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao> {
    public static final String WORKER_NAME = "forecast-checkpoint-maintain";

    public ForecastCheckpointMaintainWorker(
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
        RateLimitedRequestWorker<CheckpointWriteRequest> targetQueue,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        Function<CheckpointMaintainRequest, Optional<CheckpointWriteRequest>> converter
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
            targetQueue,
            stateTtl,
            nodeStateManager,
            converter,
            Context.FORECAST
        );

        this.batchSize = FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE, it -> this.batchSize = it);

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
