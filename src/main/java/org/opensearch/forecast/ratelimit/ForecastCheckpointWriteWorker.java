package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastModelState;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointWriteWorker;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastCheckpointWriteWorker extends TimeSeriesCheckpointWriteWorker<ForecastNodeState, RCFCaster, ForecastModelState<EntityModel<RCFCaster>>, ForecastCheckpointDao> {
    public static final String WORKER_NAME = "forecast-checkpoint-write";

    public ForecastCheckpointWriteWorker(long heapSize, int singleRequestSize,
            Setting<Float> maxHeapPercentForQueueSetting, ClusterService clusterService, Random random,
            TimeSeriesCircuitBreakerService adCircuitBreakerService, ThreadPool threadPool, Settings settings,
            float maxQueuedTaskRatio, Clock clock, float mediumSegmentPruneRatio, float lowSegmentPruneRatio,
            int maintenanceFreqConstant, Duration executionTtl,
            Duration stateTtl,
            TimeSeriesNodeStateManager<ForecastNodeState> timeSeriesNodeStateManager,
            ForecastCheckpointDao checkpoint, String indexName, Duration checkpointInterval) {
        super(WORKER_NAME, heapSize, singleRequestSize, maxHeapPercentForQueueSetting, clusterService, random,
                adCircuitBreakerService, threadPool, settings, maxQueuedTaskRatio, clock, mediumSegmentPruneRatio,
                lowSegmentPruneRatio, maintenanceFreqConstant, FORECAST_CHECKPOINT_WRITE_QUEUE_CONCURRENCY, executionTtl, FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE, stateTtl,
                timeSeriesNodeStateManager, checkpoint, indexName, checkpointInterval);
    }

}
