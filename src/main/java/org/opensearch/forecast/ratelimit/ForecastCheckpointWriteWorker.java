package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastCheckpointWriteWorker extends
    CheckpointWriteWorker<NodeState, RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao> {
    public static final String WORKER_NAME = "forecast-checkpoint-write";

    public ForecastCheckpointWriteWorker(
        long heapSize,
        int singleRequestSize,
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
        Duration stateTtl,
        NodeStateManager<NodeState> timeSeriesNodeStateManager,
        ForecastCheckpointDao checkpoint,
        String indexName,
        Duration checkpointInterval
    ) {
        super(
            WORKER_NAME,
            heapSize,
            singleRequestSize,
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
            FORECAST_CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl,
            timeSeriesNodeStateManager,
            checkpoint,
            indexName,
            checkpointInterval
        );
    }

}
