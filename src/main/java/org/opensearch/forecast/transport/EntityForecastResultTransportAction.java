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
package org.opensearch.forecast.transport;


import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.timeseries.transport.TimeSeriesEntityResultProcessor;
import org.opensearch.common.inject.Inject;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndices;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastModelState;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastCheckpointReadWorker;
import org.opensearch.forecast.ratelimit.ForecastColdEntityWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.forecast.ratelimit.ForecastResultWriteWorker;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.EntityCacheProvider;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.RCFCaster;

/**
 * Entry-point for HC forecast workflow.  We have created multiple queues for coordinating
 * the workflow. The overrall workflow is:
 * 1. We store as many frequently used entity models in a cache as allowed by the
 *  memory limit (by default 10% heap). If an entity feature is a hit, we use the in-memory model
 *  to forecast and record results using the result write queue.
 * 2. If an entity feature is a miss, we check if there is free memory or any other
 *  entity's model can be evacuated. An in-memory entity's frequency may be lower
 *  compared to the cache miss entity. If that's the case, we replace the lower
 *  frequency entity's model with the higher frequency entity's model. To load the
 *  higher frequency entity's model, we first check if a model exists on disk by
 *  sending a checkpoint read queue request. If there is a checkpoint, we load it
 *  to memory, perform forecast, and save the result using the result write queue.
 *  Otherwise, we enqueue a cold start request to the cold start queue for model
 *  training. If training is successful, we save the learned model via the checkpoint
 *  write queue.
 * 3. We also have the cold entity queue configured for cold entities, and the model
 * training and inference are connected by serial juxtaposition to limit resource usage.
 */
public class EntityForecastResultTransportAction extends HandledTransportAction<EntityForecastResultRequest, AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityForecastResultTransportAction.class);
    private TimeSeriesCircuitBreakerService circuitBreakerService;
    private EntityCacheProvider<RCFCaster, ForecastModelState<EntityModel<RCFCaster>>> cache;
    private final ForecastNodeStateManager stateManager;
    private ThreadPool threadPool;
    private TimeSeriesEntityResultProcessor<EntityForecastResultRequest, RCFCaster, ForecastModelState<EntityModel<RCFCaster>>, ForecastNodeState, RCFCasterResult, ForecastIndices, ForecastResult, ForecastResultWriteRequest, ForecastResultBulkRequest, ForecastCheckpointDao, ForecastModelManager> intervalDataProcessor;

    @Inject
    public EntityForecastResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ForecastModelManager manager,
        TimeSeriesCircuitBreakerService adCircuitBreakerService,
        EntityCacheProvider<RCFCaster, ForecastModelState<EntityModel<RCFCaster>>> entityCache,
        ForecastNodeStateManager stateManager,
        ForecastIndices indexUtil,
        ForecastResultWriteWorker resultWriteQueue,
        ForecastCheckpointReadWorker checkpointReadQueue,
        ForecastColdEntityWorker coldEntityQueue,
        ThreadPool threadPool,
        ForecastColdStartWorker entityColdStartWorker,
        TimeSeriesStats timeSeriesStats
    ) {
        super(EntityForecastResultAction.NAME, transportService, actionFilters, EntityForecastResultRequest::new);
        this.circuitBreakerService = adCircuitBreakerService;
        this.cache = entityCache;
        this.stateManager = stateManager;
        this.threadPool = threadPool;
        this.intervalDataProcessor = new TimeSeriesEntityResultProcessor<>(entityCache, manager, ForecastIndex.RESULT,
                indexUtil, resultWriteQueue, ForecastResultWriteRequest.class, timeSeriesStats, entityColdStartWorker, checkpointReadQueue,
                coldEntityQueue);
    }

    @Override
    protected void doExecute(Task task, EntityForecastResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (circuitBreakerService.isOpen()) {
            threadPool.executor(TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME).execute(() -> cache.get().releaseMemoryForOpenCircuitBreaker());
            listener
                .onFailure(new LimitExceededException(request.getConfigId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String forecasterId = request.getConfigId();

            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(forecasterId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error("Previous exception of {}: {}", forecasterId, exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }

                listener = ExceptionUtil.wrapListener(listener, exception, forecasterId);
            }

            stateManager.getConfig(forecasterId, intervalDataProcessor.onGetConfig(listener, forecasterId, request, previousException));
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }
    }
}

