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

package org.opensearch.ad.transport;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.ADNodeState;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ADModelState;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADColdEntityWorker;
import org.opensearch.ad.ratelimit.ADEntityColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.ad.ratelimit.ADResultWriteWorker;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.timeseries.transport.TimeSeriesEntityResultRequest;
import org.opensearch.timeseries.transport.TimeSeriesEntityResultProcessor;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkRequest;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.EntityCacheProvider;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.TimeSeriesIndices;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ml.TimeSeriesCheckpointDao;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.EntityFeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.ratelimit.TimeSeriesResultWriteWorker;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Entry-point for HCAD workflow. We have created multiple queues for
 * coordinating the workflow. The overrall workflow is: 1. We store as many
 * frequently used entity models in a cache as allowed by the memory limit (10%
 * heap). If an entity feature is a hit, we use the in-memory model to detect
 * anomalies and record results using the result write queue. 2. If an entity
 * feature is a miss, we check if there is free memory or any other entity's
 * model can be evacuated. An in-memory entity's frequency may be lower compared
 * to the cache miss entity. If that's the case, we replace the lower frequency
 * entity's model with the higher frequency entity's model. To load the higher
 * frequency entity's model, we first check if a model exists on disk by sending
 * a checkpoint read queue request. If there is a checkpoint, we load it to
 * memory, perform detection, and save the result using the result write queue.
 * Otherwise, we enqueue a cold start request to the cold start queue for model
 * training. If training is successful, we save the learned model via the
 * checkpoint write queue. 3. We also have the cold entity queue configured for
 * cold entities, and the model training and inference are connected by serial
 * juxtaposition to limit resource usage.
 */
public class EntityADResultTransportAction extends HandledTransportAction<EntityADResultRequest, AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityADResultTransportAction.class);
    private TimeSeriesCircuitBreakerService adCircuitBreakerService;
    private EntityCacheProvider<ThresholdedRandomCutForest, ADModelState<EntityModel<ThresholdedRandomCutForest>>> cache;
    private final ADNodeStateManager stateManager;
    private ThreadPool threadPool;
    private TimeSeriesEntityResultProcessor<EntityADResultRequest, ThresholdedRandomCutForest, ADModelState<EntityModel<ThresholdedRandomCutForest>>, ADNodeState, ThresholdingResult, AnomalyDetectionIndices, AnomalyResult, ADResultWriteRequest, ADResultBulkRequest, ADCheckpointDao, ADModelManager> intervalDataProcessor;

    @Inject
    public EntityADResultTransportAction(ActionFilters actionFilters, TransportService transportService,
            ADModelManager manager, TimeSeriesCircuitBreakerService adCircuitBreakerService,
            EntityCacheProvider<ThresholdedRandomCutForest, ADModelState<EntityModel<ThresholdedRandomCutForest>>> entityCache,
            ADNodeStateManager stateManager, AnomalyDetectionIndices indexUtil, ADResultWriteWorker resultWriteQueue,
            ADCheckpointReadWorker checkpointReadQueue, ADColdEntityWorker coldEntityQueue, ThreadPool threadPool,
            ADEntityColdStartWorker entityColdStartWorker, TimeSeriesStats timeSeriesStats) {
        super(EntityADResultAction.NAME, transportService, actionFilters, EntityADResultRequest::new);
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.cache = entityCache;
        this.stateManager = stateManager;
        this.threadPool = threadPool;
        this.intervalDataProcessor = new TimeSeriesEntityResultProcessor<>(entityCache, manager, ADIndex.RESULT,
                indexUtil, resultWriteQueue, ADResultWriteRequest.class, timeSeriesStats, entityColdStartWorker, checkpointReadQueue,
                coldEntityQueue);
    }

    @Override
    protected void doExecute(Task task, EntityADResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)
                    .execute(() -> cache.get().releaseMemoryForOpenCircuitBreaker());
            listener.onFailure(new LimitExceededException(request.getConfigId(),
                    CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String detectorId = request.getConfigId();

            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(detectorId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error("Previous exception of {}: {}", detectorId, exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }

                listener = ExceptionUtil.wrapListener(listener, exception, detectorId);
            }

            stateManager.getConfig(detectorId,
                    intervalDataProcessor.onGetConfig(listener, detectorId, request, previousException));
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }
    }
}
