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
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.ad.ADNodeState;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ADModelState;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ADEntityColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.ad.ratelimit.ADResultWriteWorker;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.indices.ForecastIndices;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastModelState;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.transport.ForecastResultBulkRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.EntityFeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointReadWorker;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ForecastCheckpointReadWorker extends
TimeSeriesCheckpointReadWorker<ForecastNodeState, RCFCaster, ForecastResult, ForecastResultWriteRequest, ForecastResultBulkRequest, ForecastModelState<EntityModel<RCFCaster>>, ForecastModelManager, ForecastCheckpointDao> {
    private static final Logger LOG = LogManager.getLogger(ADCheckpointReadWorker.class);
    public static final String WORKER_NAME = "forecast-checkpoint-read";

    public ForecastCheckpointReadWorker(
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
        ForecastModelManager modelManager,
        ForecastCheckpointDao checkpointDao,
        ForecastColdStartWorker entityColdStartQueue,
        ForecastResultWriteWorker resultWriteQueue,
        ForecastNodeStateManager stateManager,
        ForecastIndices indexUtil,
        CacheProvider<RCFCaster, ForecastModelState<EntityModel<RCFCaster>>> cacheProvider,
        Duration stateTtl,
        ForecastCheckpointWriteWorker checkpointWriteQueue,
        TimeSeriesStats adStats
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
            executionTtl,
            modelManager,
            checkpointDao,
            entityColdStartQueue,
            resultWriteQueue,
            stateManager,
            indexUtil,
            cacheProvider,
            stateTtl,
            checkpointWriteQueue,
            adStats,
            FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY,
            FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
            ADCommonName.CHECKPOINT_INDEX_NAME
        );
    }

    @Override
    protected ActionListener<Optional<? extends Config>> processIterationUsingConfig(
            EntityFeatureRequest origRequest,
            int index,
            String configId,
            List<EntityFeatureRequest> toProcess,
            Map<String, MultiGetItemResponse> successfulRequests,
            Set<String> retryableRequests,
            ForecastModelState<EntityModel<RCFCaster>> restoredModelState,
            Entity entity,
            String modelId
        ) {
            return ActionListener.wrap(detectorOptional -> {
                if (false == detectorOptional.isPresent()) {
                    LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", configId));
                    processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                    return;
                }

                Config config = detectorOptional.get();

                ThresholdingResult result = null;
                try {
                    result = modelManager
                        .getAnomalyResultForEntity(origRequest.getCurrentFeature(), modelState, modelId, entity, config);
                } catch (IllegalArgumentException e) {
                    // fail to score likely due to model corruption. Re-cold start to recover.
                    LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", origRequest.getModelId()), e);
                    adStats.getStat(StatNames.MODEL_CORRUTPION_COUNT.getName()).increment();
                    if (origRequest.getModelId().isPresent()) {
                        String entityModelId = origRequest.getModelId().get();
                        checkpointDao
                            .deleteModelCheckpoint(
                                entityModelId,
                                ActionListener
                                    .wrap(
                                        r -> LOG.debug(new ParameterizedMessage("Succeeded in deleting checkpoint [{}].", entityModelId)),
                                        ex -> LOG.error(new ParameterizedMessage("Failed to delete checkpoint [{}].", entityModelId), ex)
                                    )
                            );
                    }

                    entityColdStartWorker.put(origRequest);
                    processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                    return;
                }

                if (result != null && result.getRcfScore() > 0) {
                    AnomalyResult resultToSave = result
                        .toAnomalyResult(
                            (AnomalyDetector) config,
                            Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                            Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + config.getIntervalInMilliseconds()),
                            Instant.now(),
                            Instant.now(),
                            ParseUtils.getFeatureData(origRequest.getCurrentFeature(), config),
                            entity,
                            indexUtil.getSchemaVersion(ADIndex.RESULT),
                            modelId,
                            null,
                            null
                        );

                    resultWriteWorker
                        .put(
                            new ADResultWriteRequest(
                                origRequest.getExpirationEpochMs(),
                                configId,
                                result.getGrade() > 0 ? RequestPriority.HIGH : RequestPriority.MEDIUM,
                                resultToSave,
                                config.getCustomResultIndex()
                            )
                        );
                }

                // try to load to cache
                boolean loaded = cacheProvider.get().hostIfPossible(config, modelState);

                if (false == loaded) {
                    // not in memory. Maybe cold entities or some other entities
                    // have filled the slot while waiting for loading checkpoints.
                    checkpointWriteWorker.write(modelState, true, RequestPriority.LOW);
                }

                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
            }, exception -> {
                LOG.error(new ParameterizedMessage("fail to get checkpoint [{}]", modelId, exception));
                nodeStateManager.setException(configId, exception);
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
            });
        }{

}
