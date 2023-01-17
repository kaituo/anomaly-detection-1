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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

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
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointReadWorker;
import org.opensearch.timeseries.ratelimit.EntityFeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * a queue for loading model checkpoint. The read is a multi-get query. Possible results are:
 * a). If a checkpoint is not found, we forward that request to the cold start queue.
 * b). When a request gets errors, the queue does not change its expiry time and puts
 *  that request to the end of the queue and automatically retries them before they expire.
 * c) When a checkpoint is found, we load that point to memory and score the input
 * data point and save the result if a complete model exists. Otherwise, we enqueue
 * the sample. If we can host that model in memory (e.g., there is enough memory),
 * we put the loaded model to cache. Otherwise (e.g., a cold entity), we write the
 * updated checkpoint back to disk.
 *
 */
public class ADCheckpointReadWorker extends
        TimeSeriesCheckpointReadWorker<ADNodeState, ThresholdedRandomCutForest, AnomalyResult, ADResultWriteRequest, ADResultBulkRequest, ADModelState<EntityModel<ThresholdedRandomCutForest>>, ADModelManager, ADCheckpointDao> {
    private static final Logger LOG = LogManager.getLogger(ADCheckpointReadWorker.class);
    public static final String WORKER_NAME = "ad-checkpoint-read";

    public ADCheckpointReadWorker(
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
        ADModelManager modelManager,
        ADCheckpointDao checkpointDao,
        ADEntityColdStartWorker entityColdStartQueue,
        ADResultWriteWorker resultWriteQueue,
        ADNodeStateManager stateManager,
        AnomalyDetectionIndices indexUtil,
        CacheProvider<ThresholdedRandomCutForest, ADModelState<EntityModel<ThresholdedRandomCutForest>>> cacheProvider,
        Duration stateTtl,
        ADCheckpointWriteWorker checkpointWriteQueue,
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
            AD_CHECKPOINT_READ_QUEUE_CONCURRENCY,
            AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
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
            ADModelState<EntityModel<ThresholdedRandomCutForest>> restoredModelState,
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
                        .getAnomalyResultForEntity(new Sample(origRequest.getCurrentFeature(), Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                                Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + config.getIntervalInMilliseconds())
                                ), restoredModelState, modelId, entity, config);
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
                boolean loaded = cacheProvider.get().hostIfPossible(config, restoredModelState);

                if (false == loaded) {
                    // not in memory. Maybe cold entities or some other entities
                    // have filled the slot while waiting for loading checkpoints.
                    checkpointWriteWorker.write(restoredModelState, true, RequestPriority.LOW);
                }

                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
            }, exception -> {
                LOG.error(new ParameterizedMessage("fail to get checkpoint [{}]", modelId, exception));
                nodeStateManager.setException(configId, exception);
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
            });
        }
}
