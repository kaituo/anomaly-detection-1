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

package org.opensearch.timeseries.ratelimit;


import java.time.Clock;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.Cache;
import org.opensearch.timeseries.ml.TimeSeriesCheckpointDao;
import org.opensearch.timeseries.ml.TimeSeriesColdStarter;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.util.ExceptionUtil;


public abstract class TimeSeriesColdStartWorker <NodeState extends ExpiringState,
    RCFModelType,
    ModelStateType,
    ModelState extends TimeSeriesModelState<ModelStateType>,
    CheckpointDaoType extends TimeSeriesCheckpointDao<RCFModelType>,
    CheckpointWriteWorkerType extends TimeSeriesCheckpointWriteWorker<NodeState, RCFModelType, CheckpointDaoType>,
    ColdStarterType extends TimeSeriesColdStarter<NodeState, RCFModelType, CheckpointDaoType, CheckpointWriteWorkerType>,
    CacheType extends Cache<ModelStateType, ModelState>
    >
    extends SingleRequestWorker<EntityRequest, NodeState> {
    private static final Logger LOG = LogManager.getLogger(TimeSeriesColdStartWorker.class);

    protected final ColdStarterType coldStarter;
    protected final Provider<CacheType> cacheProvider;

    public TimeSeriesColdStartWorker(
            String workerName,
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
            Setting<Integer> concurrency,
            Duration executionTtl,
            ColdStarterType coldStarter,
            Duration stateTtl,
            TimeSeriesNodeStateManager<NodeState> nodeStateManager,
            Provider<CacheType> cacheProvider
        ) {
            super(
                    workerName,
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
                concurrency,
                executionTtl,
                stateTtl,
                nodeStateManager
            );
            this.coldStarter = coldStarter;
            this.cacheProvider = cacheProvider;
        }

    @Override
    protected void executeRequest(EntityRequest coldStartRequest, ActionListener<Void> listener) {
        String configId = coldStartRequest.getId();

        Optional<String> modelId = coldStartRequest.getModelId();

        if (false == modelId.isPresent()) {
            String error = String.format(Locale.ROOT, "Fail to get model id for request %s", coldStartRequest);
            LOG.warn(error);
            listener.onFailure(new RuntimeException(error));
            return;
        }

        ModelState modelState = createEmptyState(coldStartRequest.getEntity(), modelId.get(), configId);

        ActionListener<Void> coldStartListener = ActionListener.wrap(r -> {
            nodeStateManager.getConfig(configId, ActionListener.wrap(detectorOptional -> {
                try {
                    if (!detectorOptional.isPresent()) {
                        LOG
                            .error(
                                new ParameterizedMessage(
                                    "fail to load trained model [{}] to cache due to the detector not being found.",
                                    modelState.getModelId()
                                )
                            );
                        return;
                    }
                    cacheProvider.get().hostIfPossible(detectorOptional.get(), modelState);

                } finally {
                    listener.onResponse(null);
                }
            }, listener::onFailure));

        }, e -> {
            try {
                if (ExceptionUtil.isOverloaded(e)) {
                    LOG.error("OpenSearch is overloaded");
                    setCoolDownStart();
                }
                nodeStateManager.setException(configId, e);
            } finally {
                listener.onFailure(e);
            }
        });

        coldStarter.trainModel(Optional.ofNullable(coldStartRequest.getEntity()), configId, modelState, coldStartListener);
    }

    protected abstract ModelState createEmptyState(Entity entity, String modelId, String configId);
}
