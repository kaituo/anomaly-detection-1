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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.TimeSeriesColdStarter;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.util.ExceptionUtil;


public abstract class TimeSeriesColdStartWorker <NodeState extends ExpiringState, RCFModelType, ModelStateType extends TimeSeriesModelState<EntityModel<RCFModelType>>> extends SingleRequestWorker<EntityRequest, NodeState> {
    private static final Logger LOG = LogManager.getLogger(TimeSeriesColdStartWorker.class);

    protected final TimeSeriesColdStarter<NodeState> coldStarter;
    protected final CacheProvider<RCFModelType, TimeSeriesModelState<EntityModel<RCFModelType>>> cacheProvider;

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
            TimeSeriesColdStarter<NodeState> coldStarter,
            Duration stateTtl,
            TimeSeriesNodeStateManager<NodeState> nodeStateManager,
            CacheProvider<RCFModelType, TimeSeriesModelState<EntityModel<RCFModelType>>> cacheProvider
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
        String detectorId = coldStartRequest.getId();

        Optional<String> modelId = coldStartRequest.getModelId();

        if (false == modelId.isPresent()) {
            String error = String.format(Locale.ROOT, "Fail to get model id for request %s", coldStartRequest);
            LOG.warn(error);
            listener.onFailure(new RuntimeException(error));
            return;
        }

        ModelStateType modelState = createEmptyState(coldStartRequest.getEntity(), modelId.get(), detectorId);

        ActionListener<Void> coldStartListener = ActionListener.wrap(r -> {
            nodeStateManager.getConfig(detectorId, ActionListener.wrap(detectorOptional -> {
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
                    Config config = detectorOptional.get();
                    EntityModel<RCFModelType> model = modelState.getModel();
                    // load to cache if cold start succeeds
                    if (model != null && model.getModel() != null) {
                        cacheProvider.get().hostIfPossible(config, modelState);
                    }
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
                nodeStateManager.setException(detectorId, e);
            } finally {
                listener.onFailure(e);
            }
        });

        coldStarter.trainModel(Optional.ofNullable(coldStartRequest.getEntity()), detectorId, modelState, coldStartListener);
    }

    protected abstract ModelStateType createEmptyState(Entity entity, String modelId, String configId);
}
