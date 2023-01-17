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
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.Context;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.util.ExceptionUtil;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class ColdStartWorker<RCFModelType extends ThresholdedRandomCutForest, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType>, CacheType extends TimeSeriesCache<RCFModelType>>
    extends SingleRequestWorker<FeatureRequest> {
    private static final Logger LOG = LogManager.getLogger(ColdStartWorker.class);

    protected final ColdStarterType coldStarter;
    protected final CacheType cacheProvider;

    public ColdStartWorker(
        String workerName,
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
        Setting<Integer> concurrency,
        Duration executionTtl,
        ColdStarterType coldStarter,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        CacheType cacheProvider,
        Context context
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
            nodeStateManager,
            context
        );
        this.coldStarter = coldStarter;
        this.cacheProvider = cacheProvider;

    }

    @Override
    protected void executeRequest(FeatureRequest coldStartRequest, ActionListener<Void> listener) {
        String configId = coldStartRequest.getConfigId();

        String modelId = coldStartRequest.getModelId();

        if (null == modelId) {
            String error = String.format(Locale.ROOT, "Fail to get model id for request %s", coldStartRequest);
            LOG.warn(error);
            listener.onFailure(new RuntimeException(error));
            return;
        }

        ModelState<RCFModelType> modelState = createEmptyState(coldStartRequest, modelId, configId);

        ActionListener<Void> coldStartListener = ActionListener.wrap(r -> {
            nodeStateManager.getConfig(configId, context, ActionListener.wrap(configOptional -> {
                try {
                    if (!configOptional.isPresent()) {
                        LOG
                            .error(
                                new ParameterizedMessage(
                                    "fail to load trained model [{}] to cache due to the config not being found.",
                                    modelState.getModelId()
                                )
                            );
                        return;
                    }
                    cacheProvider.hostIfPossible(configOptional.get(), modelState);

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

        coldStarter.trainModel(coldStartRequest.getEntity(), configId, modelState, coldStartListener);
    }

    protected abstract ModelState<RCFModelType> createEmptyState(FeatureRequest coldStartRequest, String modelId, String configId);
}
