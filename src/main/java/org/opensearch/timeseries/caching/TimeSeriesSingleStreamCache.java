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

package org.opensearch.timeseries.caching;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesMemoryTracker;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.TimeSeriesCheckpointDao;
import org.opensearch.timeseries.ml.TimeSeriesColdStarter;
import org.opensearch.timeseries.ml.TimeSeriesMemoryAwareConcurrentHashmap;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointWriteWorker;
import org.opensearch.timeseries.util.DateUtils;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;


public abstract class TimeSeriesSingleStreamCache<
    RCFModelType extends ThresholdedRandomCutForest,
    ModelState extends TimeSeriesModelState<RCFModelType>,
    NodeStateType extends ExpiringState,
    CheckpointDaoType extends TimeSeriesCheckpointDao<RCFModelType>,
    CheckpointWriterType extends TimeSeriesCheckpointWriteWorker<NodeStateType, RCFModelType, CheckpointDaoType>,
    ResultType extends IntermediateResult,
    ColdStarterType extends TimeSeriesColdStarter<NodeStateType, RCFModelType, CheckpointDaoType, CheckpointWriterType>,
    ModelManagerType extends ModelManager<RCFModelType, NodeStateType, ResultType, CheckpointDaoType, CheckpointWriterType, ColdStarterType>>
    implements SingleStreamCache<RCFModelType, ModelState> {

    private final Logger LOG = LogManager.getLogger(TimeSeriesSingleStreamCache.class);

    protected TimeSeriesMemoryAwareConcurrentHashmap<RCFModelType, ModelState> models;
    protected final Duration modelTtl;
    protected final Clock clock;
    protected Duration checkpointInterval;
    protected CheckpointWriterType checkpointWriter;
    private final ModelManagerType modelManager;

    public TimeSeriesSingleStreamCache(TimeSeriesMemoryTracker memoryTracker, Duration modelTtl, Clock clock, Setting<TimeValue> checkpointIntervalSetting,
            Settings settings, ClusterService clusterService,
            CheckpointWriterType checkpointWriter, ModelManagerType modelManager) {
        this.models = new TimeSeriesMemoryAwareConcurrentHashmap<RCFModelType, ModelState>(memoryTracker);
        this.modelTtl = modelTtl;
        this.clock = clock;
        this.checkpointInterval = DateUtils.toDuration(checkpointIntervalSetting.get(settings));
        if (clusterService != null) {
            clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(checkpointIntervalSetting, it -> this.checkpointInterval = DateUtils.toDuration(it));
        }
        this.checkpointWriter = checkpointWriter;
        this.modelManager = modelManager;
    }

    @Override
    public void maintenance() {
        Instant now = clock.instant();
        try {
            models.entrySet().stream().forEach(entry -> {
                String configId = entry.getKey();
                ModelState modelState = entry.getValue();
                // remove expired cache buffer
                if (modelState.expired(modelTtl)) {
                    models.remove(configId);
                } else if (modelState.getLastCheckpointTime().plus(checkpointInterval).isBefore(now)) {
                    checkpointWriter.write(modelState, false, RequestPriority.MEDIUM);
                }
            });
        } catch (Exception e) {
            // will be thrown to ES's transport broadcast handler
            throw new TimeSeriesException("Fail to maintain cache", e);
        }

    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     * When stop realtime job, will call this method to clear all model cache
     * and checkpoints.
     *
     * @param configId id the of the config for which models are to be permanently deleted
     */
    @Override
    public void clear(String configId) {
        modelManager.clearModels(configId, models, ActionListener
                    .wrap(
                        r -> LOG.info("Deleted model for [{}] with response [{}] ", configId, r),
                        e -> LOG.error("Fail to delete model for " + configId, e)
                    ));
    }

    @Override
    public Map<String, Long> getModelSize(String configId) {
        return models.getModelSize(configId);
    }

    @Override
    public boolean doesModelExist(String configId) {
        return models.doesModelExist(configId);
    }

    @Override
    public int getModelCount() {
        return models.size();
    }

    @Override
    public long getTotalUpdates(String modelId) {
        // use the Optional class to handle null values in the call chain
        Optional<ModelState> stateOptional = Optional.ofNullable(models.get(modelId));
        return stateOptional.map(ModelState::getModel)
                            .map(RCFModelType::getForest)
                            .map(RandomCutForest::getTotalUpdates)
                            .orElse(0L);
    }

    @Override
    public boolean hostIfPossible(String modelId, ModelState toUpdate) {
        return models.hostIfPossible(modelId, toUpdate);
    }

    @Override
    public ModelState get(String modelId) {
        return models.get(modelId);
    }

    @Override
    public List<ModelState> getAllModels() {
        return models.values().stream().collect(Collectors.toList());
    }
}
