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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.util.DateUtils;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class BaseSingleStreamCache<RCFModelType extends ThresholdedRandomCutForest, NodeStateType extends NodeState, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, CheckpointMaintainerType extends CheckpointMaintainWorker<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, CacheBufferType extends SingleStreamCacheBuffer<RCFModelType, NodeStateType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType>>
    implements
        SingleStreamCache<RCFModelType> {

    private static final Logger LOG = LogManager.getLogger(BaseSingleStreamCache.class);

    private final Map<String, CacheBufferType> activeEnities;
    protected final Duration modelTtl;
    private final CheckpointDaoType checkpointDao;
    private final int numberOfTrees;
    protected final MemoryTracker memoryTracker;
    protected Origin origin;
    protected final Clock clock;
    protected int checkpointIntervalHrs;

    public BaseSingleStreamCache(
        MemoryTracker memoryTracker,
        Duration modelTtl,
        Setting<TimeValue> checkpointIntervalSetting,
        Settings settings,
        ClusterService clusterService,
        CheckpointDaoType checkpointDao,
        int numberOfTrees,
        Origin origin,
        Clock clock,
        Setting<TimeValue> checkpointSavingFreq
    ) {
        this.activeEnities = new ConcurrentHashMap<>();
        this.modelTtl = modelTtl;

        this.checkpointDao = checkpointDao;
        this.numberOfTrees = numberOfTrees;
        this.memoryTracker = memoryTracker;
        this.origin = origin;
        this.clock = clock;
        this.checkpointIntervalHrs = DateUtils.toDuration(checkpointSavingFreq.get(settings)).toHoursPart();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(checkpointSavingFreq, it -> {
            this.checkpointIntervalHrs = DateUtils.toDuration(it).toHoursPart();
            this.setCheckpointFreqListener();
        });
    }

    private void setCheckpointFreqListener() {
        activeEnities.values().stream().forEach(cacheBuffer -> cacheBuffer.setCheckpointIntervalHrs(checkpointIntervalHrs));
    }

    @Override
    public void maintenance() {
        try {
            activeEnities.entrySet().stream().forEach(cacheBufferEntry -> {
                String configId = cacheBufferEntry.getKey();
                CacheBufferType cacheBuffer = cacheBufferEntry.getValue();
                // remove expired cache buffer
                if (cacheBuffer.expired(modelTtl)) {
                    activeEnities.remove(configId);
                    cacheBuffer.clear();
                } else {
                    cacheBuffer.maintenance();
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
        if (Strings.isEmpty(configId)) {
            return;
        }
        CacheBufferType buffer = activeEnities.remove(configId);
        if (buffer != null) {
            buffer.clear();
        }
        checkpointDao.deleteModelCheckpointByConfigId(configId);
    }

    @Override
    public Map<String, Long> getModelSize(String configId) {
        CacheBufferType cacheBuffer = activeEnities.get(configId);
        return Map.of(configId, cacheBuffer.getMemoryConsumptionPerModel());
    }

    private CacheBufferType computeBufferIfAbsent(Config config, String configId) {
        CacheBufferType buffer = activeEnities.get(configId);
        if (buffer == null) {
            long requiredBytes = getRequiredMemoryPerEntity(config, memoryTracker, numberOfTrees);
            if (memoryTracker.canAllocateReserved(requiredBytes)) {
                memoryTracker.consumeMemory(requiredBytes, true, origin);
                buffer = createEmptyCacheBuffer(config, requiredBytes);
                activeEnities.put(configId, buffer);
            } else {
                throw new LimitExceededException(configId, CommonMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG);
            }

        }
        return buffer;
    }

    @Override
    public boolean hostIfPossible(Config config, ModelState<RCFModelType> toUpdate) {
        if (toUpdate == null || toUpdate.getModel() == null) {
            return false;
        }
        String modelId = toUpdate.getModelId();
        String detectorId = toUpdate.getConfigId();

        if (Strings.isEmpty(modelId) || Strings.isEmpty(detectorId)) {
            return false;
        }

        CacheBufferType buffer = computeBufferIfAbsent(config, detectorId);
        buffer.setModelState(toUpdate);

        return true;
    }

    @Override
    public ModelState<RCFModelType> get(Config config) {
        String configId = config.getId();
        CacheBufferType buffer = computeBufferIfAbsent(config, configId);
        return buffer.getModelState();
    }

    @Override
    public boolean doesModelExist(String configId) {
        return activeEnities.containsKey(configId);
    }

    @Override
    public int getModelCount() {
        return activeEnities.size();
    }

    @Override
    public long getTotalUpdates(String configId) {
        return Optional.ofNullable(activeEnities.get(configId)).map(cacheBuffer -> getTotalUpdates(cacheBuffer.getModelState())).orElse(0L);
    }

    @Override
    public List<ModelState<RCFModelType>> getAllModels() {
        List<ModelState<RCFModelType>> states = new ArrayList<>();
        activeEnities.values().stream().forEach(cacheBuffer -> states.add(cacheBuffer.getModelState()));
        return states;
    }

    /**
     * Remove model from active entity buffer and delete checkpoint. Used to clean corrupted model.
     * @param configId config Id
     */
    @Override
    public void removeModel(String configId) {
        Optional
            .ofNullable(activeEnities.remove(configId))
            .map(CacheBufferType::getModelState)
            .map(ModelState::getModelId)
            .ifPresent(
                modelId -> checkpointDao
                    .deleteModelCheckpoint(
                        modelId,
                        ActionListener
                            .wrap(
                                r -> LOG.debug(new ParameterizedMessage("Succeeded in deleting checkpoint [{}].", modelId)),
                                e -> LOG.error(new ParameterizedMessage("Failed to delete checkpoint [{}].", modelId), e)
                            )
                    )
            );
    }

    protected abstract CacheBufferType createEmptyCacheBuffer(Config config, long memoryConsumptionPerEntity);
}
