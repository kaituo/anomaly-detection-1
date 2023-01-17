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

package org.opensearch.timeseries.ml;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.timeseries.TimeSeriesMemoryTracker;
import org.opensearch.timeseries.TimeSeriesMemoryTracker.Origin;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A customized ConcurrentHashMap that can automatically consume and release memory.
 * This enables minimum change to our single-stream code as we just have to replace
 * the map implementation.
 *
 * Note: this is mainly used for single-stream configs. The key is model id.
 */
public class TimeSeriesMemoryAwareConcurrentHashmap<RCFModelType extends ThresholdedRandomCutForest, ModelState extends TimeSeriesModelState<RCFModelType>> extends ConcurrentHashMap<String, ModelState> {
    protected final TimeSeriesMemoryTracker memoryTracker;

    public TimeSeriesMemoryAwareConcurrentHashmap(TimeSeriesMemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    @Override
    public ModelState remove(Object key) {
        ModelState deletedModelState = super.remove(key);
        if (deletedModelState != null && deletedModelState.getModel() != null) {
            long memoryToRelease = memoryTracker.estimateTRCFModelSize(deletedModelState.getModel());
            memoryTracker.releaseMemory(memoryToRelease, true, Origin.SINGLE_ENTITY_DETECTOR);
        }
        return deletedModelState;
    }

    @Override
    public ModelState put(String key, ModelState value) {
        ModelState previousAssociatedState = super.put(key, value);
        if (value != null && value.getModel() != null) {
            long memoryToConsume = memoryTracker.estimateTRCFModelSize(value.getModel());
            memoryTracker.consumeMemory(memoryToConsume, true, Origin.SINGLE_ENTITY_DETECTOR);
        }
        return previousAssociatedState;
    }

    /**
     * Gets all of a config's model sizes hosted on a node
     *
     * @param id Analysis Id
     * @return a map of model id to its memory size
     */
    public Map<String, Long> getModelSize(String configId) {
        Map<String, Long> res = new HashMap<>();
        super
            .entrySet()
            .stream()
            .filter(entry -> SingleStreamModelIdMapper.getConfigIdForModelId(entry.getKey()).equals(configId))
            .forEach(entry -> { res.put(entry.getKey(), memoryTracker.estimateTRCFModelSize(entry.getValue().getModel())); });
        return res;
    }

    /**
    * Checks if a model exists for the given config.
    * @param config Config Id
    * @return `true` if the model exists, `false` otherwise.
    */
    public boolean doesModelExist(String configId) {
        return super
        .entrySet()
        .stream()
        .filter(entry -> SingleStreamModelIdMapper.getConfigIdForModelId(entry.getKey()).equals(configId))
        .anyMatch(n -> true);
    }

    public boolean hostIfPossible(String modelId, ModelState toUpdate) {
        return Optional.ofNullable(toUpdate)
                .filter(state -> state.getModel() != null)
                .filter(state -> memoryTracker.isHostingAllowed(modelId, state.getModel()))
                .map(state -> {
                    super.put(modelId, toUpdate);
                    return true;
                })
                .orElse(false);
    }
}
