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

import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;


public abstract class TimeSeriesMemoryAwareConcurrentHashmap<K, RCFModelType, ModelState extends TimeSeriesModelState<RCFModelType>> extends ConcurrentHashMap<K, ModelState> {
    protected final MemoryTracker memoryTracker;

    public TimeSeriesMemoryAwareConcurrentHashmap(MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    @Override
    public ModelState remove(Object key) {
        ModelState deletedModelState = super.remove(key);
        if (deletedModelState != null && deletedModelState.getModel() != null) {
            long memoryToRelease = estimateModelSize(deletedModelState.getModel());
            memoryTracker.releaseMemory(memoryToRelease, true, Origin.SINGLE_ENTITY_DETECTOR);
        }
        return deletedModelState;
    }

    @Override
    public ModelState put(K key, ModelState value) {
        ModelState previousAssociatedState = super.put(key, value);
        if (value != null && value.getModel() != null) {
            long memoryToConsume = estimateModelSize(value.getModel());
            memoryTracker.consumeMemory(memoryToConsume, true, Origin.SINGLE_ENTITY_DETECTOR);
        }
        return previousAssociatedState;
    }

    protected abstract long estimateModelSize(RCFModelType model);
}
