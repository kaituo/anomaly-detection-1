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

package org.opensearch.ad.ml;


import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.ml.TimeSeriesMemoryAwareConcurrentHashmap;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A customized ConcurrentHashMap that can automatically consume and release memory.
 * This enables minimum change to our single-entity code as we just have to replace
 * the map implementation.
 *
 * Note: this is mainly used for single-entity detectors.
 */
public class TRCFMemoryAwareConcurrentHashmap<K> extends TimeSeriesMemoryAwareConcurrentHashmap<K, ThresholdedRandomCutForest, ADModelState<ThresholdedRandomCutForest>> {

    public TRCFMemoryAwareConcurrentHashmap(MemoryTracker memoryTracker) {
        super(memoryTracker);
    }

    @Override
    protected long estimateModelSize(ThresholdedRandomCutForest model) {
        return memoryTracker.estimateTRCFModelSize(model);
    }
}
