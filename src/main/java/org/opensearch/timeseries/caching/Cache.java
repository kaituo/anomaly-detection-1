/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.caching;

import java.util.Optional;

import org.opensearch.timeseries.AnalysisModelSize;
import org.opensearch.timeseries.CleanState;
import org.opensearch.timeseries.MaintenanceState;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public interface Cache<RCFModelType extends ThresholdedRandomCutForest> extends MaintenanceState, CleanState, AnalysisModelSize {
    /**
    *
    * @param config Analysis config
    * @param toUpdate Model state candidate
    * @return if we can host the given model state
    */
    boolean hostIfPossible(Config config, ModelState<RCFModelType> toUpdate);

    /**
    *
    * @param config Detector config accessor
    * @param memoryTracker memory tracker
    * @param numberOfTrees number of trees
    * @return Memory in bytes required for hosting one entity model
    */
    default long getRequiredMemoryPerEntity(Config config, MemoryTracker memoryTracker, int numberOfTrees) {
        int dimension = config.getEnabledFeatureIds().size() * config.getShingleSize();
        return memoryTracker
            .estimateTRCFModelSize(
                dimension,
                numberOfTrees,
                TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO,
                config.getShingleSize().intValue(),
                true
            );
    }

    default long getTotalUpdates(ModelState<RCFModelType> modelState) {
        // TODO: make it work for shingles. samples.size() is not the real shingle
        long accumulatedShingles = Optional
            .ofNullable(modelState)
            .flatMap(model -> model.getModel())
            .map(trcf -> trcf.getForest())
            .map(rcf -> rcf.getTotalUpdates())
            .orElseGet(
                () -> Optional
                    .ofNullable(modelState)
                    .map(model -> model.getSamples())
                    .map(samples -> samples.size())
                    .map(Long::valueOf)
                    .orElse(0L)
            );
        return accumulatedShingles;
    }
}
