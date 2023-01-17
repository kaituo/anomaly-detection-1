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

package org.opensearch.forecast.ml;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastModelManager extends ModelManager<RCFCaster, ForecastNodeState, ForecastModelState<EntityModel<RCFCaster>>> {
    private RCFCasterMemoryAwareConcurrentHashmap<String> forests;

    public ForecastModelManager(
            int rcfNumTrees,
            int rcfNumSamplesInTree,
            double rcfTimeDecay,
            int rcfNumMinSamples,
            ForecastColdStarter entityColdStarter,
            MemoryTracker memoryTracker,
            Clock clock
            ) {
        super(rcfNumTrees, rcfNumSamplesInTree, rcfTimeDecay, rcfNumMinSamples, entityColdStarter, memoryTracker, clock);
        this.forests = new RCFCasterMemoryAwareConcurrentHashmap<String>(memoryTracker);
    }

    @Override
    public Map<String, Long> getModelSize(String configId) {
        Map<String, Long> res = new HashMap<>();
        forests
            .entrySet()
            .stream()
            .filter(entry -> SingleStreamModelIdMapper.getConfigIdForModelId(entry.getKey()).equals(configId))
            .forEach(entry -> { res.put(entry.getKey(), memoryTracker.estimateRCFCasterModelSize(entry.getValue().getModel())); });
        return res;
    }

}
