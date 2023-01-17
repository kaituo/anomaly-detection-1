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


import java.time.Clock;

import org.opensearch.timeseries.AnalysisModelSize;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.MemoryTracker;


public abstract class ModelManager<RCFModelType, NodeStateType extends ExpiringState, EntityModelStateType extends TimeSeriesModelState<EntityModel<RCFModelType>>> implements AnalysisModelSize {

    public enum ModelType {
        RCF("rcf"),
        THRESHOLD("threshold"),
        ENTITY("entity"),
        RCFCASTER("rcf_caster");

        private String name;

        ModelType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    protected final int rcfNumTrees;
    protected final int rcfNumSamplesInTree;
    protected final double rcfTimeDecay;
    protected final int rcfNumMinSamples;
    protected TimeSeriesColdStarter<NodeStateType> entityColdStarter;
    protected MemoryTracker memoryTracker;
    protected final Clock clock;

    public ModelManager(int rcfNumTrees,
            int rcfNumSamplesInTree,
            double rcfTimeDecay,
            int rcfNumMinSamples,
            TimeSeriesColdStarter<NodeStateType> entityColdStarter,
            MemoryTracker memoryTracker,
            Clock clock) {
        this.rcfNumTrees = rcfNumTrees;
        this.rcfNumSamplesInTree = rcfNumSamplesInTree;
        this.rcfTimeDecay = rcfTimeDecay;
        this.rcfNumMinSamples = rcfNumMinSamples;
        this.entityColdStarter = entityColdStarter;
        this.memoryTracker = memoryTracker;
        this.clock = clock;
    }
}
