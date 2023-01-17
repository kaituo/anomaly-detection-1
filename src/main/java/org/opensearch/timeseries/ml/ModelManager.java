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
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.opensearch.action.ActionListener;
import org.opensearch.timeseries.AnalysisModelSize;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesMemoryTracker;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.TimeSeriesCheckpointWriteWorker;


public abstract class ModelManager<RCFModelType, NodeStateType extends ExpiringState,
    ResultType extends IntermediateResult, CheckpointDaoType extends TimeSeriesCheckpointDao<RCFModelType>,
    CheckpointWriteWorkerType extends TimeSeriesCheckpointWriteWorker<NodeStateType, RCFModelType, CheckpointDaoType>,
    ColdStarterType extends TimeSeriesColdStarter<NodeStateType, RCFModelType, CheckpointDaoType, CheckpointWriteWorkerType>> implements AnalysisModelSize {

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
    protected ColdStarterType entityColdStarter;
    protected TimeSeriesMemoryTracker memoryTracker;
    protected final Clock clock;
    protected FeatureManager featureManager;
    protected final CheckpointDaoType checkpointDao;

    public ModelManager(int rcfNumTrees,
            int rcfNumSamplesInTree,
            double rcfTimeDecay,
            int rcfNumMinSamples,
            ColdStarterType entityColdStarter,
            TimeSeriesMemoryTracker memoryTracker,
            Clock clock,
            FeatureManager featureManager,
            CheckpointDaoType checkpointDao) {
        this.rcfNumTrees = rcfNumTrees;
        this.rcfNumSamplesInTree = rcfNumSamplesInTree;
        this.rcfTimeDecay = rcfTimeDecay;
        this.rcfNumMinSamples = rcfNumMinSamples;
        this.entityColdStarter = entityColdStarter;
        this.memoryTracker = memoryTracker;
        this.clock = clock;
        this.featureManager = featureManager;
        this.checkpointDao = checkpointDao;
    }

    public <EntityModelStateType extends TimeSeriesModelState<EntityModel<RCFModelType>>> ResultType getResultForEntity(
            Sample sample,
            EntityModelStateType modelState,
            String modelId,
            Entity entity,
            Config config) {
        ResultType result = createEmptyResult();
            if (modelState != null) {
                EntityModel<RCFModelType> entityModel = modelState.getModel();

                if (entityModel == null) {
                    entityModel = new EntityModel<>(entity, new ArrayDeque<>(), null);
                    modelState.setModel(entityModel);
                }

                if (!entityModel.getModel().isPresent()) {
                    entityColdStarter.trainModelFromExistingSamples(modelState.getModel().getSamples(), modelState, Optional.ofNullable(entity), config);
                }

                if (entityModel.getModel().isPresent()) {
                    result = score(sample, modelId, modelState, config);
                } else {
                    entityModel.addSample(sample);
                }
            }
            return result;
    }

    public void clearModels(String detectorId, Map<String, ?> models, ActionListener<Void> listener) {
        Iterator<String> id = models.keySet().iterator();
        clearModelForIterator(detectorId, models, id, listener);
    }

    protected void clearModelForIterator(String detectorId, Map<String, ?> models, Iterator<String> idIter, ActionListener<Void> listener) {
        if (idIter.hasNext()) {
            String modelId = idIter.next();
            if (SingleStreamModelIdMapper.getConfigIdForModelId(modelId).equals(detectorId)) {
                models.remove(modelId);
                checkpointDao
                    .deleteModelCheckpoint(
                        modelId,
                        ActionListener.wrap(r -> clearModelForIterator(detectorId, models, idIter, listener), listener::onFailure)
                    );
            } else {
                clearModelForIterator(detectorId, models, idIter, listener);
            }
        } else {
            listener.onResponse(null);
        }
    }

    protected abstract ResultType createEmptyResult();

    protected abstract <EntityModelStateType extends TimeSeriesModelState<EntityModel<RCFModelType>>> ResultType score(Sample sample, String modelId,
            EntityModelStateType modelState, Config config);
}
