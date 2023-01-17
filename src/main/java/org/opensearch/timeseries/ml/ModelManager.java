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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class ModelManager<RCFModelType extends ThresholdedRandomCutForest, NodeStateType extends NodeState, ResultType extends IntermediateResult, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<NodeStateType, RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType>> {

    private static final Logger LOG = LogManager.getLogger(ModelManager.class);

    public enum ModelType {
        RCF("rcf"),
        THRESHOLD("threshold"),
        TRCF("trcf"),
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
    protected ColdStarterType coldStarter;
    protected MemoryTracker memoryTracker;
    protected final Clock clock;
    protected FeatureManager featureManager;
    protected final CheckpointDaoType checkpointDao;

    public ModelManager(
        int rcfNumTrees,
        int rcfNumSamplesInTree,
        double rcfTimeDecay,
        int rcfNumMinSamples,
        ColdStarterType coldStarter,
        MemoryTracker memoryTracker,
        Clock clock,
        FeatureManager featureManager,
        CheckpointDaoType checkpointDao
    ) {
        this.rcfNumTrees = rcfNumTrees;
        this.rcfNumSamplesInTree = rcfNumSamplesInTree;
        this.rcfTimeDecay = rcfTimeDecay;
        this.rcfNumMinSamples = rcfNumMinSamples;
        this.coldStarter = coldStarter;
        this.memoryTracker = memoryTracker;
        this.clock = clock;
        this.featureManager = featureManager;
        this.checkpointDao = checkpointDao;
    }

    public ResultType getResult(
        Sample sample,
        ModelState<RCFModelType> modelState,
        String modelId,
        Optional<Entity> entity,
        Config config
    ) {
        ResultType result = createEmptyResult();
        if (modelState != null) {
            Optional<RCFModelType> entityModel = modelState.getModel();

            if (entityModel.isEmpty()) {
                coldStarter.trainModelFromExistingSamples(modelState, entity, config);
            }

            if (modelState.getModel().isPresent()) {
                result = score(sample, modelId, modelState, config);
            } else {
                modelState.addSample(sample);
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

    @SuppressWarnings("unchecked")
    public <RCFDescriptor extends AnomalyDescriptor> ResultType score(
        Sample sample,
        String modelId,
        ModelState<RCFModelType> modelState,
        Config config
    ) {

        ResultType result = createEmptyResult();
        Optional<RCFModelType> model = modelState.getModel();
        try {
            if (model != null && model.isPresent()) {
                RCFModelType rcfModel = model.get();

                Pair<double[][], Sample> dataSamplePair = featureManager
                    .getContinuousSamples(config, modelState.getSamples(), modelState.getLastProcessedSample(), sample);

                double[][] data = dataSamplePair.getKey();
                RCFDescriptor lastResult = null;
                for (int i = 0; i < data.length; i++) {
                    // we are sure that the process method will indeed return an instance of RCFDescriptor.
                    lastResult = (RCFDescriptor) rcfModel.process(data[i], 0);
                }
                modelState.clearSamples();

                result = toResult(rcfModel.getForest(), lastResult);
                modelState.setLastProcessedSample(dataSamplePair.getValue());
            }
        } catch (Exception e) {
            LOG
                .error(
                    new ParameterizedMessage(
                        "Fail to score for [{}]: model Id [{}], feature [{}]",
                        modelState.getEntity().isEmpty() ? modelState.getConfigId() : modelState.getEntity().get(),
                        modelId,
                        Arrays.toString(sample.getValueList())
                    ),
                    e
                );
            throw e;
        } finally {
            modelState.setLastUsedTime(clock.instant());
        }
        return result;
    }

    protected abstract ResultType createEmptyResult();

    protected abstract <RCFDescriptor extends AnomalyDescriptor> ResultType toResult(
        RandomCutForest forecast,
        RCFDescriptor castDescriptor
    );
}
