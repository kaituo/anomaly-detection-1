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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.util.DataUtil;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class ModelManager<RCFModelType extends ThresholdedRandomCutForest, IndexableResultType extends IndexableResult, IntermediateResultType extends IntermediateResult<IndexableResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, CheckpointDaoType extends CheckpointDaoInterface<RCFModelType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, DataManagementType, IndexableResultType>> {

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
    protected final int rcfNumMinSamples;
    protected ColdStarterType coldStarter;
    protected MemoryTracker memoryTracker;
    protected final Clock clock;
    protected FeatureManager featureManager;
    protected final CheckpointDaoType checkpointDao;
    protected final StateManager nodeStateManager;
    protected final AnalysisType analysisType;

    public ModelManager(
        int rcfNumTrees,
        int rcfNumSamplesInTree,
        int rcfNumMinSamples,
        ColdStarterType coldStarter,
        MemoryTracker memoryTracker,
        Clock clock,
        FeatureManager featureManager,
        CheckpointDaoType checkpointDao,
        StateManager nodeStateManager,
        AnalysisType analysisType
    ) {
        this.rcfNumTrees = rcfNumTrees;
        this.rcfNumSamplesInTree = rcfNumSamplesInTree;
        this.rcfNumMinSamples = rcfNumMinSamples;
        this.coldStarter = coldStarter;
        this.memoryTracker = memoryTracker;
        this.clock = clock;
        this.featureManager = featureManager;
        this.checkpointDao = checkpointDao;
        this.nodeStateManager = nodeStateManager;
        this.analysisType = analysisType;
    }

    public IntermediateResultType getResult(
        Sample sample,
        ModelState<RCFModelType> modelState,
        String modelId,
        Config config,
        String taskId
    ) {
        IntermediateResultType result = createEmptyResult();
        if (modelState != null) {
            Optional<RCFModelType> entityModel = modelState.getModel();

            if (entityModel.isEmpty()) {
                coldStarter.trainModelFromExistingSamples(modelState, config, taskId);
            }

            if (modelState.getModel().isPresent()) {
                result = score(sample, modelId, modelState, config);
            } else {
                modelState.addSample(sample);
            }
        }
        return result;
    }

    public void clearModels(String detectorId, String tenantId, Map<String, ?> models, ActionListener<Void> listener) {
        List<String> modelIdsToDelete = new ArrayList<>();
        for (Map.Entry<String, ?> modelEntry : models.entrySet()) {
            String modelId = modelEntry.getKey();
            if (isModelForConfig(detectorId, tenantId, modelId, modelEntry.getValue())) {
                modelIdsToDelete.add(modelId);
            }
        }

        if (modelIdsToDelete.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        for (String modelId : modelIdsToDelete) {
            models.remove(modelId);
        }

        nodeStateManager.getConfig(detectorId, tenantId, analysisType, false, ActionListener.wrap(config -> {
            if (config.isPresent()) {
                clearCheckpointForIterator(config.get(), modelIdsToDelete.iterator(), listener);
            } else {
                LOG.debug("Skipping checkpoint deletion for [{}] because the config no longer exists", detectorId);
                listener.onResponse(null);
            }
        }, e -> {
            LOG
                .warn(
                    new ParameterizedMessage(
                        "Removed in-memory models for [{}] but failed to load config for checkpoint deletion",
                        detectorId
                    ),
                    e
                );
            listener.onResponse(null);
        }));
    }

    private void clearCheckpointForIterator(Config config, Iterator<String> idIter, ActionListener<Void> listener) {
        if (idIter.hasNext()) {
            String modelId = idIter.next();
            checkpointDao
                .deleteModelCheckpoint(
                    config,
                    modelId,
                    ActionListener.wrap(r -> clearCheckpointForIterator(config, idIter, listener), listener::onFailure)
                );
        } else {
            listener.onResponse(null);
        }
    }

    private boolean isModelForConfig(String detectorId, String tenantId, String modelId, Object modelState) {
        if (modelState instanceof ModelState<?> state && state.getConfigId() != null) {
            if (detectorId.equals(state.getConfigId()) == false) {
                return false;
            }
            return tenantMatches(tenantId, state.getTenantId());
        }

        return modelIdMatchesConfig(detectorId, modelId);
    }

    private boolean tenantMatches(String expectedTenantId, String modelTenantId) {
        if (expectedTenantId == null || expectedTenantId.isEmpty()) {
            return true;
        }
        if (modelTenantId == null || modelTenantId.isEmpty()) {
            return true;
        }
        return expectedTenantId.equals(modelTenantId);
    }

    private boolean modelIdMatchesConfig(String detectorId, String modelId) {
        try {
            return detectorId.equals(SingleStreamModelIdMapper.getConfigIdForModelId(modelId));
        } catch (IllegalArgumentException e) {
            String entityModelInfix = "_entity_";
            int entityModelInfixPosition = modelId.lastIndexOf(entityModelInfix);
            if (entityModelInfixPosition <= 0 || entityModelInfixPosition + entityModelInfix.length() >= modelId.length()) {
                LOG.debug("Skipping unrecognized model id [{}] while clearing [{}]", modelId, detectorId, e);
                return false;
            }
            return detectorId.equals(modelId.substring(0, entityModelInfixPosition));
        }
    }

    public <RCFDescriptor extends AnomalyDescriptor> IntermediateResultType score(
        Sample sample,
        String modelId,
        ModelState<RCFModelType> modelState,
        Config config
    ) {
        Optional<RCFModelType> model = modelState.getModel();
        try {
            if (model != null && model.isPresent()) {
                RCFModelType rcfModel = model.get();

                if (!modelState.getSamples().isEmpty()) {
                    for (Sample unProcessedSample : modelState.getSamples()) {
                        // we are sure that the process method will indeed return an instance of RCFDescriptor.
                        double[] unProcessedPoint = unProcessedSample.getValueList();
                        int[] missingIndices = DataUtil.generateMissingIndicesArray(unProcessedPoint);
                        rcfModel.process(unProcessedPoint, unProcessedSample.getDataEndTime().getEpochSecond(), missingIndices);
                        modelState.setLastProcessedDataEndTime(unProcessedSample.getDataEndTime());
                    }
                    modelState.clearSamples();
                }

                IntermediateResultType result = score(sample, config, rcfModel);
                modelState.setLastProcessedDataEndTime(sample.getDataEndTime());
                return result;
            }
        } catch (Exception e) {
            LOG
                .error(
                    new ParameterizedMessage(
                        "Fail to score for [{}] at [{}]: model Id [{}], feature [{}]",
                        modelState.getEntity().isEmpty() ? modelState.getConfigId() : modelState.getEntity().get(),
                        sample.getDataEndTime().getEpochSecond(),
                        modelId,
                        Arrays.toString(sample.getValueList())
                    ),
                    e
                );
            throw e;
        }
        return createEmptyResult();
    }

    @SuppressWarnings("unchecked")
    public <RCFDescriptor extends AnomalyDescriptor> IntermediateResultType score(Sample sample, Config config, RCFModelType rcfModel) {
        double[] point = sample.getValueList();

        int[] missingValues = DataUtil.generateMissingIndicesArray(point);
        RCFDescriptor lastResult = (RCFDescriptor) rcfModel.process(point, sample.getDataEndTime().getEpochSecond(), missingValues);
        if (lastResult != null) {
            return toResult(rcfModel.getForest(), lastResult, point, missingValues != null, config);
        }
        return createEmptyResult();
    }

    protected abstract IntermediateResultType createEmptyResult();

    protected abstract <RCFDescriptor extends AnomalyDescriptor> IntermediateResultType toResult(
        RandomCutForest forecast,
        RCFDescriptor castDescriptor,
        double[] point,
        boolean featureImputed,
        Config config
    );
}
