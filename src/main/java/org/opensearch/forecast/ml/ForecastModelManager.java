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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.forecast.ForecastNodeState;
import org.opensearch.timeseries.TimeSeriesMemoryTracker;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.ForecastDescriptor;
import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastModelManager extends ModelManager<RCFCaster, ForecastNodeState, ForecastModelState<EntityModel<RCFCaster>>, RCFCasterResult, ForecastCheckpointDao> {
    private static final Logger logger = LogManager.getLogger(ForecastModelManager.class);

    private RCFCasterMemoryAwareConcurrentHashmap<String> forests;

    public ForecastModelManager(
            int rcfNumTrees,
            int rcfNumSamplesInTree,
            double rcfTimeDecay,
            int rcfNumMinSamples,
            ForecastColdStarter entityColdStarter,
            TimeSeriesMemoryTracker memoryTracker,
            Clock clock,
            FeatureManager featureManager
            ) {
        super(rcfNumTrees, rcfNumSamplesInTree, rcfTimeDecay, rcfNumMinSamples, entityColdStarter, memoryTracker, clock, featureManager);
        this.forests = new RCFCasterMemoryAwareConcurrentHashmap<String>(memoryTracker);
    }

    @Override
    public Map<String, Long> getModelSize(String configId) {
        Map<String, Long> res = new HashMap<>();
        forests
            .entrySet()
            .stream()
            .filter(entry -> SingleStreamModelIdMapper.getConfigIdForModelId(entry.getKey()).equals(configId))
            .forEach(entry -> { res.put(entry.getKey(), memoryTracker.estimateTRCFModelSize(entry.getValue().getModel())); });
        return res;
    }

    @Override
    protected RCFCasterResult createEmptyResult() {
        return new RCFCasterResult(null, 0, 0, 0);
    }

    @Override
    public RCFCasterResult score(Sample sample, String modelId,
            ForecastModelState<EntityModel<RCFCaster>> modelState, Config config) {
        RCFCasterResult result = createEmptyResult();
        EntityModel<RCFCaster> model = modelState.getModel();
        try {
            if (model != null && model.getModel().isPresent()) {
                RCFCaster rcfCaster = model.getModel().get();

                Pair<double[][], Sample> dataSamplePair = featureManager.getContinuousSamples(config,
                        model.getSamples(), modelState.getLastProcessedSample(), sample);

                double[][] data = dataSamplePair.getKey();
                ForecastDescriptor lastResult = null;
                for (int i = 0; i < data.length; i++) {
                    lastResult = rcfCaster.process(data[i], 0);
                }
                model.clearSamples();

                result = toResult(rcfCaster.getForest(), lastResult);
                modelState.setLastProcessedSample(dataSamplePair.getValue());
            }
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("Fail to score for [{}]: model Id [{}], feature [{}]",
                    modelState.getModel().getEntity(), modelId, Arrays.toString(sample.getValueList())), e);
            throw e;
        } finally {
            modelState.setLastUsedTime(clock.instant());
        }
        return result;
    }

    private RCFCasterResult toResult(RandomCutForest forecast, ForecastDescriptor castDescriptor) {
        return new RCFCasterResult(castDescriptor.getTimedForecast().rangeVector, castDescriptor.getDataConfidence(),
                forecast.getTotalUpdates(), castDescriptor.getRCFScore());
    }
}
