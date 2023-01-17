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
import java.time.Duration;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.calibration.Calibration;

public class ForecastColdStart extends
    ModelColdStart<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker> {

    private static final Logger logger = LogManager.getLogger(ForecastColdStart.class);

    public ForecastColdStart(
        Clock clock,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager,
        int rcfSampleSize,
        int numberOfTrees,
        int numMinSamples,
        SearchFeatureDao searchFeatureDao,
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Duration modelTtl,
        ForecastCheckpointWriteWorker checkpointWriteWorker,
        int coolDownMinutes,
        long rcfSeed,
        int defaultTrainSamples,
        int maxRoundofColdStart
    ) {
        // 1 means we sample all real data if possible
        super(
            modelTtl,
            coolDownMinutes,
            clock,
            threadPool,
            numMinSamples,
            checkpointWriteWorker,
            rcfSeed,
            numberOfTrees,
            rcfSampleSize,
            thresholdMinPvalue,
            nodeStateManager,
            1,
            defaultTrainSamples,
            searchFeatureDao,
            featureManager,
            maxRoundofColdStart,
            TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME,
            AnalysisType.FORECAST
        );
    }

    @Override
    protected List<Entry<Long, double[]>> trainModelFromDataSegments(
        Pair<List<Entry<Long, double[]>>, Sample> pointSamplePair,
        Optional<Entity> entity,
        ModelState<RCFCaster> modelState,
        Config config,
        String taskId
    ) {
        List<Entry<Long, double[]>> dataPoints = pointSamplePair.getKey();
        if (dataPoints == null || dataPoints.size() == 0) {
            logger.info("Return early since data points must not be empty.");
            return null;
        }

        double[] firstPoint = dataPoints.get(0).getValue();
        if (firstPoint == null || firstPoint.length == 0) {
            logger.info("Return early since data points must not be empty.");
            return null;
        }

        int shingleSize = config.getShingleSize();
        int forecastHorizon = ((Forecaster) config).getHorizon();
        int dimensions = firstPoint.length * shingleSize;

        RCFCaster.Builder casterBuilder = RCFCaster
            .builder()
            .dimensions(dimensions)
            .numberOfTrees(numberOfTrees)
            .shingleSize(shingleSize)
            .sampleSize(rcfSampleSize)
            .internalShinglingEnabled(true)
            .precision(Precision.FLOAT_32)
            .anomalyRate(1 - this.thresholdMinPvalue)
            .outputAfter(numMinSamples)
            .calibration(Calibration.MINIMAL)
            .timeDecay(config.getTimeDecay())
            .parallelExecutionEnabled(false)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            // the following affects the moving average in many of the transformations
            // the 0.02 corresponds to a half life of 1/0.02 = 50 observations
            // this is different from the timeDecay() of RCF; however it is a similar
            // concept
            .transformDecay(config.getTimeDecay())
            .forecastHorizon(forecastHorizon)
            .initialAcceptFraction(initialAcceptFraction)
            // normalize transform is required to deal with trend change in forecasting
            .transformMethod(TransformMethod.NORMALIZE);

        if (rcfSeed > 0) {
            casterBuilder.randomSeed(rcfSeed);
        }

        RCFCaster caster = casterBuilder.build();

        for (int i = 0; i < dataPoints.size(); i++) {
            double[] dataValue = dataPoints.get(i).getValue();
            caster.process(dataValue, 0);
        }

        modelState.setModel(caster);
        modelState.setLastUsedTime(clock.instant());
        modelState.setLastProcessedSample(pointSamplePair.getValue());
        // save to checkpoint for real time cold start that has no taskId
        if (null == taskId) {
            checkpointWriteWorker.write(modelState, true, RequestPriority.MEDIUM);
        }
        return dataPoints;
    }

    @Override
    protected boolean isInterpolationInColdStartEnabled() {
        return false;
    }
}
