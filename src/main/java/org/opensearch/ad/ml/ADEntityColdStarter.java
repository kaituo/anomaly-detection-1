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

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.Optional;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.ADNodeState;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.dataprocessor.InterpolationMethod;
import org.opensearch.timeseries.dataprocessor.Interpolator;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ml.TimeSeriesColdStarter;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.RequestPriority;

import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Training models for HCAD detectors
 *
 */
public class ADEntityColdStarter extends TimeSeriesColdStarter<ADNodeState> {
    private static final Logger logger = LogManager.getLogger(ADEntityColdStarter.class);

    /**
     * Constructor
     *
     * @param clock UTC clock
     * @param threadPool Accessor to different threadpools
     * @param nodeStateManager Storing node state
     * @param rcfSampleSize The sample size used by stream samplers in this forest
     * @param numberOfTrees The number of trees in this forest.
     * @param rcfTimeDecay rcf samples time decay constant
     * @param numMinSamples The number of points required by stream samplers before
     *  results are returned.
     * @param defaultSampleStride default sample distances measured in detector intervals.
     * @param defaultTrainSamples Default train samples to collect.
     * @param interpolator Used to generate data points between samples.
     * @param searchFeatureDao Used to issue ES queries.
     * @param thresholdMinPvalue min P-value for thresholding
     * @param featureManager Used to create features for models.
     * @param settings ES settings accessor
     * @param modelTtl time-to-live before last access time of the cold start cache.
     *   We have a cache to record entities that have run cold starts to avoid
     *   repeated unsuccessful cold start.
     * @param checkpointWriteWorker queue to insert model checkpoints
     * @param rcfSeed rcf random seed
     * @param maxRoundofColdStart max number of rounds of cold start
     */
    public ADEntityColdStarter(
        Clock clock,
        ThreadPool threadPool,
        ADNodeStateManager nodeStateManager,
        int rcfSampleSize,
        int numberOfTrees,
        double rcfTimeDecay,
        int numMinSamples,
        int defaultSampleStride,
        int defaultTrainSamples,
        EnumMap<InterpolationMethod, Interpolator> interpolators,
        SearchFeatureDao searchFeatureDao,
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Settings settings,
        Duration modelTtl,
        ADCheckpointWriteWorker checkpointWriteWorker,
        long rcfSeed,
        int maxRoundofColdStart,
        int coolDownMinutes
    ) {
        super(modelTtl, coolDownMinutes, clock, threadPool, numMinSamples, checkpointWriteWorker, rcfSeed, numberOfTrees, rcfSampleSize, thresholdMinPvalue, rcfTimeDecay,
                nodeStateManager, defaultSampleStride, defaultTrainSamples, interpolators, searchFeatureDao, featureManager, maxRoundofColdStart, TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME);
    }

    public ADEntityColdStarter(
        Clock clock,
        ThreadPool threadPool,
        ADNodeStateManager nodeStateManager,
        int rcfSampleSize,
        int numberOfTrees,
        double rcfTimeDecay,
        int numMinSamples,
        int maxSampleStride,
        int maxTrainSamples,
        EnumMap<InterpolationMethod, Interpolator> interpolators,
        SearchFeatureDao searchFeatureDao,
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Settings settings,
        Duration modelTtl,
        ADCheckpointWriteWorker checkpointWriteQueue,
        int maxRoundofColdStart,
        int coolDownMinutes
    ) {
        this(
            clock,
            threadPool,
            nodeStateManager,
            rcfSampleSize,
            numberOfTrees,
            rcfTimeDecay,
            numMinSamples,
            maxSampleStride,
            maxTrainSamples,
            interpolators,
            searchFeatureDao,
            thresholdMinPvalue,
            featureManager,
            settings,
            modelTtl,
            checkpointWriteQueue,
            -1,
            maxRoundofColdStart,
            coolDownMinutes
        );
    }

    /**
     * Train model using given data points and save the trained model.
     *
     * @param pointSamplePair A pair consisting of a queue of continuous data points,
     *  in ascending order of timestamps and last seen sample.
     * @param entity Entity instance
     * @param entityState Entity state associated with the model Id
     */
    @Override
    protected void trainModelFromDataSegments(
        Pair<double[][], Sample> pointSamplePair,
        Optional<Entity> entity,
        TimeSeriesModelState<?> entityState,
        Config config
    ) {
        if (entity.isEmpty()) {
            throw new IllegalArgumentException("We offer only HC cold start");
        }

        double[][] dataPoints = pointSamplePair.getKey();
        if (dataPoints == null || dataPoints.length == 0) {
            throw new IllegalArgumentException("Data points must not be empty.");
        }

        double[] firstPoint = dataPoints[0];
        if (firstPoint == null || firstPoint.length == 0) {
            throw new IllegalArgumentException("Data points must not be empty.");
        }
        int shingleSize = config.getShingleSize();
        int dimensions = firstPoint.length * shingleSize;
        ThresholdedRandomCutForest.Builder<?> rcfBuilder = ThresholdedRandomCutForest
            .builder()
            .dimensions(dimensions)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .initialAcceptFraction(initialAcceptFraction)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            // same with dimension for opportunistic memory saving
            // Usually, we use it as shingleSize(dimension). When a new point comes in, we will
            // look at the point store if there is any overlapping. Say the previously-stored
            // vector is x1, x2, x3, x4, now we add x3, x4, x5, x6. RCF will recognize
            // overlapping x3, x4, and only store x5, x6.
            .shingleSize(shingleSize)
            .internalShinglingEnabled(true)
            .anomalyRate(1 - this.thresholdMinPvalue);

        if (rcfSeed > 0) {
            rcfBuilder.randomSeed(rcfSeed);
        }
        ThresholdedRandomCutForest trcf = new ThresholdedRandomCutForest(rcfBuilder);

        for (int i=0; i<dataPoints.length; i++) {
            trcf.process(dataPoints[i], 0);
        }

        @SuppressWarnings("unchecked")
        EntityModel<ThresholdedRandomCutForest> model = (EntityModel<ThresholdedRandomCutForest>) entityState.getModel();
        if (model == null) {
            model = new EntityModel<>(entity.get(), new ArrayDeque<>(), null);
        }
        model.setModel(trcf);

        entityState.setLastUsedTime(clock.instant());
        entityState.setLastProcessedSample(pointSamplePair.getValue());

        // save to checkpoint
        checkpointWriteWorker.write(entityState, true, RequestPriority.MEDIUM);
    }

    @Override
    protected boolean isInterpolationInColdStartEnabled() {
        return ADEnabledSetting.isInterpolationInColdStartEnabled();
    }
}
