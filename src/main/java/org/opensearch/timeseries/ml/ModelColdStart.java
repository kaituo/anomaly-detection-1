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
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.CleanState;
import org.opensearch.timeseries.MaintenanceState;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.caching.DoorKeeper;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ExceptionUtil;

/**
 * The class bootstraps a model by performing a cold start
 *
 * @param <NodeStateType> Node state type
 * @param <RCFModelType> RCF model type
 * @param <CheckpointDaoType> CheckpointDao type
 * @param <CheckpointWriteWorkerType> CheckpointWriteWorkerType
 */
public abstract class ModelColdStart<RCFModelType, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>>
    implements
        MaintenanceState,
        CleanState {
    private static final Logger logger = LogManager.getLogger(ModelColdStart.class);

    private final Duration modelTtl;

    // A bloom filter checked before cold start to ensure we don't repeatedly
    // retry cold start of the same model.
    // keys are detector ids.
    protected Map<String, DoorKeeper> doorKeepers;
    protected Instant lastThrottledColdStartTime;
    protected int coolDownMinutes;
    protected final Clock clock;
    protected final ThreadPool threadPool;
    protected final int numMinSamples;
    protected CheckpointWriteWorkerType checkpointWriteWorker;
    // make sure rcf use a specific random seed. Otherwise, we will use a random random (not a typo) seed.
    // this is mainly used for testing to make sure the model we trained and the reference rcf produce
    // the same results
    protected final long rcfSeed;
    protected final int numberOfTrees;
    protected final int rcfSampleSize;
    protected final double thresholdMinPvalue;
    protected final double rcfTimeDecay;
    protected final double initialAcceptFraction;
    protected final NodeStateManager nodeStateManager;
    protected final int defaulStrideLength;
    protected final int defaultNumberOfSamples;
    protected final SearchFeatureDao searchFeatureDao;
    protected final FeatureManager featureManager;
    protected final int maxRoundofColdStart;
    protected final String threadPoolName;
    protected final AnalysisType context;

    public ModelColdStart(
        Duration modelTtl,
        int coolDownMinutes,
        Clock clock,
        ThreadPool threadPool,
        int numMinSamples,
        CheckpointWriteWorkerType checkpointWriteWorker,
        long rcfSeed,
        int numberOfTrees,
        int rcfSampleSize,
        double thresholdMinPvalue,
        double rcfTimeDecay,
        NodeStateManager nodeStateManager,
        int defaultSampleStride,
        int defaultTrainSamples,
        SearchFeatureDao searchFeatureDao,
        FeatureManager featureManager,
        int maxRoundofColdStart,
        String threadPoolName,
        AnalysisType context
    ) {
        this.modelTtl = modelTtl;
        this.coolDownMinutes = coolDownMinutes;
        this.clock = clock;
        this.threadPool = threadPool;
        this.numMinSamples = numMinSamples;
        this.checkpointWriteWorker = checkpointWriteWorker;
        this.rcfSeed = rcfSeed;
        this.numberOfTrees = numberOfTrees;
        this.rcfSampleSize = rcfSampleSize;
        this.thresholdMinPvalue = thresholdMinPvalue;
        this.rcfTimeDecay = rcfTimeDecay;

        this.doorKeepers = new ConcurrentHashMap<>();
        this.lastThrottledColdStartTime = Instant.MIN;
        this.initialAcceptFraction = numMinSamples * 1.0d / rcfSampleSize;

        this.nodeStateManager = nodeStateManager;
        this.defaulStrideLength = defaultSampleStride;
        this.defaultNumberOfSamples = defaultTrainSamples;
        this.searchFeatureDao = searchFeatureDao;
        this.featureManager = featureManager;
        this.maxRoundofColdStart = maxRoundofColdStart;
        this.threadPoolName = threadPoolName;
        this.context = context;
    }

    @Override
    public void maintenance() {
        doorKeepers.entrySet().stream().forEach(doorKeeperEntry -> {
            String id = doorKeeperEntry.getKey();
            DoorKeeper doorKeeper = doorKeeperEntry.getValue();
            if (doorKeeper.expired(modelTtl)) {
                doorKeepers.remove(id);
            } else {
                doorKeeper.maintenance();
            }
        });
    }

    @Override
    public void clear(String id) {
        doorKeepers.remove(id);
    }

    /**
     * Train models
     * @param entity The entity info if we are training for an HC entity
     * @param configId Config Id
     * @param modelState Model state
     * @param listener callback before the method returns whenever ColdStarter
     * finishes training or encounters exceptions.  The listener helps notify the
     * cold start queue to pull another request (if any) to execute.
     */
    public void trainModel(Optional<Entity> entity, String configId, ModelState<RCFModelType> modelState, ActionListener<Void> listener) {
        nodeStateManager.getConfig(configId, context, ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                logger.warn(new ParameterizedMessage("Config [{}] is not available.", configId));
                listener.onFailure(new TimeSeriesException(configId, "fail to find config"));
                return;
            }

            Config config = detectorOptional.get();

            String modelId = modelState.getModelId();

            if (modelState.getSamples().size() < this.numMinSamples) {
                // we cannot get last RCF score since cold start happens asynchronously
                coldStart(modelId, entity, modelState, config, listener);
            } else {
                try {
                    trainModelFromExistingSamples(modelState, entity, config);
                    listener.onResponse(null);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }, listener::onFailure));
    }

    public void trainModelFromExistingSamples(ModelState<RCFModelType> modelState, Optional<Entity> entity, Config config) {
        Pair<double[][], Sample> continuousSamples = featureManager.getContinuousSamples(config, modelState.getSamples());
        trainModelFromDataSegments(continuousSamples, entity, modelState, config);
    }

    /**
     * Training model
     * @param modelId model Id corresponding to the entity
     * @param entity the entity's information if we are training for HC entity
     * @param modelState model state
     * @param config config accessor
     * @param listener call back to call after cold start
     */
    private void coldStart(
        String modelId,
        Optional<Entity> entity,
        ModelState<RCFModelType> modelState,
        Config config,
        ActionListener<Void> listener
    ) {
        logger.debug("Trigger cold start for {}", modelId);

        if (modelState == null) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT, "Cannot have empty model state")));
            return;
        }

        if (lastThrottledColdStartTime.plus(Duration.ofMinutes(coolDownMinutes)).isAfter(clock.instant())) {
            listener.onResponse(null);
            return;
        }

        String configId = config.getId();
        boolean earlyExit = true;
        try {
            DoorKeeper doorKeeper = doorKeepers
                .computeIfAbsent(
                    configId,
                    id -> {
                        // reset every 60 intervals
                        return new DoorKeeper(
                            TimeSeriesSettings.DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION,
                            TimeSeriesSettings.DOOR_KEEPER_FALSE_POSITIVE_RATE,
                            config.getIntervalDuration().multipliedBy(TimeSeriesSettings.DOOR_KEEPER_MAINTENANCE_FREQ),
                            clock
                        );
                    }
                );

            // Won't retry cold start within 60 intervals for an entity
            if (doorKeeper.mightContain(modelId)) {
                return;
            }

            doorKeeper.put(modelId);

            ActionListener<Pair<double[][], Sample>> coldStartCallBack = ActionListener.wrap(trainingData -> {
                try {
                    if (trainingData != null && trainingData.getKey() != null) {
                        double[][] dataPoints = trainingData.getKey();
                        // only train models if we have enough samples
                        if (dataPoints.length >= numMinSamples) {
                            // The function trainModelFromDataSegments will save a trained a model. trainModelFromDataSegments is called by
                            // multiple places so I want to make the saving model implicit just in case I forgot.
                            trainModelFromDataSegments(trainingData, entity, modelState, config);
                            logger.info("Succeeded in training entity: {}", modelId);
                        } else {
                            // save to checkpoint
                            checkpointWriteWorker.write(modelState, true, RequestPriority.MEDIUM);
                            logger.info("Not enough data to train model: {}, currently we have {}", modelId, dataPoints.length);
                        }
                    } else {
                        logger.info("Cannot get training data for {}", modelId);
                    }
                    listener.onResponse(null);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, exception -> {
                try {
                    logger.error(new ParameterizedMessage("Error while cold start {}", modelId), exception);
                    Throwable cause = Throwables.getRootCause(exception);
                    if (ExceptionUtil.isOverloaded(cause)) {
                        logger.error("too many requests");
                        lastThrottledColdStartTime = Instant.now();
                    } else if (cause instanceof TimeSeriesException || exception instanceof TimeSeriesException) {
                        // e.g., cannot find anomaly detector
                        nodeStateManager.setException(configId, exception);
                    } else {
                        nodeStateManager.setException(configId, new TimeSeriesException(configId, cause));
                    }
                    listener.onFailure(exception);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });

            threadPool
                .executor(threadPoolName)
                .execute(
                    () -> getColdStartData(
                        configId,
                        entity,
                        config.getImputer(),
                        new ThreadedActionListener<>(logger, threadPool, threadPoolName, coldStartCallBack, false)
                    )
                );
            earlyExit = false;
        } finally {
            if (earlyExit) {
                listener.onResponse(null);
            }
        }
    }

    /**
     * Get training data for an entity.
     *
     * We first note the maximum and minimum timestamp, and sample at most 24 points
     * (with 60 points apart between two neighboring samples) between those minimum
     * and maximum timestamps.  Samples can be missing.  We only interpolate points
     * between present neighboring samples. We then transform samples and interpolate
     * points to shingles. Finally, full shingles will be used for cold start.
     *
     * @param configId config Id
     * @param entity the entity's information
     * @param listener listener to return training data
     */
    private void getColdStartData(
        String configId,
        Optional<Entity> entity,
        Imputer imputer,
        ActionListener<Pair<double[][], Sample>> listener
    ) {
        ActionListener<Optional<? extends Config>> getDetectorListener = ActionListener.wrap(configOp -> {
            if (!configOp.isPresent()) {
                listener.onFailure(new EndRunException(configId, "Config is not available.", false));
                return;
            }
            Config config = configOp.get();

            ActionListener<Optional<Long>> minTimeListener = ActionListener.wrap(earliest -> {
                if (earliest.isPresent()) {
                    long startTimeMs = earliest.get().longValue();

                    // End time uses milliseconds as start time is assumed to be in milliseconds.
                    // Opensearch uses a set of preconfigured formats to recognize and parse these
                    // strings into a long value
                    // representing milliseconds-since-the-epoch in UTC.
                    // More on https://tinyurl.com/wub4fk92

                    long endTimeMs = clock.millis();
                    Pair<Integer, Integer> params = selectRangeParam(config);
                    int stride = params.getLeft();
                    int numberOfSamples = params.getRight();

                    // we start with round 0
                    getFeatures(
                        listener,
                        0,
                        Pair.of(new double[0][0], new Sample()),
                        config,
                        entity,
                        stride,
                        numberOfSamples,
                        startTimeMs,
                        endTimeMs,
                        imputer
                    );
                } else {
                    listener.onResponse(Pair.of(new double[0][0], new Sample()));
                }
            }, listener::onFailure);

            searchFeatureDao
                .getMinDataTime(
                    config,
                    entity,
                    context,
                    new ThreadedActionListener<>(logger, threadPool, threadPoolName, minTimeListener, false)
                );

        }, listener::onFailure);

        nodeStateManager
            .getConfig(configId, context, new ThreadedActionListener<>(logger, threadPool, threadPoolName, getDetectorListener, false));
    }

    /**
     * Select strideLength and numberOfSamples, where stride is the number of intervals
     * between two samples and trainSamples is training samples to fetch. If we disable
     * interpolation, strideLength is 1 and numberOfSamples is shingleSize + numMinSamples;
     *
     * Algorithm:
     *
     * delta is the length of the detector interval in minutes.
     *
     * 1. Suppose delta ≤ 30 and divides 60. Then set numberOfSamples = ceil ( (shingleSize + 32)/ 24 )*24
     * and strideLength = 60/delta. Note that if there is enough data — we may have lot more than shingleSize+32
     * points — which is only good. This step tries to match data with hourly pattern.
     * 2. otherwise, set numberOfSamples = (shingleSize + 32) and strideLength = 1.
     * This should be an uncommon case as we are assuming most users think in terms of multiple of 5 minutes
     *(say 10 or 30 minutes). But if someone wants a 23 minutes interval —- and the system permits --
     * we give it to them. In this case, we disable interpolation as we want to interpolate based on the hourly pattern.
     * That's why we use 60 as a dividend in case 1. The 23 minute case does not fit that pattern.
     * Note the smallest delta that does not divide 60 is 7 which is quite large to wait for one data point.
     * @return the chosen strideLength and numberOfSamples
     */
    private Pair<Integer, Integer> selectRangeParam(Config config) {
        int shingleSize = config.getShingleSize();
        if (isInterpolationInColdStartEnabled()) {
            long delta = config.getIntervalInMinutes();

            int strideLength = defaulStrideLength;
            int numberOfSamples = defaultNumberOfSamples;
            if (delta <= 30 && 60 % delta == 0) {
                strideLength = (int) (60 / delta);
                numberOfSamples = (int) Math.ceil((shingleSize + numMinSamples) / 24.0d) * 24;
            } else {
                strideLength = 1;
                numberOfSamples = shingleSize + numMinSamples;
            }
            return Pair.of(strideLength, numberOfSamples);
        } else {
            return Pair.of(1, shingleSize + numMinSamples);
        }

    }

    private void getFeatures(
        ActionListener<Pair<double[][], Sample>> listener,
        int round,
        Pair<double[][], Sample> lastRounddataSample,
        Config config,
        Optional<Entity> entity,
        int stride,
        int numberOfSamples,
        long startTimeMs,
        long endTimeMs,
        Imputer imputer
    ) {
        if (startTimeMs >= endTimeMs || endTimeMs - startTimeMs < config.getIntervalInMilliseconds()) {
            listener.onResponse(lastRounddataSample);
            return;
        }

        // Create ranges in descending order to make sure the last sample's end time is the given endTimeMs.
        // We will reorder the ranges in ascending order in Opensearch's response.
        List<Entry<Long, Long>> sampleRanges = getTrainSampleRanges(config, startTimeMs, endTimeMs, stride, numberOfSamples);

        if (sampleRanges.isEmpty()) {
            listener.onResponse(lastRounddataSample);
            return;
        }

        ActionListener<List<Optional<double[]>>> getFeaturelistener = ActionListener.wrap(featureSamples -> {

            if (featureSamples.size() != sampleRanges.size()) {
                logger
                    .error(
                        "We don't expect different featureSample size {} and sample range size {}.",
                        featureSamples.size(),
                        sampleRanges.size()
                    );
                listener.onResponse(lastRounddataSample);
                return;
            }

            int totalNumSamples = featureSamples.size();
            int numEnabledFeatures = config.getEnabledFeatureIds().size();
            double[][] trainingData = new double[totalNumSamples][numEnabledFeatures];

            // featuresSamples are in ascending order of time.
            for (int index = 0; index < featureSamples.size(); index++) {
                Optional<double[]> featuresOptional = featureSamples.get(index);
                if (featuresOptional.isPresent()) {
                    // the order of the elements in the Stream is the same as the order of the elements in the List entry.getValue()
                    trainingData[index] = featuresOptional.get();
                } else {
                    // create an array of Double.NaN
                    trainingData[index] = DoubleStream.generate(() -> Double.NaN).limit(numEnabledFeatures).toArray();
                }
            }

            double[][] currentRoundColdStartData = imputer.impute(trainingData, totalNumSamples);

            Pair<double[][], Sample> concatenatedDataSample = null;
            double[][] lastRoundColdStartData = lastRounddataSample.getKey();
            // make sure the following logic making sense via checking lastRoundFirstStartTime > 0
            if (lastRoundColdStartData != null && lastRoundColdStartData.length > 0) {
                double[][] concatenated = new double[currentRoundColdStartData.length + lastRoundColdStartData.length][numEnabledFeatures];
                System.arraycopy(lastRoundColdStartData, 0, concatenated, 0, lastRoundColdStartData.length);
                System
                    .arraycopy(currentRoundColdStartData, 0, concatenated, lastRoundColdStartData.length, currentRoundColdStartData.length);
                trainingData = imputer.impute(concatenated, concatenated.length);
                concatenatedDataSample = Pair.of(trainingData, lastRounddataSample.getValue());
            } else {
                concatenatedDataSample = Pair
                    .of(
                        currentRoundColdStartData,
                        new Sample(
                            currentRoundColdStartData[currentRoundColdStartData.length - 1],
                            Instant.ofEpochMilli(endTimeMs - config.getIntervalInMilliseconds()),
                            Instant.ofEpochMilli(endTimeMs)
                        )
                    );
            }

            // If the first round of probe provides (32+shingleSize) points (note that if S0 is
            // missing or all Si​ for some i > N is missing then we would miss a lot of points.
            // Otherwise we can issue another round of query — if there is any sample in the
            // second round then we would have 32 + shingleSize points. If there is no sample
            // in the second round then we should wait for real data.
            if (currentRoundColdStartData.length >= config.getShingleSize() + numMinSamples || round + 1 >= maxRoundofColdStart) {
                listener.onResponse(concatenatedDataSample);
            } else {
                // the earliest sample's start time is the endTimeMs of next round of probe.
                long earliestSampleStartTime = sampleRanges.get(sampleRanges.size() - 1).getKey();
                getFeatures(
                    listener,
                    round + 1,
                    concatenatedDataSample,
                    config,
                    entity,
                    stride,
                    numberOfSamples,
                    startTimeMs,
                    earliestSampleStartTime,
                    imputer
                );
            }
        }, listener::onFailure);

        try {
            searchFeatureDao
                .getColdStartSamplesForPeriods(
                    config,
                    sampleRanges,
                    entity,
                    // Accept empty bucket.
                    // 0, as returned by the engine should constitute a valid answer, “null” is a missing answer — it may be that 0
                    // is meaningless in some case, but 0 is also meaningful in some cases. It may be that the query defining the
                    // metric is ill-formed, but that cannot be solved by cold-start strategy of the AD plugin — if we attempt to do
                    // that, we will have issues with legitimate interpretations of 0.
                    true,
                    context,
                    new ThreadedActionListener<>(logger, threadPool, threadPoolName, getFeaturelistener, false)
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Get train samples within a time range.
     *
     * @param config accessor to config
     * @param startMilli range start
     * @param endMilli range end
     * @param stride the number of intervals between two samples
     * @param numberOfSamples maximum training samples to fetch
     * @return list of sample time ranges
     */
    private List<Entry<Long, Long>> getTrainSampleRanges(Config config, long startMilli, long endMilli, int stride, int numberOfSamples) {
        long bucketSize = ((IntervalTimeConfiguration) config.getInterval()).toDuration().toMillis();
        int numBuckets = (int) Math.floor((endMilli - startMilli) / (double) bucketSize);
        // adjust if numStrides is more than the max samples
        int numStrides = Math.min((int) Math.floor(numBuckets / (double) stride), numberOfSamples);
        List<Entry<Long, Long>> sampleRanges = Stream
            .iterate(endMilli, i -> i - stride * bucketSize)
            .limit(numStrides)
            .map(time -> new SimpleImmutableEntry<>(time - bucketSize, time))
            .collect(Collectors.toList());
        return sampleRanges;
    }

    protected abstract void trainModelFromDataSegments(
        Pair<double[][], Sample> dataPoints,
        Optional<Entity> entity,
        ModelState<RCFModelType> state,
        Config config
    );

    protected abstract boolean isInterpolationInColdStartEnabled();
}
