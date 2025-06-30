/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ml;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.MaintenanceState;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ExpiringValue;
import org.opensearch.timeseries.util.ModelUtil;

import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Since we assume model state's last access time is current time and compare it with incoming data's execution time,
 * this class is only meant to be used by real time analysis.
 *
 * @param <RCFModelType> the RCF model type
 * @param <ResultType> the indexable result type
 * @param <RCFResultType> the intermediate result type
 * @param <IndexType> the time series index enum type
 * @param <DataManagementType> the data management implementation type
 * @param <CheckpointDaoType> the checkpoint DAO type
 * @param <CheckpointWriterType> the checkpoint write worker type
 * @param <ColdStarterType> the cold start implementation type
 * @param <ModelManagerType> the model manager type
 * @param <SaveResultStrategyType> the result persistence strategy type
 * @param <CacheType> the cache implementation type
 * @param <TaskCacheManagerType> the task cache manager type
 * @param <TaskTypeEnum> the task type enum
 * @param <TaskClass> the time series task type
 * @param <TaskManagerType> the task manager type
 * @param <ColdStartWorkerType> the cold start worker type
 */
public abstract class RealTimeInferencer<RCFModelType extends ThresholdedRandomCutForest, ResultType extends IndexableResult, RCFResultType extends IntermediateResult<ResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, CheckpointDaoType extends CheckpointDaoInterface<RCFModelType>, CheckpointWriterType extends CheckpointWriteWorker<RCFModelType, IndexType, DataManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, DataManagementType, ResultType>, ModelManagerType extends ModelManager<RCFModelType, ResultType, RCFResultType, IndexType, DataManagementType, CheckpointDaoType, ColdStarterType>, SaveResultStrategyType extends SaveResultStrategy<ResultType, RCFResultType>, CacheType extends TimeSeriesCache<RCFModelType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, DataManagementType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, DataManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, CacheType, ResultType, RCFResultType, ModelManagerType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType>>
    implements
        MaintenanceState {

    private static final Logger LOG = LogManager.getLogger(RealTimeInferencer.class);
    protected ModelManagerType modelManager;
    protected Stats stats;
    private String modelCorruptionStat;
    protected CheckpointDaoInterface<RCFModelType> checkpointDao;
    protected ColdStartWorkerType coldStartWorker;
    protected SaveResultStrategyType resultWriteWorker;
    private CacheProvider<RCFModelType, CacheType> cache;
    // ensure no two threads can score samples at the same time which can happen in tests
    // where we send a lot of requests in a fast pace and the run API returns immediately
    // without waiting for the requests get finished processing. It can also happen in
    // production as the impute request and actual data scoring in the next interval
    // can happen at the same time.
    private Map<String, ExpiringValue<Lock>> modelLocks;
    private ThreadPool threadPool;
    private String threadPoolName;
    // ensure we process samples in the ascending order of time in case race conditions.
    private Map<String, ExpiringValue<ConcurrentSkipListSet<Sample>>> sampleQueues;
    // ensure only one getFeatures runs per model at a time
    private Map<String, ExpiringValue<AtomicBoolean>> featureFetchInFlight;
    private Comparator<Sample> sampleComparator;
    private Clock clock;
    private SearchFeatureDao searchFeatureDao;
    private AnalysisType analysisContext;

    public RealTimeInferencer(
        ModelManagerType modelManager,
        Stats stats,
        String modelCorruptionStat,
        CheckpointDaoInterface<RCFModelType> checkpointDao,
        ColdStartWorkerType coldStartWorker,
        SaveResultStrategyType resultWriteWorker,
        CacheProvider<RCFModelType, CacheType> cache,
        ThreadPool threadPool,
        String threadPoolName,
        Clock clock,
        SearchFeatureDao searchFeatureDao,
        AnalysisType analysisContext
    ) {
        this.modelManager = modelManager;
        this.stats = stats;
        this.modelCorruptionStat = modelCorruptionStat;
        this.checkpointDao = checkpointDao;
        this.coldStartWorker = coldStartWorker;
        this.resultWriteWorker = resultWriteWorker;
        this.cache = cache;
        this.threadPool = threadPool;
        this.threadPoolName = threadPoolName;
        this.modelLocks = new ConcurrentHashMap<>();
        this.sampleQueues = new ConcurrentHashMap<>();
        this.featureFetchInFlight = new ConcurrentHashMap<>();
        this.sampleComparator = Comparator.comparing(Sample::getDataEndTime);
        this.clock = clock;
        this.searchFeatureDao = searchFeatureDao;
        this.analysisContext = analysisContext;
    }

    /**
     *
     * @param sample Sample to process
     * @param modelState model state
     * @param config Config accessor
     * @param taskId task Id for batch analysis
     */
    public void process(
        Sample sample,
        ModelState<RCFModelType> modelState,
        Config config,
        String taskId,
        ActionListener<Boolean> listener
    ) {
        final long processStartNanos = System.nanoTime();
        String modelId = modelState.getModelId();
        if (modelState.hasProcessedDataEndTime(sample.getDataEndTime())) {
            LOG
                .info(
                    "realtime process skipping already processed sample config={} model={} taskId={} sampleEnd={} lastProcessedEnd={}",
                    config.getId(),
                    modelId,
                    taskId,
                    sample.getDataEndTime().toEpochMilli(),
                    modelState.getLastProcessedDataEndTime().toEpochMilli()
                );
            listener.onResponse(true);
            return;
        }
        ExpiringValue<ConcurrentSkipListSet<Sample>> expiringSampleQueue = sampleQueues
            .computeIfAbsent(
                modelId,
                k -> new ExpiringValue<>(
                    new ConcurrentSkipListSet<>(sampleComparator),
                    config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
                    clock
                )
            );
        ConcurrentSkipListSet<Sample> queue = expiringSampleQueue.getValue();
        int queueSizeBefore = queue.size();
        int bufferedSamples = modelState.getSamples() == null ? 0 : modelState.getSamples().size();
        // model state might have samples that are not processed yet
        addSamples(queue, modelState.getSamples(), config);
        // record the last unprocessed historical sample's data end time
        // this is used to calculate the time gap between last input timestamp and current sample's data end time
        Instant lastSampleDataEndTime = queue.isEmpty() ? Instant.MIN : queue.last().getDataEndTime();
        // add current sample to queue
        addSample(queue, sample, config);
        Optional<RCFModelType> modelOptional = modelState.getModel();
        LOG
            .info(
                "realtime process queued config={} model={} taskId={} sampleStart={} sampleEnd={} queueBefore={} bufferedSamples={} queueAfter={} modelPresent={} elapsedMs={}",
                config.getId(),
                modelId,
                taskId,
                sample.getDataStartTime().toEpochMilli(),
                sample.getDataEndTime().toEpochMilli(),
                queueSizeBefore,
                bufferedSamples,
                queue.size(),
                modelOptional.isPresent(),
                elapsedMillis(processStartNanos)
            );
        if (modelOptional.isPresent()) {
            // we need to use the latest sample in the queue to calculate the time gap because last scored RCF sample might not be the
            // latest sample in the queue
            long lastInputTimestampSecs = Math
                .max(ModelUtil.getLastInputTimestampSeconds(modelOptional.get()), lastSampleDataEndTime.getEpochSecond());
            // Current sample is already retrieved. We need to figure out how many data points before current sample.
            // We send data end time in seconds to rcf, so we need to find the gap between last input timestamp and current sample's data.
            long currentTimeSecs = sample.getDataEndTime().getEpochSecond();
            long diffSecs = currentTimeSecs - lastInputTimestampSecs;
            LOG
                .debug(
                    "diffSecs:{} interval:{} maxFrequencyMultiple:{} lastInputTimestampSecs:{} currentTimeSecs:{}",
                    diffSecs,
                    config.getIntervalInSeconds(),
                    TimeSeriesSettings.MAX_FREQUENCY_MULTIPLE,
                    lastInputTimestampSecs,
                    currentTimeSecs
                );
            // it is expected that the time gap is at least 1 interval. So 2 intervals is the minimum gap to fetch data.
            long minGapSecs = 2 * config.getIntervalInSeconds();
            LOG
                .info(
                    "realtime process gap config={} model={} taskId={} diffSecs={} minGapSecs={} intervalSecs={} lastInputSecs={} currentSecs={} queueSize={} elapsedMs={}",
                    config.getId(),
                    modelId,
                    taskId,
                    diffSecs,
                    minGapSecs,
                    config.getIntervalInSeconds(),
                    lastInputTimestampSecs,
                    currentTimeSecs,
                    queue.size(),
                    elapsedMillis(processStartNanos)
                );
            if (diffSecs >= minGapSecs && diffSecs / config.getIntervalInSeconds() <= TimeSeriesSettings.MAX_FREQUENCY_MULTIPLE) {
                AtomicBoolean inFlight = featureFetchInFlight
                    .computeIfAbsent(
                        modelId,
                        k -> new ExpiringValue<>(
                            new AtomicBoolean(false),
                            config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
                            clock
                        )
                    )
                    .getValue();
                if (false == inFlight.compareAndSet(false, true)) {
                    LOG.info("getFeatures already in-flight for model [{}]; skipping duplicate fetch.", modelId);
                    listener.onResponse(false);
                    return;
                }
                long featureFetchStartNanos = System.nanoTime();
                LOG
                    .info(
                        "realtime feature fetch start config={} model={} taskId={} startMs={} endMs={} elapsedMs={}",
                        config.getId(),
                        modelId,
                        taskId,
                        lastInputTimestampSecs * 1000,
                        sample.getDataStartTime().getEpochSecond() * 1000,
                        elapsedMillis(processStartNanos)
                    );
                // get features for the interval since last input timestamp and current sample's data start time
                // getFeatures uses milliseconds as unit, so we need to convert seconds to milliseconds
                // We avoid querying the same features twice. Each query begins at the
                // latest existing sample, ensuring we always process it at least once.
                // Future queries start from that sample onward, never looking back
                // before it.
                getFeatures(
                    config,
                    modelState.getEntity(),
                    lastInputTimestampSecs * 1000,
                    sample.getDataStartTime().getEpochSecond() * 1000,
                    ActionListener.wrap(samples -> {
                        try {
                            LOG
                                .info(
                                    "realtime feature fetch complete config={} model={} taskId={} fetchedSamples={} featureElapsedMs={} totalElapsedMs={}",
                                    config.getId(),
                                    modelId,
                                    taskId,
                                    samples.size(),
                                    elapsedMillis(featureFetchStartNanos),
                                    elapsedMillis(processStartNanos)
                                );
                            for (Sample s : samples) {
                                addSample(queue, s, config);
                            }
                            processWithTimeout(modelState, config, taskId, sample, listener);
                        } finally {
                            inFlight.set(false);
                        }
                    }, e -> {
                        inFlight.set(false);
                        LOG
                            .error(
                                new ParameterizedMessage(
                                    "realtime feature fetch failed config={} model={} taskId={} featureElapsedMs={} totalElapsedMs={}",
                                    config.getId(),
                                    modelId,
                                    taskId,
                                    elapsedMillis(featureFetchStartNanos),
                                    elapsedMillis(processStartNanos)
                                ),
                                e
                            );
                        listener.onFailure(e);
                    })
                );
            } else if (diffSecs < 0) {
                // prevent out of order processing
                LOG
                    .warn(
                        "Time gap {} is negative for config [{}], model [{}]. Skipping. totalElapsedMs={}",
                        diffSecs,
                        config.getId(),
                        modelId,
                        elapsedMillis(processStartNanos)
                    );
                listener.onResponse(false);
            } else if (diffSecs < minGapSecs) {
                LOG
                    .info(
                        "realtime direct scoring config={} model={} taskId={} diffSecs={} elapsedMs={}",
                        config.getId(),
                        modelId,
                        taskId,
                        diffSecs,
                        elapsedMillis(processStartNanos)
                    );
                processWithTimeout(modelState, config, taskId, sample, listener);
            } else {
                LOG
                    .warn(
                        "Time gap {} is too large for config [{}], model [{}]. Triggering cold start. totalElapsedMs={}",
                        diffSecs,
                        config.getId(),
                        modelId,
                        elapsedMillis(processStartNanos)
                    );
                reColdStart(config, modelId, null, sample, taskId);
                listener.onResponse(false);
            }
        } else {
            // model not present, cannot process.
            LOG
                .warn(
                    "Model not present for config [{}], model [{}]. Skipping. totalElapsedMs={}",
                    config.getId(),
                    modelId,
                    elapsedMillis(processStartNanos)
                );
            listener.onResponse(false);
        }
    }

    public void processWithTimeout(
        ModelState<RCFModelType> modelState,
        Config config,
        String taskId,
        Sample sample,
        ActionListener<Boolean> listener
    ) {
        final long processWithTimeoutStartNanos = System.nanoTime();
        String modelId = modelState.getModelId();
        ReentrantLock lock = (ReentrantLock) modelLocks
            .computeIfAbsent(
                modelId,
                k -> new ExpiringValue<>(
                    new ReentrantLock(),
                    config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
                    clock
                )
            )
            .getValue();

        boolean success = false;
        ExpiringValue<ConcurrentSkipListSet<Sample>> queueValue = sampleQueues.get(modelId);
        int queuedSamples = queueValue == null ? -1 : queueValue.getValue().size();
        LOG
            .info(
                "realtime processWithTimeout enter config={} model={} taskId={} sampleEnd={} queueSize={} lockLocked={} lockQueueLength={} now={}",
                config.getId(),
                modelId,
                taskId,
                sample.getDataEndTime().toEpochMilli(),
                queuedSamples,
                lock.isLocked(),
                lock.getQueueLength(),
                clock.millis()
            );
        if (lock.tryLock()) {
            LOG
                .info(
                    "realtime processWithTimeout lock acquired config={} model={} taskId={} lockWaitMs={}",
                    config.getId(),
                    modelId,
                    taskId,
                    elapsedMillis(processWithTimeoutStartNanos)
                );
            try {
                ConcurrentSkipListSet<Sample> queue = sampleQueues.get(modelId).getValue();
                LOG
                    .info(
                        "realtime processWithTimeout queue drain config={} model={} taskId={} queueSize={}",
                        config.getId(),
                        modelId,
                        taskId,
                        queue.size()
                    );
                if (!queue.isEmpty()) {
                    List<Sample> samples = new ArrayList<>(queue)
                        .stream()
                        .filter(queuedSample -> modelState.hasProcessedDataEndTime(queuedSample.getDataEndTime()) == false)
                        .toList();
                    queue.clear();

                    if (samples.isEmpty()) {
                        LOG
                            .info(
                                "realtime processWithTimeout queue only contained already processed samples config={} model={} taskId={}",
                                config.getId(),
                                modelId,
                                taskId
                            );
                        listener.onResponse(true);
                        return;
                    }

                    double[][] points = new double[samples.size()][];
                    long[] timestamps = new long[samples.size()];
                    List<Instant> dataStarts = new ArrayList<>();
                    List<Instant> dataEnds = new ArrayList<>();
                    for (int i = 0; i < samples.size(); i++) {
                        points[i] = samples.get(i).getValueList();
                        Instant dataStart = samples.get(i).getDataStartTime();
                        dataStarts.add(dataStart);
                        Instant dataEnd = samples.get(i).getDataEndTime();
                        dataEnds.add(dataEnd);
                        timestamps[i] = dataEnd.getEpochSecond();
                    }

                    RCFModelType model = modelState.getModel().get();
                    LOG
                        .info(
                            "realtime scoring start config={} model={} taskId={} samples={} timestamps={} entity={}",
                            config.getId(),
                            modelId,
                            taskId,
                            samples.size(),
                            Arrays.toString(timestamps),
                            modelState.getEntity().map(Object::toString).orElse("null")
                        );
                    long scoringStartNanos = System.nanoTime();
                    List<AnomalyDescriptor> results = model.processSequentially(points, timestamps, x -> true);
                    LOG
                        .info(
                            "realtime scoring complete config={} model={} taskId={} results={} scoringElapsedMs={} totalElapsedMs={}",
                            config.getId(),
                            modelId,
                            taskId,
                            results.size(),
                            elapsedMillis(scoringStartNanos),
                            elapsedMillis(processWithTimeoutStartNanos)
                        );
                    List<RCFResultType> intermediateResults = new ArrayList<>();
                    long conversionStartNanos = System.nanoTime();
                    for (int i = 0; i < results.size(); i++) {
                        Sample sampleI = samples.get(i);
                        AnomalyDescriptor result = results.get(i);
                        RCFResultType rcfResult = modelManager
                            .toResult(model.getForest(), result, sampleI.getValueList(), result.getMissingValues() != null, config);
                        intermediateResults.add(rcfResult);
                    }
                    LOG
                        .info(
                            "realtime result conversion complete config={} model={} taskId={} intermediateResults={} conversionElapsedMs={} totalElapsedMs={}",
                            config.getId(),
                            modelId,
                            taskId,
                            intermediateResults.size(),
                            elapsedMillis(conversionStartNanos),
                            elapsedMillis(processWithTimeoutStartNanos)
                        );
                    long saveStartNanos = System.nanoTime();
                    resultWriteWorker
                        .saveAllResults(
                            intermediateResults,
                            config,
                            dataStarts,
                            dataEnds,
                            modelId,
                            Arrays.asList(points),
                            modelState.getEntity(),
                            taskId
                        );
                    LOG
                        .info(
                            "realtime saveAllResults returned config={} model={} taskId={} saveElapsedMs={} totalElapsedMs={}",
                            config.getId(),
                            modelId,
                            taskId,
                            elapsedMillis(saveStartNanos),
                            elapsedMillis(processWithTimeoutStartNanos)
                        );
                    modelState.setLastProcessedDataEndTime(samples.get(samples.size() - 1).getDataEndTime());
                    modelState.clearSamples();
                    success = true;
                } else {
                    LOG.info("realtime processWithTimeout queue empty config={} model={} taskId={}", config.getId(), modelId, taskId);
                }
                LOG
                    .info(
                        "realtime processWithTimeout listener success config={} model={} taskId={} success={} totalElapsedMs={}",
                        config.getId(),
                        modelId,
                        taskId,
                        success,
                        elapsedMillis(processWithTimeoutStartNanos)
                    );
                listener.onResponse(success);
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("incorrect ordering of time")) {
                    // ignore current timestamp.
                    LOG
                        .info(
                            String
                                .format(
                                    Locale.ROOT,
                                    "incorrect ordering of time for config %s model %s at data end time %d after %d ms",
                                    config.getId(),
                                    modelState.getModelId(),
                                    sample.getDataEndTime().toEpochMilli(),
                                    elapsedMillis(processWithTimeoutStartNanos)
                                )
                        );
                    listener.onResponse(false);
                } else {
                    LOG
                        .error(
                            new ParameterizedMessage(
                                "Error processing samples config={} model={} taskId={} totalElapsedMs={}",
                                config.getId(),
                                modelId,
                                taskId,
                                elapsedMillis(processWithTimeoutStartNanos)
                            ),
                            e
                        );
                    reColdStart(config, modelId, e, sample, taskId);
                    listener.onFailure(e);
                }
            } finally {
                LOG
                    .info(
                        "realtime processWithTimeout unlock config={} model={} taskId={} totalElapsedMs={}",
                        config.getId(),
                        modelId,
                        taskId,
                        elapsedMillis(processWithTimeoutStartNanos)
                    );
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        } else {
            long windowDelayMillis = config.getWindowDelay() == null
                ? 0
                : ((IntervalTimeConfiguration) config.getWindowDelay()).toDuration().toMillis();
            long curExecutionEnd = sample.getDataEndTime().toEpochMilli() + windowDelayMillis;
            long nextExecutionEnd = curExecutionEnd + config.getIntervalInMilliseconds();
            long nowMillis = clock.millis();
            // schedule a retry if not already time out
            if (nowMillis >= nextExecutionEnd) {
                LOG
                    .warn(
                        "realtime processWithTimeout lock busy timeout config={} model={} taskId={} sampleEnd={} nextExecutionEnd={} now={} totalElapsedMs={}",
                        config.getId(),
                        modelId,
                        taskId,
                        sample.getDataEndTime().toEpochMilli(),
                        nextExecutionEnd,
                        nowMillis,
                        elapsedMillis(processWithTimeoutStartNanos)
                    );
                listener.onResponse(false);
            } else {
                try {
                    LOG
                        .info(
                            "realtime processWithTimeout lock busy retry config={} model={} taskId={} sampleEnd={} nextExecutionEnd={} now={} windowDelay={} intervalMs={} totalElapsedMs={}",
                            config.getId(),
                            modelId,
                            taskId,
                            sample.getDataEndTime().toEpochMilli(),
                            nextExecutionEnd,
                            nowMillis,
                            windowDelayMillis,
                            config.getIntervalInMilliseconds(),
                            elapsedMillis(processWithTimeoutStartNanos)
                        );
                    // Schedule a retry in one second
                    threadPool
                        .schedule(
                            () -> processWithTimeout(modelState, config, taskId, sample, listener),
                            new TimeValue(1, TimeUnit.SECONDS),
                            threadPoolName
                        );
                    // no call to listener as we scheduled a retry and listener will be called when the retry is successful
                } catch (Exception e) {
                    LOG.error("Failed to schedule retry", e);
                    listener.onFailure(e);
                }
            }
        }
    }

    public void reColdStart(Config config, String modelId, Exception e, Sample sample, String taskId) {
        // fail to score likely due to model corruption. Re-cold start to recover.
        if (e != null) {
            LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", modelId), e);
        } else {
            LOG.warn(new ParameterizedMessage("Likely model corruption for [{}]", modelId));
        }
        stats.getStat(modelCorruptionStat).increment();
        cache.get().removeModel(config.getTenantId(), config.getId(), modelId);
        if (null != modelId) {
            checkpointDao
                .deleteModelCheckpoint(
                    config,
                    modelId,
                    ActionListener
                        .wrap(
                            r -> LOG.debug(new ParameterizedMessage("Succeeded in deleting checkpoint [{}].", modelId)),
                            ex -> LOG.error(new ParameterizedMessage("Failed to delete checkpoint [{}].", modelId), ex)
                        )
                );
        }

        coldStartWorker
            .put(
                new FeatureRequest(
                    clock.millis() + config.getInferredFrequencyInMilliseconds(),
                    config.getId(),
                    RequestPriority.MEDIUM,
                    modelId,
                    sample.getValueList(),
                    sample.getDataStartTime().toEpochMilli(),
                    taskId,
                    config.getTenantId(),
                    config,
                    clock.millis()
                )
            );
    }

    /**
     * Simplified version of getFeatures without round and lastRounddataSample parameters.
     * This method fetches features for a single time period without recursive calling logic.
     *
     * @param config Config accessor
     * @param entity Optional entity for which to fetch samples
     * @param startTimeMs Start time in milliseconds
     * @param endTimeMs End time in milliseconds
     * @param listener ActionListener to return available samples
     */
    private void getFeatures(
        Config config,
        Optional<Entity> entity,
        long startTimeMs,
        long endTimeMs,
        ActionListener<List<Sample>> listener
    ) {
        if (startTimeMs == 0 || startTimeMs >= endTimeMs || endTimeMs - startTimeMs < config.getIntervalInMilliseconds()) {
            listener.onResponse(new ArrayList<>());
            return;
        }

        int numberOfSamples = (int) Math.floor((endTimeMs - startTimeMs) / (double) config.getIntervalInMilliseconds());

        if (numberOfSamples > TimeSeriesSettings.MAX_FREQUENCY_MULTIPLE) {
            listener.onResponse(new ArrayList<>());
            return;
        }

        // Create ranges in ascending order where the last sample's end time is the given endTimeMs.
        // Sample ranges are also in ascending order in OpenSearch's response.
        List<Entry<Long, Long>> sampleRanges = searchFeatureDao
            .getTrainSampleRanges((IntervalTimeConfiguration) config.getInterval(), startTimeMs, endTimeMs, numberOfSamples);

        if (sampleRanges.isEmpty()) {
            listener.onResponse(new ArrayList<>());
            return;
        }

        ActionListener<List<Optional<double[]>>> getFeatureListener = ActionListener.wrap(featureSamples -> {
            int totalNumSamples = featureSamples.size();

            if (totalNumSamples != sampleRanges.size()) {
                String err = String
                    .format(
                        Locale.ROOT,
                        "length mismatch: totalNumSamples %d != time range length %d",
                        totalNumSamples,
                        sampleRanges.size()
                    );
                listener.onFailure(new IllegalArgumentException(err));
                return;
            }

            // featuresSamples are in ascending order of time.
            List<Sample> samples = new ArrayList<>();
            for (int index = 0; index < featureSamples.size(); index++) {
                Optional<double[]> featuresOptional = featureSamples.get(index);
                Entry<Long, Long> curRange = sampleRanges.get(index);
                if (featuresOptional.isPresent()) {
                    samples
                        .add(
                            new Sample(
                                featuresOptional.get(),
                                Instant.ofEpochMilli(curRange.getKey()),
                                Instant.ofEpochMilli(curRange.getValue())
                            )
                        );
                } else {
                    samples
                        .add(
                            new Sample(
                                createMissingFeature(config),
                                Instant.ofEpochMilli(curRange.getKey()),
                                Instant.ofEpochMilli(curRange.getValue())
                            )
                        );
                }
            }

            listener.onResponse(samples);
        }, listener::onFailure);

        try (Releasable ignored = searchFeatureDao.bindRouting(config.getTenantId(), config.getDataSourceId())) {
            searchFeatureDao
                .getColdStartSamplesForPeriods(
                    config,
                    sampleRanges,
                    entity,
                    // Accept empty bucket.
                    // 0, as returned by the engine should constitute a valid answer, "null" is a missing answer — it may be that 0
                    // is meaningless in some case, but 0 is also meaningful in other cases. It may be that the query defining the
                    // metric is ill-formed, but that cannot be solved by cold-start strategy of the AD plugin — if we attempt to do
                    // that, we will have issues with legitimate interpretations of 0.
                    true,
                    true,
                    analysisContext,
                    new ThreadedActionListener<>(LOG, threadPool, threadPoolName, getFeatureListener, false)
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private double[] createMissingFeature(Config config) {
        double[] missingFeature = new double[config.getEnabledFeatureIds().size()];
        Arrays.fill(missingFeature, Double.NaN);
        return missingFeature;
    }

    /**
     * Adds a sample to the queue only if it's at least one detector interval apart from its potential neighbors (previous and next).
     * This is to avoid an IllegalArgumentException from the RCF model due to out-of-order processing of timestamps.
     * @param queue The queue to add the sample to.
     * @param sample The sample to be added.
     * @param config The detector configuration.
     */
    private void addSample(ConcurrentSkipListSet<Sample> queue, Sample sample, Config config) {
        long intervalSeconds = config.getIntervalInSeconds();
        long sampleTime = sample.getDataStartTime().getEpochSecond();

        Sample previousSample = queue.floor(sample);
        Sample nextSample = queue.ceiling(sample);

        LOG
            .debug(
                "lastSample: {} sample: {}",
                previousSample == null ? "null" : previousSample.getDataStartTime().getEpochSecond(),
                sample.getDataStartTime().getEpochSecond()
            );

        boolean previousGapOk = (previousSample == null)
            || (sampleTime - previousSample.getDataStartTime().getEpochSecond() >= intervalSeconds);

        boolean nextGapOk = (nextSample == null) || (nextSample.getDataStartTime().getEpochSecond() - sampleTime >= intervalSeconds);

        if (previousGapOk && nextGapOk) {
            queue.add(sample);
        }
    }

    private void addSamples(ConcurrentSkipListSet<Sample> queue, Deque<Sample> samples, Config config) {
        if (samples != null) {
            for (Sample sample : samples) {
                addSample(queue, sample, config);
            }
        }
    }

    @Override
    public void maintenance() {
        try {
            // clean up expired items
            modelLocks.entrySet().removeIf(entry -> entry.getValue().isExpired());
            sampleQueues.entrySet().removeIf(entry -> entry.getValue().isExpired());
            featureFetchInFlight.entrySet().removeIf(entry -> entry.getValue().isExpired());
        } catch (Exception e) {
            // will be thrown to transport broadcast handler
            throw new TimeSeriesException("Failed to maintain RealTimeInferencer", e);
        }
    }

    public Map<String, ExpiringValue<Lock>> getModelLocks() {
        return modelLocks;
    }

    public Map<String, ExpiringValue<ConcurrentSkipListSet<Sample>>> getSampleQueues() {
        return sampleQueues;
    }

    private static long elapsedMillis(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }
}
