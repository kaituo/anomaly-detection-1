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

package org.opensearch.forecast.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.PAGE_SIZE;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.ADNodeStateManager;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.timeseries.transport.TimeSeriesResultTransport;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.NetworkExceptionHelper;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.node.NodeClosedException;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.common.exception.ClientException;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.NotSerializedADExceptionName;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.feature.CompositeRetriever;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SinglePointFeatures;
import org.opensearch.timeseries.feature.CompositeRetriever.PageIterator;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.ActionNotFoundTransportException;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

public class ForecastResultTransportAction extends HandledTransportAction<ForecastResultRequest, ForecastResultResponse> {

    private static final Logger LOG = LogManager.getLogger(ForecastResultTransportAction.class);

    private final TransportService transportService;
    private final ForecastNodeStateManager forecastStateManager;
    private final NodeStateManager stateManager;
    private final FeatureManager featureManager;
    private final ForecastModelManager modelManager;
    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final TimeSeriesStats timeSeriesStats;
    private final TimeSeriesCircuitBreakerService circuitBreakerService;
    private final ThreadPool threadPool;
    private final Client client;
    private final SecurityClientUtil clientUtil;
    private final ForecastTaskManager forecastTaskManager;

    // Cache HC detector id. This is used to count HC failure stats. We can tell a forecaster
    // is HC or not by checking if forecaster id exists in this field or not. Will add
    // forecaster id to this field when start to run realtime detection and remove detector
    // id once realtime detection done.
    private final Set<String> hcDetectors;
    private NamedXContentRegistry xContentRegistry;
    private Settings settings;
    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the forecast interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    private final float intervalRatioForRequest;
    private int maxEntitiesPerInterval;
    private int pageSize;

    @Inject
    public ForecastResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        Client client,
        SecurityClientUtil clientUtil,
        ForecastNodeStateManager forecastStateManager,
        NodeStateManager stateManager,
        FeatureManager featureManager,
        ForecastModelManager modelManager,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TimeSeriesCircuitBreakerService circuitBreakerService,
        TimeSeriesStats timeSeriesStats,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager forecastTaskManager
    ) {
        super(ForecastResultAction.NAME, transportService, actionFilters, ForecastResultRequest::new);
        this.transportService = transportService;
        this.settings = settings;
        this.client = client;
        this.clientUtil = clientUtil;
        this.forecastStateManager = forecastStateManager;
        this.stateManager = stateManager;
        this.featureManager = featureManager;
        this.modelManager = modelManager;
        this.hashRing = hashRing;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.circuitBreakerService = circuitBreakerService;
        this.timeSeriesStats = timeSeriesStats;
        this.threadPool = threadPool;
        this.hcDetectors = new HashSet<>();
        this.xContentRegistry = xContentRegistry;
        this.intervalRatioForRequest = AnomalyDetectorSettings.INTERVAL_RATIO_FOR_REQUESTS;

        this.maxEntitiesPerInterval = MAX_ENTITIES_PER_QUERY.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ENTITIES_PER_QUERY, it -> maxEntitiesPerInterval = it);

        this.pageSize = PAGE_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PAGE_SIZE, it -> pageSize = it);
        this.forecastTaskManager = forecastTaskManager;
    }

    /**
     * All the exceptions thrown by forecasting is a subclass of TimeSeriesException.
     *  ClientException is a subclass of TimeSeriesException. All exception visible to
     *   Client is under ClientVisible. Two classes directly extends ClientException:
     *   - InternalFailure for "root cause unknown failure. Maybe transient." We can continue the
     *    analysis running.
     *   - EndRunException for "failures that might impact the customer." The method endNow() is
     *    added to indicate whether the client should immediately terminate running an analysis.
     *      + endNow() returns true for "unrecoverable issue". We want to terminate the analysis run
     *       immediately.
     *      + endNow() returns false for "maybe unrecoverable issue but worth retrying a few more
     *       times." We want to wait for a few more times on different requests before terminating
     *        the analysis run.
     *
     *  The plugin may not be able to get a forecast but can find a feature vector.  Consider the
     *  case when the shingle is not ready.  In that case, the plugin just return the feature vector
     *  without a forecast. If the plugin cannot even find a feature vector, it throws
     *  EndRunException if there is an issue or returns empty response (i.e., feature array is empty.)
     *
     *  Known causes of EndRunException with endNow returning false:
     *   + training data for cold start not available
     *   + cold start cannot succeed
     *   + unknown prediction error
     *   + memory circuit breaker tripped
     *   + invalid search query
     *
     *  Known causes of EndRunException with endNow returning true:
     *   + a model's memory size reached limit
     *   + models' total memory size reached limit
     *   + Having trouble querying feature data due to
     *    * index does not exist
     *    * all features have been disabled
     *
     *   + forecaster is not available
     *   + Time series analytics plugin is disabled
     *   + training data is invalid due to serious internal bug(s)
     *
     *  Known causes of InternalFailure:
     *   + rcf model node is not available
     *   + cluster read/write is blocked
     *   + cold start hasn't been finished
     *   + fail to get all of rcf model nodes' responses
     *   + RCF model node failing to get checkpoint to restore model before timeout
     *
     */
    @Override
    protected void doExecute(Task task, ForecastResultRequest request, ActionListener<ForecastResultResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            String forecasterID = request.getForecasterID();
            ActionListener<ForecastResultResponse> original = listener;
            listener = ActionListener.wrap(r -> {
                hcDetectors.remove(forecasterID);
                original.onResponse(r);
            }, e -> {
                // If exception is AnomalyDetectionException and it should not be counted in stats,
                // we will not count it in failure stats.
                if (!(e instanceof TimeSeriesException) || ((TimeSeriesException) e).isCountedInStats()) {
                    timeSeriesStats.getStat(StatNames.FORECAST_EXECUTE_FAIL_COUNT.getName()).increment();
                    if (hcDetectors.contains(forecasterID)) {
                        timeSeriesStats.getStat(StatNames.FORECAST_HC_EXECUTE_FAIL_COUNT.getName()).increment();
                    }
                }
                hcDetectors.remove(forecasterID);
                original.onFailure(e);
            });

            if (!ForecastEnabledSetting.isForecastEnabled()) {
                throw new EndRunException(forecasterID, ForecastCommonMessages.DISABLED_ERR_MSG, true).countedInStats(false);
            }

            timeSeriesStats.getStat(StatNames.FORECAST_EXECUTE_REQUEST_COUNT.getName()).increment();

            if (circuitBreakerService.isOpen()) {
                listener.onFailure(new LimitExceededException(forecasterID, CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
                return;
            }
            try {
                forecastStateManager.getForecaster(forecasterID, onGetForecaster(listener, forecasterID, request));
            } catch (Exception ex) {
                TimeSeriesResultTransport.handleExecuteException(ex, listener, forecasterID);
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * didn't use ActionListener.wrap so that I can
     * 1) use this to refer to the listener inside the listener
     * 2) pass parameters using constructors
     *
     */
    class PageListener implements ActionListener<CompositeRetriever.Page> {
        private PageIterator pageIterator;
        private String detectorId;
        private long dataStartTime;
        private long dataEndTime;

        PageListener(PageIterator pageIterator, String detectorId, long dataStartTime, long dataEndTime) {
            this.pageIterator = pageIterator;
            this.detectorId = detectorId;
            this.dataStartTime = dataStartTime;
            this.dataEndTime = dataEndTime;
        }

        @Override
        public void onResponse(CompositeRetriever.Page entityFeatures) {
            if (pageIterator.hasNext()) {
                pageIterator.next(this);
            }
            if (entityFeatures != null && false == entityFeatures.isEmpty()) {
                // wrap expensive operation inside forecast threadpool
                threadPool.executor(TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME).execute(() -> {
                    try {

                        Set<Entry<DiscoveryNode, Map<Entity, double[]>>> node2Entities = entityFeatures
                            .getResults()
                            .entrySet()
                            .stream()
                            .filter(e -> hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(e.getKey().toString()).isPresent())
                            .collect(
                                Collectors
                                    .groupingBy(
                                        // from entity name to its node
                                        e -> hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(e.getKey().toString()).get(),
                                        Collectors.toMap(Entry::getKey, Entry::getValue)
                                    )
                            )
                            .entrySet();

                        Iterator<Entry<DiscoveryNode, Map<Entity, double[]>>> iterator = node2Entities.iterator();

                        while (iterator.hasNext()) {
                            Entry<DiscoveryNode, Map<Entity, double[]>> entry = iterator.next();
                            DiscoveryNode modelNode = entry.getKey();
                            if (modelNode == null) {
                                iterator.remove();
                                continue;
                            }
                            String modelNodeId = modelNode.getId();
                            if (stateManager.isMuted(modelNodeId, detectorId)) {
                                LOG
                                    .info(
                                        String
                                            .format(Locale.ROOT, TimeSeriesResultTransport.NODE_UNRESPONSIVE_ERR_MSG + " %s for detector %s", modelNodeId, detectorId)
                                    );
                                iterator.remove();
                            }
                        }

                        final AtomicReference<Exception> failure = new AtomicReference<>();
                        node2Entities.stream().forEach(nodeEntity -> {
                            DiscoveryNode node = nodeEntity.getKey();
                            transportService
                                .sendRequest(
                                    node,
                                    EntityADResultAction.NAME,
                                    new EntityADResultRequest(detectorId, nodeEntity.getValue(), dataStartTime, dataEndTime),
                                    option,
                                    new ActionListenerResponseHandler<>(
                                        new EntityResultListener(node.getId(), detectorId, failure),
                                        AcknowledgedResponse::new,
                                        ThreadPool.Names.SAME
                                    )
                                );
                        });

                    } catch (Exception e) {
                        LOG.error("Unexpected exception", e);
                        handleException(e);
                    }
                });
            }
        }

        @Override
        public void onFailure(Exception e) {
            LOG.error("Unexpetected exception", e);
            handleException(e);
        }

        private void handleException(Exception e) {
            Exception convertedException = convertedQueryFailureException(e, detectorId);
            if (false == (convertedException instanceof TimeSeriesException)) {
                Throwable cause = ExceptionsHelper.unwrapCause(convertedException);
                convertedException = new InternalFailure(detectorId, cause);
            }
            forecastStateManager.setException(detectorId, convertedException);
        }
    }

    private ActionListener<Optional<Forecaster>> onGetForecaster(
        ActionListener<ForecastResultResponse> listener,
        String adID,
        ForecastResultRequest request
    ) {
        return ActionListener.wrap(forecasterOptional -> {
            if (!forecasterOptional.isPresent()) {
                listener.onFailure(new EndRunException(adID, "Forecaster is not available.", true));
                return;
            }

            Forecaster forecaster = forecasterOptional.get();
            if (forecaster.isHC()) {
                hcDetectors.add(adID);
                timeSeriesStats.getStat(StatNames.FORECAST_HC_EXECUTE_REQUEST_COUNT.getName()).increment();
            }

            long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) forecaster.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
            long dataStartTime = request.getStart() - delayMillis;
            long dataEndTime = request.getEnd() - delayMillis;

            forecastTaskManager
                .initRealtimeTaskCache(
                    adID,
                    forecaster,
                    transportService,
                    ActionListener
                        .runAfter(
                            initRealtimeTaskCacheListener(adID),
                            () -> executeAnomalyDetection(listener, adID, request, anomalyDetector, dataStartTime, dataEndTime)
                        )
                );
        }, exception -> TimeSeriesResultTransport.handleExecuteException(exception, listener, adID));
    }

    private ActionListener<Boolean> initRealtimeTaskCacheListener(String detectorId) {
        return ActionListener.wrap(r -> {
            if (r) {
                LOG.debug("Realtime task cache initied for detector {}", detectorId);
            }
        }, e -> LOG.error("Failed to init realtime task cache for " + detectorId, e));
    }

    private void executeAnomalyDetection(
        ActionListener<AnomalyResultResponse> listener,
        String adID,
        AnomalyResultRequest request,
        AnomalyDetector anomalyDetector,
        long dataStartTime,
        long dataEndTime
    ) {
        // HC logic starts here
        if (anomalyDetector.isHC()) {
            Optional<Exception> previousException = forecastStateManager.fetchExceptionAndClear(adID);
            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error(new ParameterizedMessage("Previous exception of [{}]", adID), exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }
            }

            // assume request are in epoch milliseconds
            long nextDetectionStartTime = request.getEnd() + (long) (anomalyDetector.getIntervalInMilliseconds()
                * intervalRatioForRequest);

            CompositeRetriever compositeRetriever = new CompositeRetriever(
                dataStartTime,
                dataEndTime,
                anomalyDetector,
                xContentRegistry,
                client,
                clientUtil,
                nextDetectionStartTime,
                settings,
                maxEntitiesPerInterval,
                pageSize,
                indexNameExpressionResolver,
                clusterService
            );

            PageIterator pageIterator = null;

            try {
                pageIterator = compositeRetriever.iterator();
            } catch (Exception e) {
                listener
                    .onFailure(
                        new EndRunException(anomalyDetector.getId(), CommonMessages.INVALID_SEARCH_QUERY_MSG, e, false)
                    );
                return;
            }

            PageListener getEntityFeatureslistener = new PageListener(pageIterator, adID, dataStartTime, dataEndTime);
            if (pageIterator.hasNext()) {
                pageIterator.next(getEntityFeatureslistener);
            }

            // We don't know when the pagination will not finish. To not
            // block the following interval request to start, we return immediately.
            // Pagination will stop itself when the time is up.
            if (previousException.isPresent()) {
                listener.onFailure(previousException.get());
            } else {
                listener
                    .onResponse(
                        new AnomalyResultResponse(
                            new ArrayList<FeatureData>(),
                            null,
                            null,
                            anomalyDetector.getIntervalInMinutes(),
                            true
                        )
                    );
            }
            return;
        }

        // HC logic ends and single entity logic starts here
        // We are going to use only 1 model partition for a single stream detector.
        // That's why we use 0 here.
        String rcfModelID = SingleStreamModelIdMapper.getRcfModelId(adID, 0);
        Optional<DiscoveryNode> asRCFNode = hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(rcfModelID);
        if (!asRCFNode.isPresent()) {
            listener.onFailure(new InternalFailure(adID, "RCF model node is not available."));
            return;
        }

        DiscoveryNode rcfNode = asRCFNode.get();

        if (!shouldStart(listener, adID, anomalyDetector, rcfNode.getId(), rcfModelID)) {
            return;
        }

        featureManager
            .getCurrentFeatures(
                anomalyDetector,
                dataStartTime,
                dataEndTime,
                onFeatureResponseForSingleEntityDetector(adID, anomalyDetector, listener, rcfModelID, rcfNode, dataStartTime, dataEndTime)
            );
    }

    // For single entity detector
    private ActionListener<SinglePointFeatures> onFeatureResponseForSingleEntityDetector(
        String adID,
        AnomalyDetector detector,
        ActionListener<AnomalyResultResponse> listener,
        String rcfModelId,
        DiscoveryNode rcfNode,
        long dataStartTime,
        long dataEndTime
    ) {
        return ActionListener.wrap(featureOptional -> {
            List<FeatureData> featureInResponse = null;
            if (featureOptional.getUnprocessedFeatures().isPresent()) {
                featureInResponse = ParseUtils.getFeatureData(featureOptional.getUnprocessedFeatures().get(), detector);
            }

            if (!featureOptional.getProcessedFeatures().isPresent()) {
                Optional<Exception> exception = coldStartIfNoCheckPoint(detector);
                if (exception.isPresent()) {
                    listener.onFailure(exception.get());
                    return;
                }

                if (!featureOptional.getUnprocessedFeatures().isPresent()) {
                    // Feature not available is common when we have data holes. Respond empty response
                    // and don't log to avoid bloating our logs.
                    LOG.debug("No data in current detection window between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                new ArrayList<FeatureData>(),
                                "No data in current detection window",
                                null,
                                null,
                                false
                            )
                        );
                } else {
                    LOG.debug("Return at least current feature value between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            new AnomalyResultResponse(featureInResponse, "No full shingle in current detection window", null, null, false)
                        );
                }
                return;
            }

            final AtomicReference<Exception> failure = new AtomicReference<Exception>();

            LOG.info("Sending RCF request to {} for model {}", rcfNode.getId(), rcfModelId);

            RCFActionListener rcfListener = new RCFActionListener(
                rcfModelId,
                failure,
                rcfNode.getId(),
                detector,
                listener,
                featureInResponse,
                adID
            );

            transportService
                .sendRequest(
                    rcfNode,
                    RCFResultAction.NAME,
                    new RCFResultRequest(adID, rcfModelId, featureOptional.getProcessedFeatures().get()),
                    option,
                    new ActionListenerResponseHandler<>(rcfListener, RCFResultResponse::new)
                );
        }, exception -> { handleQueryFailure(exception, listener, adID); });
    }

    private void handleQueryFailure(Exception exception, ActionListener<AnomalyResultResponse> listener, String adID) {
        Exception convertedQueryFailureException = convertedQueryFailureException(exception, adID);

        if (convertedQueryFailureException instanceof EndRunException) {
            // invalid feature query
            listener.onFailure(convertedQueryFailureException);
        } else {
            TimeSeriesResultTransport.handleExecuteException(convertedQueryFailureException, listener, adID);
        }
    }

    /**
     * Convert a query related exception to EndRunException
     *
     * These query exception can happen during the starting phase of the OpenSearch
     * process.  Thus, set the stopNow parameter of these EndRunException to false
     * and confirm the EndRunException is not a false positive.
     *
     * @param exception Exception
     * @param adID detector Id
     * @return the converted exception if the exception is query related
     */
    private Exception convertedQueryFailureException(Exception exception, String adID) {
        if (ExceptionUtil.isIndexNotAvailable(exception)) {
            return new EndRunException(adID, TimeSeriesResultTransport.TROUBLE_QUERYING_ERR_MSG + exception.getMessage(), false).countedInStats(false);
        } else if (exception instanceof SearchPhaseExecutionException && invalidQuery((SearchPhaseExecutionException) exception)) {
            // This is to catch invalid aggregation on wrong field type. For example,
            // sum aggregation on text field. We should end detector run for such case.
            return new EndRunException(
                adID,
                CommonMessages.INVALID_SEARCH_QUERY_MSG + " " + ((SearchPhaseExecutionException) exception).getDetailedMessage(),
                exception,
                false
            ).countedInStats(false);
        }

        return exception;
    }

    /**
     * Verify failure of rcf or threshold models. If there is no model, trigger cold
     * start. If there is an exception for the previous cold start of this detector,
     * throw exception to the caller.
     *
     * @param failure  object that may contain exceptions thrown
     * @param detector detector object
     * @return exception if AD job execution gets resource not found exception
     * @throws Exception when the input failure is not a ResourceNotFoundException.
     *   List of exceptions we can throw
     *     1. Exception from cold start:
     *       1). InternalFailure due to
     *         a. OpenSearchTimeoutException thrown by putModelCheckpoint during cold start
     *       2). EndRunException with endNow equal to false
     *         a. training data not available
     *         b. cold start cannot succeed
     *         c. invalid training data
     *       3) EndRunException with endNow equal to true
     *         a. invalid search query
     *     2. LimitExceededException from one of RCF model node when the total size of the models
     *      is more than X% of heap memory.
     *     3. InternalFailure wrapping OpenSearchTimeoutException inside caused by
     *      RCF/Threshold model node failing to get checkpoint to restore model before timeout.
     */
    private Exception coldStartIfNoModel(AtomicReference<Exception> failure, AnomalyDetector detector) throws Exception {
        Exception exp = failure.get();
        if (exp == null) {
            return null;
        }

        // return exceptions like LimitExceededException to caller
        if (!(exp instanceof ResourceNotFoundException)) {
            return exp;
        }

        // fetch previous cold start exception
        String adID = detector.getId();
        final Optional<Exception> previousException = forecastStateManager.fetchExceptionAndClear(adID);
        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error("Previous exception of {}: {}", () -> adID, () -> exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return exception;
            }
        }
        LOG.info("Trigger cold start for {}", detector.getId());
        coldStart(detector);
        return previousException.orElse(new InternalFailure(adID, ADCommonMessages.NO_MODEL_ERR_MSG));
    }

    private void findException(Throwable cause, String adID, AtomicReference<Exception> failure, String nodeId) {
        if (cause == null) {
            LOG.error(new ParameterizedMessage("Null input exception"));
            return;
        }
        if (cause instanceof Error) {
            // we cannot do anything with Error.
            LOG.error(new ParameterizedMessage("Error during prediction for {}: ", adID), cause);
            return;
        }

        Exception causeException = (Exception) cause;

        if (causeException instanceof TimeSeriesException) {
            failure.set(causeException);
        } else if (causeException instanceof NotSerializableExceptionWrapper) {
            // we only expect this happens on AD exceptions
            Optional<TimeSeriesException> actualException = NotSerializedADExceptionName
                .convertWrappedAnomalyDetectionException((NotSerializableExceptionWrapper) causeException, adID);
            if (actualException.isPresent()) {
                TimeSeriesException adException = actualException.get();
                failure.set(adException);
                if (adException instanceof ResourceNotFoundException) {
                    // During a rolling upgrade or blue/green deployment, ResourceNotFoundException might be caused by old node using RCF
                    // 1.0
                    // cannot recognize new checkpoint produced by the coordinating node using compact RCF. Add pressure to mute the node
                    // after consecutive failures.
                    stateManager.addPressure(nodeId, adID);
                }
            } else {
                // some unexpected bugs occur while predicting anomaly
                failure.set(new EndRunException(adID, CommonMessages.BUG_RESPONSE, causeException, false));
            }
        } else if (causeException instanceof IndexNotFoundException
            && causeException.getMessage().contains(ADCommonName.CHECKPOINT_INDEX_NAME)) {
            // checkpoint index does not exist
            // ResourceNotFoundException will trigger cold start later
            failure.set(new ResourceNotFoundException(adID, causeException.getMessage()));
        } else if (causeException instanceof OpenSearchTimeoutException) {
            // we can have OpenSearchTimeoutException when a node tries to load RCF or
            // threshold model
            failure.set(new InternalFailure(adID, causeException));
        } else if (causeException instanceof IllegalArgumentException) {
            // we can have IllegalArgumentException when a model is corrupted
            failure.set(new InternalFailure(adID, causeException));
        } else {
            // some unexpected bug occurred or cluster is unstable (e.g., ClusterBlockException) or index is red (e.g.
            // NoShardAvailableActionException) while predicting anomaly
            failure.set(new EndRunException(adID, CommonMessages.BUG_RESPONSE, causeException, false));
        }
    }

    private boolean invalidQuery(SearchPhaseExecutionException ex) {
        // If all shards return bad request and failure cause is IllegalArgumentException, we
        // consider the feature query is invalid and will not count the error in failure stats.
        for (ShardSearchFailure failure : ex.shardFailures()) {
            if (RestStatus.BAD_REQUEST != failure.status() || !(failure.getCause() instanceof IllegalArgumentException)) {
                return false;
            }
        }
        return true;
    }

    // For single entity detector
    class RCFActionListener implements ActionListener<RCFResultResponse> {
        private String modelID;
        private AtomicReference<Exception> failure;
        private String rcfNodeID;
        private AnomalyDetector detector;
        private ActionListener<ForecastResultResponse> listener;
        private List<FeatureData> featureInResponse;
        private final String adID;

        RCFActionListener(
            String modelID,
            AtomicReference<Exception> failure,
            String rcfNodeID,
            AnomalyDetector detector,
            ActionListener<AnomalyResultResponse> listener,
            List<FeatureData> features,
            String adID
        ) {
            this.modelID = modelID;
            this.failure = failure;
            this.rcfNodeID = rcfNodeID;
            this.detector = detector;
            this.listener = listener;
            this.featureInResponse = features;
            this.adID = adID;
        }

        @Override
        public void onResponse(RCFResultResponse response) {
            try {
                forecastStateManager.resetBackpressureCounter(rcfNodeID, adID);
                if (response != null) {
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                response.getAnomalyGrade(),
                                response.getConfidence(),
                                response.getRCFScore(),
                                featureInResponse,
                                null,
                                response.getTotalUpdates(),
                                detector.getIntervalInMinutes(),
                                false,
                                response.getRelativeIndex(),
                                response.getAttribution(),
                                response.getPastValues(),
                                response.getExpectedValuesList(),
                                response.getLikelihoodOfValues(),
                                response.getThreshold()
                            )
                        );
                } else {
                    LOG.warn(TimeSeriesResultTransport.NULL_RESPONSE + " {} for {}", modelID, rcfNodeID);
                    listener.onFailure(new InternalFailure(adID, ADCommonMessages.NO_MODEL_ERR_MSG));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                TimeSeriesResultTransport.handleExecuteException(ex, listener, adID);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                handlePredictionFailure(e, adID, rcfNodeID, failure);
                Exception exception = coldStartIfNoModel(failure, detector);
                if (exception != null) {
                    listener.onFailure(exception);
                } else {
                    listener.onFailure(new InternalFailure(adID, "Node connection problem or unexpected exception"));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                TimeSeriesResultTransport.handleExecuteException(ex, listener, adID);
            }
        }
    }

    /**
     * Handle a prediction failure.  Possibly (i.e., we don't always need to do that)
     * convert the exception to a form that AD can recognize and handle and sets the
     * input failure reference to the converted exception.
     *
     * @param e prediction exception
     * @param adID Detector Id
     * @param nodeID Node Id
     * @param failure Parameter to receive the possibly converted function for the
     *  caller to deal with
     */
    private void handlePredictionFailure(Exception e, String adID, String nodeID, AtomicReference<Exception> failure) {
        LOG.error(new ParameterizedMessage("Received an error from node {} while doing model inference for {}", nodeID, adID), e);
        if (e == null) {
            return;
        }
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (hasConnectionIssue(cause)) {
            handleConnectionException(nodeID, adID);
        } else {
            findException(cause, adID, failure, nodeID);
        }
    }

    /**
     * Check if the input exception indicates connection issues.
     * During blue-green deployment, we may see ActionNotFoundTransportException.
     * Count that as connection issue and isolate that node if it continues to happen.
     *
     * @param e exception
     * @return true if we get disconnected from the node or the node is not in the
     *         right state (being closed) or transport request times out (sent from TimeoutHandler.run)
     */
    private boolean hasConnectionIssue(Throwable e) {
        return e instanceof ConnectTransportException
            || e instanceof NodeClosedException
            || e instanceof ReceiveTimeoutTransportException
            || e instanceof NodeNotConnectedException
            || e instanceof ConnectException
            || NetworkExceptionHelper.isCloseConnectionException(e)
            || e instanceof ActionNotFoundTransportException;
    }

    private void handleConnectionException(String node, String detectorId) {
        final DiscoveryNodes nodes = clusterService.state().nodes();
        if (!nodes.nodeExists(node)) {
            hashRing.buildCirclesForRealtimeAD();
            return;
        }
        // rebuilding is not done or node is unresponsive
        stateManager.addPressure(node, detectorId);
    }

    /**
     * Since we need to read from customer index and write to anomaly result index,
     * we need to make sure we can read and write.
     *
     * @param state Cluster state
     * @return whether we have global block or not
     */
    private boolean checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ) != null
            || state.blocks().globalBlockedException(ClusterBlockLevel.WRITE) != null;
    }

    /**
     * Similar to checkGlobalBlock, we check block on the indices level.
     *
     * @param state   Cluster state
     * @param level   block level
     * @param indices the indices on which to check block
     * @return whether any of the index has block on the level.
     */
    private boolean checkIndicesBlocked(ClusterState state, ClusterBlockLevel level, String... indices) {
        // the original index might be an index expression with wildcards like "log*",
        // so we need to expand the expression to concrete index name
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);

        return state.blocks().indicesBlockedException(level, concreteIndices) != null;
    }

    /**
     * Check if we should start anomaly prediction.
     *
     * @param listener listener to respond back to AnomalyResultRequest.
     * @param adID     detector ID
     * @param detector detector instance corresponds to adID
     * @param rcfNodeId the rcf model hosting node ID for adID
     * @param rcfModelID the rcf model ID for adID
     * @return if we can start anomaly prediction.
     */
    private boolean shouldStart(
        ActionListener<AnomalyResultResponse> listener,
        String adID,
        AnomalyDetector detector,
        String rcfNodeId,
        String rcfModelID
    ) {
        ClusterState state = clusterService.state();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(adID, TimeSeriesResultTransport.READ_WRITE_BLOCKED));
            return false;
        }

        if (stateManager.isMuted(rcfNodeId, adID)) {
            listener
                .onFailure(
                    new InternalFailure(
                        adID,
                        String.format(Locale.ROOT, TimeSeriesResultTransport.NODE_UNRESPONSIVE_ERR_MSG + " %s for rcf model %s", rcfNodeId, rcfModelID)
                    )
                );
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, detector.getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(adID, TimeSeriesResultTransport.INDEX_READ_BLOCKED));
            return false;
        }

        return true;
    }

    private void coldStart(AnomalyDetector detector) {
        String detectorId = detector.getId();

        // If last cold start is not finished, we don't trigger another one
        if (forecastStateManager.isColdStartRunning(detectorId)) {
            return;
        }

        final Releasable coldStartFinishingCallback = forecastStateManager.markColdStartRunning(detectorId);

        ActionListener<Optional<double[][]>> listener = ActionListener.wrap(trainingData -> {
            if (trainingData.isPresent()) {
                double[][] dataPoints = trainingData.get();

                ActionListener<Void> trainModelListener = ActionListener
                    .wrap(res -> { LOG.info("Succeeded in training {}", detectorId); }, exception -> {
                        if (exception instanceof TimeSeriesException) {
                            // e.g., partitioned model exceeds memory limit
                            forecastStateManager.setException(detectorId, exception);
                        } else if (exception instanceof IllegalArgumentException) {
                            // IllegalArgumentException due to invalid training data
                            forecastStateManager
                                .setException(detectorId, new EndRunException(detectorId, "Invalid training data", exception, false));
                        } else if (exception instanceof OpenSearchTimeoutException) {
                            forecastStateManager
                                .setException(
                                    detectorId,
                                    new InternalFailure(detectorId, "Time out while indexing cold start checkpoint", exception)
                                );
                        } else {
                            forecastStateManager
                                .setException(detectorId, new EndRunException(detectorId, "Error while training model", exception, false));
                        }
                    });

                modelManager
                    .trainModel(
                        detector,
                        dataPoints,
                        new ThreadedActionListener<>(LOG, threadPool, TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME, trainModelListener, false)
                    );
            } else {
                forecastStateManager.setException(detectorId, new EndRunException(detectorId, "Cannot get training data", false));
            }
        }, exception -> {
            if (exception instanceof OpenSearchTimeoutException) {
                forecastStateManager.setException(detectorId, new InternalFailure(detectorId, "Time out while getting training data", exception));
            } else if (exception instanceof TimeSeriesException) {
                // e.g., Invalid search query
                forecastStateManager.setException(detectorId, exception);
            } else {
                forecastStateManager.setException(detectorId, new EndRunException(detectorId, "Error while cold start", exception, false));
            }
        });

        final ActionListener<Optional<double[][]>> listenerWithReleaseCallback = ActionListener
            .runAfter(listener, coldStartFinishingCallback::close);

        threadPool
            .executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)
            .execute(
                () -> featureManager
                    .getColdStartData(
                        detector,
                        new ThreadedActionListener<>(
                            LOG,
                            threadPool,
                            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
                            listenerWithReleaseCallback,
                            false
                        )
                    )
            );
    }

    /**
     * Check if checkpoint for an detector exists or not.  If not and previous
     *  run is not EndRunException whose endNow is true, trigger cold start.
     * @param detector detector object
     * @return previous cold start exception
     */
    private Optional<Exception> coldStartIfNoCheckPoint(AnomalyDetector detector) {
        String detectorId = detector.getId();

        Optional<Exception> previousException = forecastStateManager.fetchExceptionAndClear(detectorId);

        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error(new ParameterizedMessage("Previous exception of {}:", detectorId), exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return previousException;
            }
        }

        forecastStateManager.getDetectorCheckpoint(detectorId, ActionListener.wrap(checkpointExists -> {
            if (!checkpointExists) {
                LOG.info("Trigger cold start for {}", id);
                coldStart(detector);
            }
        }, exception -> {
            Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (cause instanceof IndexNotFoundException) {
                LOG.info("Trigger cold start for {}", id);
                coldStart(detector);
            } else {
                String errorMsg = String.format(Locale.ROOT, "Fail to get checkpoint state for %s", id);
                LOG.error(errorMsg, exception);
                forecastStateManager.setException(id, new TimeSeriesException(errorMsg, exception));
            }
        }));

        return previousException;
    }

    class EntityResultListener implements ActionListener<AcknowledgedResponse> {
        private String nodeId;
        private final String adID;
        private AtomicReference<Exception> failure;

        EntityResultListener(String nodeId, String adID, AtomicReference<Exception> failure) {
            this.nodeId = nodeId;
            this.adID = adID;
            this.failure = failure;
        }

        @Override
        public void onResponse(AcknowledgedResponse response) {
            try {
                if (response.isAcknowledged() == false) {
                    LOG.error("Cannot send entities' features to {} for {}", nodeId, adID);
                    stateManager.addPressure(nodeId, adID);
                } else {
                    stateManager.resetBackpressureCounter(nodeId, adID);
                }
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
                handleException(ex);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                // e.g., we have connection issues with all of the nodes while restarting clusters
                LOG.error(new ParameterizedMessage("Cannot send entities' features to {} for {}", nodeId, adID), e);

                handleException(e);

            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
                handleException(ex);
            }
        }

        private void handleException(Exception e) {
            handlePredictionFailure(e, adID, nodeId, failure);
            if (failure.get() != null) {
                forecastStateManager.setException(adID, failure.get());
            }
        }
    }
}

