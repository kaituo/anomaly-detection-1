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

package org.opensearch.timeseries.transport;

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
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.task.ADTaskManager;
//import org.opensearch.ad.ADNodeStateManager;
//import org.opensearch.ad.constant.ADCommonMessages;
//import org.opensearch.ad.constant.ADCommonName;
//import org.opensearch.ad.ml.ADModelManager;
//import org.opensearch.ad.model.AnomalyDetector;
//import org.opensearch.ad.settings.AnomalyDetectorSettings;
//import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.timeseries.stats.TimeSeriesStats;
import org.opensearch.timeseries.task.TimeSeriesRealTimeTaskManager;
//import org.opensearch.ad.task.ADTaskManager;
//import org.opensearch.ad.transport.EntityADResultAction;
//import org.opensearch.ad.transport.EntityADResultRequest;
//import org.opensearch.ad.transport.AnomalyResultTransportAction.EntityResultListener;
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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.NetworkExceptionHelper;
import org.opensearch.common.unit.TimeValue;
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
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
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
import org.opensearch.timeseries.model.Config;
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

public class TimeSeriesResultProcessor<EntityResultRequestType extends TimeSeriesEntityResultRequest, NodeStateType extends ExpiringState,
    TransportResultRequestType extends TimeSeriesResultRequest,
    TransportResultResponseType extends TimeSeriesResultResponse
    > {

    private static final Logger LOG = LogManager.getLogger(TimeSeriesResultProcessor.class);

    static final String WAIT_FOR_THRESHOLD_ERR_MSG = "Exception in waiting for threshold result";

    static final String NO_ACK_ERR = "no acknowledgements from model hosting nodes.";

    public static final String TROUBLE_QUERYING_ERR_MSG = "Having trouble querying data: ";

    public static final String NULL_RESPONSE = "Received null response from";

    public static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";

    public static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";

    public static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute node";

    private final TransportRequestOptions option;
    private String entityResultAction;
    private Class<EntityResultRequestType> entityResultClazz;
    private Class<TransportResultResponseType> transportResultResponseClazz;
    private StatNames hcRequestCountStat;
    private String threadPoolName;
    // Cache HC detector id. This is used to count HC failure stats. We can tell a detector
    // is HC or not by checking if detector id exists in this field or not. Will add
    // detector id to this field when start to run realtime detection and remove detector
    // id once realtime detection done.
    private final Set<String> hcDetectors;
    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the detection interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    private final float intervalRatioForRequest;
    private int maxEntitiesPerInterval;
    private int pageSize;
    private final ThreadPool threadPool;
    private final HashRing hashRing;
    private final TimeSeriesNodeStateManager<NodeStateType> nodeStateManager;
    private final TransportService transportService;
    private final TimeSeriesStats timeSeriesStats;
    private final TimeSeriesRealTimeTaskManager timeSeriesRealTimeTaskManager;
    private NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final SecurityClientUtil clientUtil;
    private Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;
    private final FeatureManager featureManager;

    public TimeSeriesResultProcessor(
        Setting<TimeValue> requestTimeoutSetting,
        float intervalRatioForRequests,
        String entityResultAction,
        Class<EntityResultRequestType> entityResultClazz,
        StatNames hcRequestCountStat,
        String threadPoolName,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        HashRing hashRing,
        TimeSeriesNodeStateManager<NodeStateType> nodeStateManager,
        TransportService transportService,
        TimeSeriesStats timeSeriesStats,
        TimeSeriesRealTimeTaskManager timeSeriesRealTimeTaskManager,
        NamedXContentRegistry xContentRegistry,
        Client client,
        SecurityClientUtil clientUtil,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Class<TransportResultResponseType> transportResultResponseClazz,
        FeatureManager featureManager
    ) {
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(requestTimeoutSetting.get(settings))
            .build();
        this.hcDetectors = new HashSet<>();
        this.intervalRatioForRequest = intervalRatioForRequests;

        this.maxEntitiesPerInterval = MAX_ENTITIES_PER_QUERY.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ENTITIES_PER_QUERY, it -> maxEntitiesPerInterval = it);

        this.pageSize = PAGE_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PAGE_SIZE, it -> pageSize = it);
        this.entityResultAction = entityResultAction;
        this.entityResultClazz = entityResultClazz;
        this.hcRequestCountStat = hcRequestCountStat;
        this.threadPool = threadPool;
        this.hashRing = hashRing;
        this.nodeStateManager = nodeStateManager;
        this.transportService = transportService;
        this.timeSeriesStats = timeSeriesStats;
        this.timeSeriesRealTimeTaskManager = timeSeriesRealTimeTaskManager;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.clientUtil = clientUtil;
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
        this.transportResultResponseClazz = transportResultResponseClazz;
        this.featureManager = featureManager;
    }

    /**
     * didn't use ActionListener.wrap so that I can
     * 1) use this to refer to the listener inside the listener
     * 2) pass parameters using constructors
     *
     */
    class PageListener implements ActionListener<CompositeRetriever.Page> {
        private PageIterator pageIterator;
        private String configId;
        private long dataStartTime;
        private long dataEndTime;

        PageListener(PageIterator pageIterator, String detectorId, long dataStartTime, long dataEndTime) {
            this.pageIterator = pageIterator;
            this.configId = detectorId;
            this.dataStartTime = dataStartTime;
            this.dataEndTime = dataEndTime;
        }

        @Override
        public void onResponse(CompositeRetriever.Page entityFeatures) {
            if (pageIterator.hasNext()) {
                pageIterator.next(this);
            }
            if (entityFeatures != null && false == entityFeatures.isEmpty()) {
                // wrap expensive operation inside ad threadpool
                threadPool.executor(threadPoolName).execute(() -> {
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
                            if (nodeStateManager.isMuted(modelNodeId, configId)) {
                                LOG
                                    .info(
                                        String
                                            .format(Locale.ROOT, TimeSeriesResultProcessor.NODE_UNRESPONSIVE_ERR_MSG + " %s for detector %s", modelNodeId, configId)
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
                                    entityResultAction,
                                    TimeSeriesEntityResultRequest.create(configId, nodeEntity.getValue(), dataStartTime, dataEndTime, entityResultClazz),
                                    option,
                                    new ActionListenerResponseHandler<>(
                                        new EntityResultListener(node.getId(), configId, failure),
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
            Exception convertedException = convertedQueryFailureException(e, configId);
            if (false == (convertedException instanceof TimeSeriesException)) {
                Throwable cause = ExceptionsHelper.unwrapCause(convertedException);
                convertedException = new InternalFailure(configId, cause);
            }
            nodeStateManager.setException(configId, convertedException);
        }
    }

    private ActionListener<Optional<Config>> onGetConfig(
        ActionListener<TransportResultResponseType> listener,
        String configID,
        TransportResultRequestType request
    ) {
        return ActionListener.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new EndRunException(configID, "config is not available.", true));
                return;
            }

            Config config = configOptional.get();
            if (config.isHC()) {
                hcDetectors.add(configID);
                timeSeriesStats.getStat(hcRequestCountStat.getName()).increment();
            }

            long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) config.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
            long dataStartTime = request.getStart() - delayMillis;
            long dataEndTime = request.getEnd() - delayMillis;

            timeSeriesRealTimeTaskManager
                .initCacheWithCleanupIfRequired(
                    configID,
                    config,
                    transportService,
                    ActionListener
                        .runAfter(
                            initRealtimeTaskCacheListener(configID),
                            () -> executeAnalysis(listener, configID, request, config, dataStartTime, dataEndTime)
                        )
                );
        }, exception -> TimeSeriesResultProcessor.handleExecuteException(exception, listener, configID));
    }

    private ActionListener<Boolean> initRealtimeTaskCacheListener(String detectorId) {
        return ActionListener.wrap(r -> {
            if (r) {
                LOG.debug("Realtime task cache initied for detector {}", detectorId);
            }
        }, e -> LOG.error("Failed to init realtime task cache for " + detectorId, e));
    }

    private void executeAnalysis(
        ActionListener<TransportResultResponseType> listener,
        String configID,
        TimeSeriesResultRequest request,
        Config config,
        long dataStartTime,
        long dataEndTime
    ) {
        // HC logic starts here
        if (config.isHC()) {
            Optional<Exception> previousException = nodeStateManager.fetchExceptionAndClear(configID);
            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error(new ParameterizedMessage("Previous exception of [{}]", configID), exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }
            }

            // assume request are in epoch milliseconds
            long nextDetectionStartTime = request.getEnd() + (long) (config.getIntervalInMilliseconds()
                * intervalRatioForRequest);

            CompositeRetriever compositeRetriever = new CompositeRetriever(
                dataStartTime,
                dataEndTime,
                config,
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
                        new EndRunException(config.getId(), CommonMessages.INVALID_SEARCH_QUERY_MSG, e, false)
                    );
                return;
            }

            PageListener getEntityFeatureslistener = new PageListener(pageIterator, configID, dataStartTime, dataEndTime);
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
                            TimeSeriesResultResponse.create(
                            new ArrayList<FeatureData>(),
                            null,
                            null,
                            config.getIntervalInMinutes(),
                            true,
                            transportResultResponseClazz
                        )
                    );
            }
            return;
        }

        // HC logic ends and single entity logic starts here
        // We are going to use only 1 model partition for a single stream detector.
        // That's why we use 0 here.
        String rcfModelID = SingleStreamModelIdMapper.getRcfModelId(configID, 0);
        Optional<DiscoveryNode> asRCFNode = hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(rcfModelID);
        if (!asRCFNode.isPresent()) {
            listener.onFailure(new InternalFailure(configID, "RCF model node is not available."));
            return;
        }

        DiscoveryNode rcfNode = asRCFNode.get();

        if (!shouldStart(listener, configID, config, rcfNode.getId(), rcfModelID)) {
            return;
        }

        featureManager
            .getCurrentFeatures(
                config,
                dataStartTime,
                dataEndTime,
                onFeatureResponseForSingleEntityDetector(configID, config, listener, rcfModelID, rcfNode, dataStartTime, dataEndTime)
            );
    }

    // For single entity detector
    private ActionListener<SinglePointFeatures> onFeatureResponseForSingleEntityDetector(
        String adID,
        Config detector,
        ActionListener<TransportResultResponseType> listener,
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
            TimeSeriesResultProcessor.handleExecuteException(convertedQueryFailureException, listener, adID);
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
            return new EndRunException(adID, TimeSeriesResultProcessor.TROUBLE_QUERYING_ERR_MSG + exception.getMessage(), false).countedInStats(false);
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
    private Exception coldStartIfNoModel(AtomicReference<Exception> failure, Config detector) throws Exception {
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
        private Config detector;
        private ActionListener<ForecastResultResponse> listener;
        private List<FeatureData> featureInResponse;
        private final String adID;

        RCFActionListener(
            String modelID,
            AtomicReference<Exception> failure,
            String rcfNodeID,
            Config detector,
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
                    LOG.warn(TimeSeriesResultProcessor.NULL_RESPONSE + " {} for {}", modelID, rcfNodeID);
                    listener.onFailure(new InternalFailure(adID, ADCommonMessages.NO_MODEL_ERR_MSG));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                TimeSeriesResultProcessor.handleExecuteException(ex, listener, adID);
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
                TimeSeriesResultProcessor.handleExecuteException(ex, listener, adID);
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
        ActionListener<TransportResultResponseType> listener,
        String adID,
        Config detector,
        String rcfNodeId,
        String rcfModelID
    ) {
        ClusterState state = clusterService.state();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(adID, TimeSeriesResultProcessor.READ_WRITE_BLOCKED));
            return false;
        }

        if (nodeStateManager.isMuted(rcfNodeId, adID)) {
            listener
                .onFailure(
                    new InternalFailure(
                        adID,
                        String.format(Locale.ROOT, TimeSeriesResultProcessor.NODE_UNRESPONSIVE_ERR_MSG + " %s for rcf model %s", rcfNodeId, rcfModelID)
                    )
                );
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, detector.getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(adID, TimeSeriesResultProcessor.INDEX_READ_BLOCKED));
            return false;
        }

        return true;
    }

    private void coldStart(Config detector) {
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
    private Optional<Exception> coldStartIfNoCheckPoint(Config detector) {
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
                LOG.info("Trigger cold start for {}", configId);
                coldStart(detector);
            }
        }, exception -> {
            Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (cause instanceof IndexNotFoundException) {
                LOG.info("Trigger cold start for {}", configId);
                coldStart(detector);
            } else {
                String errorMsg = String.format(Locale.ROOT, "Fail to get checkpoint state for %s", configId);
                LOG.error(errorMsg, exception);
                forecastStateManager.setException(configId, new TimeSeriesException(errorMsg, exception));
            }
        }));

        return previousException;
    }

    public static void handleExecuteException(Exception ex, ActionListener<? extends ActionResponse> listener, String id) {
        if (ex instanceof ClientException) {
            listener.onFailure(ex);
        } else if (ex instanceof TimeSeriesException) {
            listener.onFailure(new InternalFailure((TimeSeriesException) ex));
        } else {
            Throwable cause = ExceptionsHelper.unwrapCause(ex);
            listener.onFailure(new InternalFailure(id, cause));
        }
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


