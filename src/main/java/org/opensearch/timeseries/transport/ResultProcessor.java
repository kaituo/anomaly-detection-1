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

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.opensearch.action.ActionResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.NetworkExceptionHelper;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.node.NodeClosedException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.ClientException;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.NotSerializedExceptionName;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.CompositeRetriever;
import org.opensearch.timeseries.feature.CompositeRetriever.PageIterator;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SinglePointFeatures;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.ActionNotFoundTransportException;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

public abstract class ResultProcessor<TransportResultRequestType extends ResultRequest, IndexableResultType extends IndexableResult, TransportResultResponseType extends ResultResponse<IndexableResultType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>> {

    private static final Logger LOG = LogManager.getLogger(ResultProcessor.class);

    static final String WAIT_FOR_THRESHOLD_ERR_MSG = "Exception in waiting for threshold result";

    static final String NO_ACK_ERR = "no acknowledgements from model hosting nodes.";

    public static final String TROUBLE_QUERYING_ERR_MSG = "Having trouble querying data: ";

    public static final String NULL_RESPONSE = "Received null response from";

    public static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";

    public static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";

    public static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute node";

    protected final TransportRequestOptions option;
    private String entityResultAction;
    protected Class<TransportResultResponseType> transportResultResponseClazz;
    private StatNames hcRequestCountStat;
    private String threadPoolName;
    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the detection interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    private final float intervalRatioForRequest;
    private int maxEntitiesPerInterval;
    private int pageSize;
    protected final ThreadPool threadPool;
    private final HashRing hashRing;
    protected final NodeStateManager nodeStateManager;
    protected final TransportService transportService;
    private final Stats timeSeriesStats;
    private final TaskManagerType realTimeTaskManager;
    private NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final SecurityClientUtil clientUtil;
    private Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;
    protected final FeatureManager featureManager;
    protected final AnalysisType context;

    public ResultProcessor(
        Setting<TimeValue> requestTimeoutSetting,
        float intervalRatioForRequests,
        String entityResultAction,
        StatNames hcRequestCountStat,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        String threadPoolName,
        HashRing hashRing,
        NodeStateManager nodeStateManager,
        TransportService transportService,
        Stats timeSeriesStats,
        TaskManagerType realTimeTaskManager,
        NamedXContentRegistry xContentRegistry,
        Client client,
        SecurityClientUtil clientUtil,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Class<TransportResultResponseType> transportResultResponseClazz,
        FeatureManager featureManager,
        Setting<Integer> maxEntitiesPerIntervalSetting,
        Setting<Integer> pageSizeSetting,
        AnalysisType context
    ) {
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(requestTimeoutSetting.get(settings))
            .build();
        this.intervalRatioForRequest = intervalRatioForRequests;

        this.maxEntitiesPerInterval = maxEntitiesPerIntervalSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(maxEntitiesPerIntervalSetting, it -> maxEntitiesPerInterval = it);

        this.pageSize = pageSizeSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(pageSizeSetting, it -> pageSize = it);

        this.entityResultAction = entityResultAction;
        this.hcRequestCountStat = hcRequestCountStat;
        this.threadPool = threadPool;
        this.hashRing = hashRing;
        this.nodeStateManager = nodeStateManager;
        this.transportService = transportService;
        this.timeSeriesStats = timeSeriesStats;
        this.realTimeTaskManager = realTimeTaskManager;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.clientUtil = clientUtil;
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
        this.transportResultResponseClazz = transportResultResponseClazz;
        this.featureManager = featureManager;
        this.context = context;
        this.threadPoolName = threadPoolName;
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
                            .filter(e -> hashRing.getOwningNodeWithSameLocalVersionForRealtime(e.getKey().toString()).isPresent())
                            .collect(
                                Collectors
                                    .groupingBy(
                                        // from entity name to its node
                                        e -> hashRing.getOwningNodeWithSameLocalVersionForRealtime(e.getKey().toString()).get(),
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
                                            .format(
                                                Locale.ROOT,
                                                ResultProcessor.NODE_UNRESPONSIVE_ERR_MSG + " %s for detector %s",
                                                modelNodeId,
                                                configId
                                            )
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
                                    new EntityResultRequest(configId, nodeEntity.getValue(), dataStartTime, dataEndTime),
                                    option,
                                    new ActionListenerResponseHandler<>(
                                        new ErrorResponseListener(node.getId(), configId, failure),
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

    public ActionListener<Optional<? extends Config>> onGetConfig(
        ActionListener<TransportResultResponseType> listener,
        String configID,
        TransportResultRequestType request,
        Set<String> hcDetectors
    ) {
        return ActionListener.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new EndRunException(configID, "config is not available.", true));
                return;
            }

            Config config = configOptional.get();
            if (config.isHighCardinality()) {
                hcDetectors.add(configID);
                timeSeriesStats.getStat(hcRequestCountStat.getName()).increment();
            }

            long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) config.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
            long dataStartTime = request.getStart() - delayMillis;
            long dataEndTime = request.getEnd() - delayMillis;

            realTimeTaskManager
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
        }, exception -> ResultProcessor.handleExecuteException(exception, listener, configID));
    }

    private ActionListener<Boolean> initRealtimeTaskCacheListener(String configId) {
        return ActionListener.wrap(r -> {
            if (r) {
                LOG.debug("Realtime task cache initied for config {}", configId);
            }
        }, e -> LOG.error("Failed to init realtime task cache for " + configId, e));
    }

    private void executeAnalysis(
        ActionListener<TransportResultResponseType> listener,
        String configID,
        ResultRequest request,
        Config config,
        long dataStartTime,
        long dataEndTime
    ) {
        // HC logic starts here
        if (config.isHighCardinality()) {
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
            long nextDetectionStartTime = request.getEnd() + (long) (config.getIntervalInMilliseconds() * intervalRatioForRequest);

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
                clusterService,
                context
            );

            PageIterator pageIterator = null;

            try {
                pageIterator = compositeRetriever.iterator();
            } catch (Exception e) {
                listener.onFailure(new EndRunException(config.getId(), CommonMessages.INVALID_SEARCH_QUERY_MSG, e, false));
                return;
            }

            PageListener getEntityFeatureslistener = new PageListener(pageIterator, configID, dataStartTime, dataEndTime);
            if (pageIterator.hasNext()) {
                pageIterator.next(getEntityFeatureslistener);
            }

            // We don't know when the pagination will finish. To not
            // block the following interval request to start, we return immediately.
            // Pagination will stop itself when the time is up.
            if (previousException.isPresent()) {
                listener.onFailure(previousException.get());
            } else {
                listener
                    .onResponse(
                        ResultResponse
                            .create(
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
        Optional<DiscoveryNode> asRCFNode = hashRing.getOwningNodeWithSameLocalVersionForRealtime(rcfModelID);
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
                onFeatureResponseForSingleStreamConfig(configID, config, listener, rcfModelID, rcfNode, dataStartTime, dataEndTime)
            );
    }

    protected abstract ActionListener<SinglePointFeatures> onFeatureResponseForSingleStreamConfig(
        String configId,
        Config config,
        ActionListener<TransportResultResponseType> listener,
        String rcfModelId,
        DiscoveryNode rcfNode,
        long dataStartTime,
        long dataEndTime
    );

    protected void handleQueryFailure(Exception exception, ActionListener<TransportResultResponseType> listener, String adID) {
        Exception convertedQueryFailureException = convertedQueryFailureException(exception, adID);

        if (convertedQueryFailureException instanceof EndRunException) {
            // invalid feature query
            listener.onFailure(convertedQueryFailureException);
        } else {
            ResultProcessor.handleExecuteException(convertedQueryFailureException, listener, adID);
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
            return new EndRunException(adID, ResultProcessor.TROUBLE_QUERYING_ERR_MSG + exception.getMessage(), false)
                .countedInStats(false);
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

    protected void findException(Throwable cause, String configID, AtomicReference<Exception> failure, String nodeId) {
        if (cause == null) {
            LOG.error(new ParameterizedMessage("Null input exception"));
            return;
        }
        if (cause instanceof Error) {
            // we cannot do anything with Error.
            LOG.error(new ParameterizedMessage("Error during prediction for {}: ", configID), cause);
            return;
        }

        Exception causeException = (Exception) cause;

        if (causeException instanceof TimeSeriesException) {
            failure.set(causeException);
        } else if (causeException instanceof NotSerializableExceptionWrapper) {
            // we only expect this happens on AD exceptions
            Optional<TimeSeriesException> actualException = NotSerializedExceptionName
                .convertWrappedTimeSeriesException((NotSerializableExceptionWrapper) causeException, configID);
            if (actualException.isPresent()) {
                TimeSeriesException adException = actualException.get();
                failure.set(adException);
                if (adException instanceof ResourceNotFoundException) {
                    // During a rolling upgrade or blue/green deployment, ResourceNotFoundException might be caused by old node using RCF
                    // 1.0
                    // cannot recognize new checkpoint produced by the coordinating node using compact RCF. Add pressure to mute the node
                    // after consecutive failures.
                    nodeStateManager.addPressure(nodeId, configID);
                }
            } else {
                // some unexpected bugs occur while predicting anomaly
                failure.set(new EndRunException(configID, CommonMessages.BUG_RESPONSE, causeException, false));
            }
        } else if (causeException instanceof OpenSearchTimeoutException) {
            // we can have OpenSearchTimeoutException when a node tries to load RCF or
            // threshold model
            failure.set(new InternalFailure(configID, causeException));
        } else if (causeException instanceof IllegalArgumentException) {
            // we can have IllegalArgumentException when a model is corrupted
            failure.set(new InternalFailure(configID, causeException));
        } else {
            // some unexpected bug occurred or cluster is unstable (e.g., ClusterBlockException) or index is red (e.g.
            // NoShardAvailableActionException) while predicting anomaly
            failure.set(new EndRunException(configID, CommonMessages.BUG_RESPONSE, causeException, false));
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
    protected void handlePredictionFailure(Exception e, String adID, String nodeID, AtomicReference<Exception> failure) {
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
            hashRing.buildCirclesForRealtime();
            return;
        }
        // rebuilding is not done or node is unresponsive
        nodeStateManager.addPressure(node, detectorId);
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
            listener.onFailure(new InternalFailure(adID, ResultProcessor.READ_WRITE_BLOCKED));
            return false;
        }

        if (nodeStateManager.isMuted(rcfNodeId, adID)) {
            listener
                .onFailure(
                    new InternalFailure(
                        adID,
                        String
                            .format(Locale.ROOT, ResultProcessor.NODE_UNRESPONSIVE_ERR_MSG + " %s for rcf model %s", rcfNodeId, rcfModelID)
                    )
                );
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, detector.getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(adID, ResultProcessor.INDEX_READ_BLOCKED));
            return false;
        }

        return true;
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

    public class ErrorResponseListener implements ActionListener<AcknowledgedResponse> {
        private String nodeId;
        private final String configId;
        private AtomicReference<Exception> failure;

        public ErrorResponseListener(String nodeId, String configId, AtomicReference<Exception> failure) {
            this.nodeId = nodeId;
            this.configId = configId;
            this.failure = failure;
        }

        @Override
        public void onResponse(AcknowledgedResponse response) {
            try {
                if (response.isAcknowledged() == false) {
                    LOG.error("Cannot send entities' features to {} for {}", nodeId, configId);
                    nodeStateManager.addPressure(nodeId, configId);
                } else {
                    nodeStateManager.resetBackpressureCounter(nodeId, configId);
                }
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, configId);
                handleException(ex);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                // e.g., we have connection issues with all of the nodes while restarting clusters
                LOG.error(new ParameterizedMessage("Cannot send entities' features to {} for {}", nodeId, configId), e);

                handleException(e);

            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, configId);
                handleException(ex);
            }
        }

        private void handleException(Exception e) {
            handlePredictionFailure(e, configId, nodeId, failure);
            if (failure.get() != null) {
                nodeStateManager.setException(configId, failure.get());
            }
        }
    }
}
