/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.ConfigDocumentStore;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.transport.BackPressureRouting;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SdkClientProvider;
import org.opensearch.timeseries.util.StringUtil;
import org.opensearch.transport.client.Client;

/**
 * Resource-based state manager that uses EventBridge for job scheduling
 * in multi-tenant environments.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Client required by remote SDK library.")
public class SdkStateManager implements StateManager {
    private static final Logger LOG = LogManager.getLogger(SdkStateManager.class);

    public static final String NO_ERROR = "no_error";

    protected ConcurrentHashMap<String, NodeState> states;
    protected NamedXContentRegistry xContentRegistry;
    protected ConfigDocumentStore configDocumentStore;
    protected final Clock clock;
    protected final Duration stateTtl;
    private final ConcurrentHashMap<String, Instant> configStateClearTimes;
    // map from detector id to the map of ES node id to the node's backpressureMuter
    private Map<String, Map<String, BackPressureRouting>> backpressureMuter;
    private int maxRetryForUnresponsiveNode;
    private TimeValue mutePeriod;
    private org.opensearch.timeseries.rest.handler.EventBridgeHandler eventBridgeHandler;

    public SdkStateManager(
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        ConfigDocumentStore configDocumentStore,
        Clock clock,
        Duration stateTtl,
        Setting<Integer> maxRetryForUnresponsiveNodeSetting,
        Setting<TimeValue> backoffMinutesSetting,
        org.opensearch.timeseries.rest.handler.EventBridgeHandler eventBridgeHandler
    ) {
        this.states = new ConcurrentHashMap<>();
        this.xContentRegistry = xContentRegistry;
        this.configDocumentStore = configDocumentStore;
        this.clock = clock;
        this.stateTtl = stateTtl;
        this.configStateClearTimes = new ConcurrentHashMap<>();
        this.backpressureMuter = new ConcurrentHashMap<>();
        this.eventBridgeHandler = eventBridgeHandler;

        this.maxRetryForUnresponsiveNode = maxRetryForUnresponsiveNodeSetting.get(settings);
        this.mutePeriod = backoffMinutesSetting.get(settings);
    }

    protected static SdkClient initializeSdkClient(Client client, NamedXContentRegistry xContentRegistry, Settings settings) {
        boolean multiTenancyEnabled = AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings)
            || ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.get(settings);
        try {
            return SdkClientProvider
                .buildSdkClient(
                    client,
                    xContentRegistry,
                    ADCommonName.AD_THREAD_POOL_NAME,
                    multiTenancyEnabled,
                    settings,
                    AnomalyDetectorSettings.REMOTE_METADATA_ENDPOINT,
                    AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME,
                    LOG
                );
        } catch (Exception e) {
            LOG.warn("Failed to initialize remote metadata SDK client.", e);
            throw e;
        }
    }

    /**
     * Clean states if it is older than our stateTtl. transportState has to be a
     * ConcurrentHashMap otherwise we will have
     * java.util.ConcurrentModificationException.
     *
     */
    @Override
    public void maintenance() {
        maintenance(states, stateTtl);
        if (false == configStateClearTimes.isEmpty()) {
            Instant now = clock.instant();
            configStateClearTimes.entrySet().removeIf(entry -> entry.getValue().plus(stateTtl).isBefore(now));
        }
    }

    /**
     * Used in delete workflow
     *
     * @param configId config ID
     */
    // TODO: use composite key for multiple tenants
    @Override
    public void clear(String tenantId, String configId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter.get(configId);
        if (routingMap != null) {
            routingMap.clear();
            backpressureMuter.remove(configId);
        }
        states.remove(configId);
    }

    @Override
    public void markConfigStateCleared(String tenantId, String configId) {
        if (Strings.isEmpty(configId)) {
            return;
        }
        configStateClearTimes.put(StringUtil.getCompositeKey(tenantId, configId), clock.instant());
    }

    @Override
    public boolean isConfigStateClearedAfter(String tenantId, String configId, long requestEpochMillis) {
        if (Strings.isEmpty(configId) || requestEpochMillis <= 0) {
            return false;
        }
        Instant clearedAt = configStateClearTimes.get(StringUtil.getCompositeKey(tenantId, configId));
        return clearedAt != null && !Instant.ofEpochMilli(requestEpochMillis).isAfter(clearedAt);
    }

    public boolean isMuted(String nodeId, String configId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter.get(configId);
        if (routingMap == null || routingMap.isEmpty()) {
            return false;
        }
        BackPressureRouting routing = routingMap.get(nodeId);
        return routing != null && routing.isMuted();
    }

    /**
     * When we have a unsuccessful call with a node, increment the backpressure counter.
     * @param nodeId an ES node's ID
     * @param configId config ID
     */
    public void addPressure(String nodeId, String configId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter
            .computeIfAbsent(configId, k -> new HashMap<String, BackPressureRouting>());
        routingMap.computeIfAbsent(nodeId, k -> new BackPressureRouting(k, clock, maxRetryForUnresponsiveNode, mutePeriod)).addPressure();
    }

    /**
     * When we have a successful call with a node, clear the backpressure counter.
     * @param nodeId an ES node's ID
     * @param configId config ID
     */
    public void resetBackpressureCounter(String nodeId, String configId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter.get(configId);
        if (routingMap == null || routingMap.isEmpty()) {
            backpressureMuter.remove(configId);
            return;
        }
        routingMap.remove(nodeId);
    }

    /**
     * Get config and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param configId config id
     * @param analysisType analysis type
     * @param function consumer function.
     * @param listener action listener. Only meant to return failure.
     * @param <T> action listener response type
     */
    public <T> void getConfig(
        String configId,
        String tenantId,
        AnalysisType analysisType,
        Consumer<Optional<? extends Config>> function,
        ActionListener<T> listener
    ) {
        fetchConfig(configId, analysisType, tenantId, ActionListener.wrap(response -> {
            if (response == null || !response.isExists()) {
                function.accept(Optional.empty());
                return;
            }
            try {
                Config config = parseConfigResponse(response, analysisType);
                function.accept(Optional.of(config));
            } catch (Exception e) {
                String message = "Failed to parse config " + configId;
                LOG.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e));
            }
        }, exception -> {
            LOG.error("Failed to get config " + configId, exception);
            listener.onFailure(exception);
        }));
    }

    @Override
    public void getConfig(
        String configID,
        String tenantId,
        AnalysisType context,
        boolean cache,
        ActionListener<Optional<? extends Config>> listener
    ) {
        NodeState state = states.get(configID);
        if (state != null && state.getConfigDef() != null) {
            listener.onResponse(Optional.of(state.getConfigDef()));
        } else {
            BiCheckedFunction<XContentParser, String, ? extends Config, IOException> configParser = context.isAD()
                ? AnomalyDetector::parse
                : Forecaster::parse;
            fetchConfig(configID, context, tenantId, onGetConfigResponse(configID, configParser, cache, listener));
        }
    }

    private Config parseConfigResponse(GetResponse response, AnalysisType analysisType) throws IOException {
        try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            if (analysisType.isAD()) {
                return AnomalyDetector.parse(parser, response.getId(), response.getVersion());
            } else if (analysisType.isForecast()) {
                return Forecaster.parse(parser, response.getId(), response.getVersion());
            }
            throw new UnsupportedOperationException("This method is not supported");
        }
    }

    private void fetchConfig(String configId, AnalysisType analysisType, String tenantId, ActionListener<GetResponse> listener) {
        GetRequest request = new GetRequest(resolveConfigIndexName(analysisType), configId);
        configDocumentStore.get(request, toTenantContext(tenantId), listener);
    }

    private String resolveConfigIndexName(AnalysisType analysisType) {
        return analysisType == AnalysisType.AD ? ADCommonName.CONFIG_INDEX : ForecastCommonName.CONFIG_INDEX;
    }

    private TenantContext toTenantContext(String tenantId) {
        return tenantId == null ? TenantContext.systemWide() : TenantContext.user(tenantId);
    }

    private ActionListener<GetResponse> onGetConfigResponse(
        String configID,
        BiCheckedFunction<XContentParser, String, ? extends Config, IOException> configParser,
        boolean cache,
        ActionListener<Optional<? extends Config>> listener
    ) {
        return ActionListener.wrap(response -> {
            if (response == null || !response.isExists()) {
                listener.onResponse(Optional.empty());
                return;
            }

            String xc = response.getSourceAsString();
            LOG.info("Fetched config: {}", xc);

            try (
                XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, xc)
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Config config = configParser.apply(parser, response.getId());
                // end execution if all features are disabled
                if (config.getEnabledFeatureIds().isEmpty()) {
                    listener
                        .onFailure(new EndRunException(configID, CommonMessages.ALL_FEATURES_DISABLED_ERR_MSG, true).countedInStats(false));
                    return;
                }

                if (cache) {
                    NodeState state = states.computeIfAbsent(configID, configId -> new NodeState(configId, clock));
                    state.setConfigDef(config);
                }

                listener.onResponse(Optional.of(config));
            } catch (Exception t) {
                LOG.error("Fail to parse config {}", configID);
                LOG.error("Stack trace:", t);
                listener.onResponse(Optional.empty());
            }
        }, e -> {
            if (e instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(e)) {
                LOG.error("Index not found for config {}", configID);
                listener.onResponse(Optional.empty());
                return;
            }
            LOG.error("Failed to get config {}", configID, e);
            listener.onFailure(e);
        });
    }

    /**
     * Get the exception of an analysis.  The method has side effect.
     * We reset error after calling the method because
     * 1) We record the exception of an analysis in each interval.
     *  There is no need to record it twice.
     * 2) EndRunExceptions can stop job running. We only want to send the same
     *  signal once for each exception.
     * @param configID config id
     * @return the config's exception
     */
    @Override
    public Optional<Exception> fetchExceptionAndClear(String configID) {
        NodeState state = states.get(configID);
        if (state == null) {
            return Optional.empty();
        }

        Optional<Exception> exception = state.getException();
        exception.ifPresent(e -> state.setException(null));
        return exception;
    }

    /**
     * For single-stream analysis, we have one exception per interval.  When
     * an interval starts, it fetches and clears the exception.
     * For HC analysis, there can be one exception per entity.  To not bloat memory
     * with exceptions, we will keep only one exception. An exception has 3 purposes:
     * 1) stop analysis if nothing else works;
     * 2) increment error stats to ticket about high-error domain
     * 3) debugging.
     *
     * For HC analysis, we record all entities' exceptions in result index. So 3)
     * is covered.  As long as we keep one exception among all exceptions, 2)
     * is covered.  So the only thing we have to pay attention is to keep EndRunException.
     * When overriding an exception, EndRunException has priority.
     * @param configId Detector Id
     * @param e Exception to set
     */
    @Override
    public void setException(String configId, Exception e) {
        if (e == null || Strings.isEmpty(configId)) {
            return;
        }
        NodeState state = states.computeIfAbsent(configId, d -> new NodeState(configId, clock));
        Optional<Exception> exception = state.getException();
        if (exception.isPresent()) {
            Exception higherPriorityException = ExceptionUtil.selectHigherPriorityException(e, exception.get());
            if (higherPriorityException != e) {
                return;
            }
        }

        state.setException(e);
    }

    /**
     * Whether last cold start for the detector is running
     * @param adID detector ID
     * @return running or not
     */
    public boolean isColdStartRunning(String adID) {
        NodeState state = states.get(adID);
        if (state != null) {
            return state.isColdStartRunning();
        }

        return false;
    }

    /**
     * Mark the cold start status of the detector
     * @param adID detector ID
     * @return a callback when cold start is done
     */
    public Releasable markColdStartRunning(String adID) {
        NodeState state = states.computeIfAbsent(adID, id -> new NodeState(id, clock));
        state.setColdStartRunning(true);
        return () -> {
            NodeState nodeState = states.get(adID);
            if (nodeState != null) {
                nodeState.setColdStartRunning(false);
            }
        };
    }

    @Override
    public void getJob(String configID, String tenantId, boolean cache, ActionListener<Optional<Job>> listener) {
        NodeState state = states.get(configID);
        if (cache && state != null && state.getJob() != null) {
            listener.onResponse(Optional.of(state.getJob()));
            return;
        }

        if (eventBridgeHandler == null) {
            listener.onResponse(Optional.empty());
            return;
        }

        // Try to get tenantId from cached config
        try {
            String eventBridgeAccountId = state != null && state.getConfigDef() != null
                ? state.getConfigDef().getEventBridgeCellId()
                : null;
            Optional<Job> jobFromSchedule = EventBridgeHandler.isMaintenanceScheduleName(configID)
                ? eventBridgeHandler.getMaintenanceJobFromSchedule(configID)
                : eventBridgeHandler.getJobFromSchedule(tenantId, configID, eventBridgeAccountId);
            if (jobFromSchedule.isPresent()) {
                // Cache the job
                NodeState nodeState = states.computeIfAbsent(configID, id -> new NodeState(id, clock));
                if (cache) {
                    nodeState.setJob(jobFromSchedule.get());
                }
                listener.onResponse(Optional.of(jobFromSchedule.get()));
                return;
            }

        } catch (Exception e) {
            LOG.error("Failed to get job from schedule", e);
            listener.onFailure(e);
            return;
        }
        listener.onResponse(Optional.empty());
    }
}
