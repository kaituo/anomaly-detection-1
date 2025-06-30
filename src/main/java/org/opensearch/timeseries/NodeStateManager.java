/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.cluster.service.ClusterService;
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
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.BackPressureRouting;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.client.Client;

@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public class NodeStateManager implements StateManager {
    private static final Logger LOG = LogManager.getLogger(NodeStateManager.class);

    public static final String NO_ERROR = "no_error";

    protected ConcurrentHashMap<String, NodeState> states;
    protected Client client;
    protected NamedXContentRegistry xContentRegistry;
    protected ClientUtil clientUtil;
    protected final Clock clock;
    protected final Duration stateTtl;
    // map from detector id to the map of ES node id to the node's backpressureMuter
    private Map<String, Map<String, BackPressureRouting>> backpressureMuter;
    private int maxRetryForUnresponsiveNode;
    private TimeValue mutePeriod;

    /**
     * Constructor
     *
     * @param client Client to make calls to OpenSearch
     * @param xContentRegistry ES named content registry
     * @param settings ES settings
     * @param clientUtil AD Client utility
     * @param clock A UTC clock
     * @param stateTtl Max time to keep state in memory
     * @param clusterService Cluster service accessor
     * @param maxRetryForUnresponsiveNodeSetting max retry number for unresponsive node
     * @param backoffMinutesSetting back off minutes setting
     * @param eventBridgeHandler EventBridge handler for coordinator nodes
     */
    public NodeStateManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        ClientUtil clientUtil,
        Clock clock,
        Duration stateTtl,
        ClusterService clusterService,
        Setting<Integer> maxRetryForUnresponsiveNodeSetting,
        Setting<TimeValue> backoffMinutesSetting,
        org.opensearch.timeseries.rest.handler.EventBridgeHandler eventBridgeHandler
    ) {
        this.states = new ConcurrentHashMap<>();
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.clientUtil = clientUtil;
        this.clock = clock;
        this.stateTtl = stateTtl;
        this.backpressureMuter = new ConcurrentHashMap<>();

        this.maxRetryForUnresponsiveNode = maxRetryForUnresponsiveNodeSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(maxRetryForUnresponsiveNodeSetting, it -> {
            this.maxRetryForUnresponsiveNode = it;
            Iterator<Map<String, BackPressureRouting>> iter = backpressureMuter.values().iterator();
            while (iter.hasNext()) {
                Map<String, BackPressureRouting> entry = iter.next();
                entry.values().forEach(v -> v.setMaxRetryForUnresponsiveNode(it));
            }
        });
        this.mutePeriod = backoffMinutesSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(backoffMinutesSetting, it -> {
            this.mutePeriod = it;
            Iterator<Map<String, BackPressureRouting>> iter = backpressureMuter.values().iterator();
            while (iter.hasNext()) {
                Map<String, BackPressureRouting> entry = iter.next();
                entry.values().forEach(v -> v.setMutePeriod(it));
            }
        });

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
     * [Important!] Make sure listener returns in function.
     *
     * Compared with the overload below, this variant is for plain retrieval paths such as GET config APIs:
     * it always reads from storage, returns the persisted config even when all features are disabled,
     * preserves the document version during parsing, and reports parse/get failures via {@code listener.onFailure(...)}.
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
        GetRequest getRequest = new GetRequest(
            analysisType == AnalysisType.AD ? ADCommonName.CONFIG_INDEX : ForecastCommonName.CONFIG_INDEX,
            configId
        );
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                function.accept(Optional.empty());
                return;
            }
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Config config = null;
                if (analysisType.isAD()) {
                    config = AnomalyDetector.parse(parser, response.getId(), response.getVersion());
                } else if (analysisType.isForecast()) {
                    config = Forecaster.parse(parser, response.getId(), response.getVersion());
                } else {
                    throw new UnsupportedOperationException("This method is not supported");
                }

                function.accept(Optional.of(config));
            } catch (Exception e) {
                String message = "Failed to parse config " + configId;
                LOG.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> {
            if (ExceptionUtil.isIndexNotFoundInMessage(exception)) {
                function.accept(Optional.empty());
                return;
            }
            LOG.error("Failed to get config " + configId, exception);
            listener.onFailure(exception);
        }));
    }

    /**
     * Get config with optional caching. If cache is true and the config is already cached, return the cached config.
     * Otherwise, fetch the config from OpenSearch and cache it.
     *
     * Compared with the consumer overload above, this variant is used by execution-oriented paths.
     * {@code cache} only controls whether the in-memory NodeState cache is read/written; setting
     * {@code cache=false} does not make the two overloads equivalent. Even with {@code cache=false},
     * this path still applies execution-time feature validation and may fail when all features are disabled.
     * This path also parses by id only and maps parse failures to {@code Optional.empty()}.
     *
     * @param configID config id
     * @param tenantId tenant id
     * @param context analysis type
     * @param cache whether to cache the config
     * @param listener action listener
     */
    public void getConfig(
        String configID,
        String tenantId,
        AnalysisType context,
        boolean cache,
        ActionListener<Optional<? extends Config>> listener
    ) {
        NodeState state = states.get(configID);
        if (cache && state != null && state.getConfigDef() != null) {
            listener.onResponse(Optional.of(state.getConfigDef()));
        } else {
            GetRequest request = new GetRequest(
                context == AnalysisType.AD ? ADCommonName.CONFIG_INDEX : ForecastCommonName.CONFIG_INDEX,
                configID
            );
            BiCheckedFunction<XContentParser, String, ? extends Config, IOException> configParser = context.isAD()
                ? AnomalyDetector::parse
                : Forecaster::parse;
            clientUtil
                .<GetRequest, GetResponse>asyncRequest(request, client::get, onGetConfigResponse(configID, configParser, cache, listener));
        }
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
            LOG.debug("Fetched config: {}", xc);

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
        }, exception -> {
            if (ExceptionUtil.isIndexNotFoundInMessage(exception)) {
                listener.onResponse(Optional.empty());
                return;
            }
            listener.onFailure(exception);
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

    /**
     * Get job with optional caching. If cache is true and the job is already cached, return the cached job.
     * Otherwise, fetch the job from OpenSearch and cache it.
     *
     * @param configID config id
     * @param tenantId tenant id
     * @param cache whether to cache the job
     * @param listener action listener
     */
    @Override
    public void getJob(String configID, String tenantId, boolean cache, ActionListener<Optional<Job>> listener) {
        NodeState state = states.get(configID);
        if (cache && state != null && state.getJob() != null) {
            listener.onResponse(Optional.of(state.getJob()));
            return;
        }

        // Directly fetch from OpenSearch index
        GetRequest request = new GetRequest(CommonName.JOB_INDEX, configID);
        clientUtil.<GetRequest, GetResponse>asyncRequest(request, client::get, onGetJobResponse(configID, cache, listener));
    }

    private ActionListener<GetResponse> onGetJobResponse(String configID, boolean cache, ActionListener<Optional<Job>> listener) {
        return ActionListener.wrap(response -> {
            if (response == null || !response.isExists()) {
                listener.onResponse(Optional.empty());
                return;
            }

            String xc = response.getSourceAsString();
            LOG.debug("Fetched config: {}", xc);

            try (
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Job job = Job.parse(parser);
                if (cache) {
                    NodeState state = states.computeIfAbsent(configID, id -> new NodeState(id, clock));
                    state.setJob(job);
                }

                listener.onResponse(Optional.of(job));
            } catch (Exception t) {
                LOG.error(new ParameterizedMessage("Fail to parse job {}", configID), t);
                listener.onResponse(Optional.empty());
            }
        }, listener::onFailure);
    }
}
