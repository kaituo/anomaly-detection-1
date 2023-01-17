package org.opensearch.timeseries;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.transport.BackPressureRouting;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.ExceptionUtil;

public abstract class NodeStateManager<NodeStateType extends NodeState> implements MaintenanceState, CleanState, ExceptionRecorder {
    private static final Logger LOG = LogManager.getLogger(NodeStateManager.class);

    public static final String NO_ERROR = "no_error";

    protected ConcurrentHashMap<String, NodeStateType> states;
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
        Setting<TimeValue> backoffMinutesSetting
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
     * @param configId detector ID
     */
    @Override
    public void clear(String configId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter.get(configId);
        if (routingMap != null) {
            routingMap.clear();
            backpressureMuter.remove(configId);
        }
        states.remove(configId);
    }

    public boolean isMuted(String nodeId, String detectorId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter.get(detectorId);
        if (routingMap == null || routingMap.isEmpty()) {
            return false;
        }
        BackPressureRouting routing = routingMap.get(nodeId);
        return routing != null && routing.isMuted();
    }

    /**
     * When we have a unsuccessful call with a node, increment the backpressure counter.
     * @param nodeId an ES node's ID
     * @param detectorId Detector ID
     */
    public void addPressure(String nodeId, String detectorId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter
            .computeIfAbsent(detectorId, k -> new HashMap<String, BackPressureRouting>());
        routingMap.computeIfAbsent(nodeId, k -> new BackPressureRouting(k, clock, maxRetryForUnresponsiveNode, mutePeriod)).addPressure();
    }

    /**
     * When we have a successful call with a node, clear the backpressure counter.
     * @param nodeId an ES node's ID
     * @param detectorId Detector ID
     */
    public void resetBackpressureCounter(String nodeId, String detectorId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter.get(detectorId);
        if (routingMap == null || routingMap.isEmpty()) {
            backpressureMuter.remove(detectorId);
            return;
        }
        routingMap.remove(nodeId);
    }

    public void getConfig(String adID, ActionListener<Optional<? extends Config>> listener) {
        NodeState state = states.get(adID);
        if (state != null && state.getConfigDef() != null) {
            listener.onResponse(Optional.of(state.getConfigDef()));
        } else {
            GetRequest request = new GetRequest(CommonName.CONFIG_INDEX, adID);
            clientUtil.<GetRequest, GetResponse>asyncRequest(request, client::get, onGetConfigResponse(adID, getConfigParser(), listener));
        }
    }

    private ActionListener<GetResponse> onGetConfigResponse(
        String configID,
        BiCheckedFunction<XContentParser, ? extends Config, String, IOException> configParser,
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

                NodeState state = states.computeIfAbsent(configID, configId -> createNodeState(configId, clock));
                state.setConfigDef(config);

                listener.onResponse(Optional.of(config));
            } catch (Exception t) {
                LOG.error("Fail to parse config {}", configID);
                LOG.error("Stack trace:", t);
                listener.onResponse(Optional.empty());
            }
        }, listener::onFailure);
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
        NodeState state = states.computeIfAbsent(configId, d -> createNodeState(configId, clock));
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
     * @return the appropriate config parser function.
     */
    protected abstract BiCheckedFunction<XContentParser, ? extends Config, String, IOException> getConfigParser();

    protected abstract NodeStateType createNodeState(String configId, Clock clock);
}
