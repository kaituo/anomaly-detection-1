package org.opensearch.timeseries;


import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.transport.BackPressureRouting;
import org.opensearch.timeseries.util.ClientUtil;

public abstract class TimeSeriesNodeStateManager<NodeStateType extends ExpiringState> implements MaintenanceState, CleanState, ExceptionRecorder {
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
    public TimeSeriesNodeStateManager(
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
     * @param detectorId detector ID
     */
    @Override
    public void clear(String detectorId) {
        states.remove(detectorId);
        Map<String, BackPressureRouting> routingMap = backpressureMuter.get(detectorId);
        if (routingMap != null) {
            routingMap.clear();
            backpressureMuter.remove(detectorId);
        }
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

    public abstract void getConfig(String id, ActionListener<Optional<? extends Config>> listener);

    public abstract Optional<? extends Config> getConfigIfPresent(String id);
}
