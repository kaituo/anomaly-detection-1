package org.opensearch.timeseries;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.time.Clock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.timeseries.transport.BackPressureRouting;

public class NodeStateManager implements CleanState {

    // map from detector id to the map of ES node id to the node's backpressureMuter
    private Map<String, Map<String, BackPressureRouting>> backpressureMuter;
    private int maxRetryForUnresponsiveNode;
    private TimeValue mutePeriod;
    private final Clock clock;

    public NodeStateManager(
            Settings settings,
            ClusterService clusterService,
            Clock clock) {
        this.backpressureMuter = new ConcurrentHashMap<>();

        this.maxRetryForUnresponsiveNode = MAX_RETRY_FOR_UNRESPONSIVE_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_RETRY_FOR_UNRESPONSIVE_NODE, it -> {
            this.maxRetryForUnresponsiveNode = it;
            Iterator<Map<String, BackPressureRouting>> iter = backpressureMuter.values().iterator();
            while (iter.hasNext()) {
                Map<String, BackPressureRouting> entry = iter.next();
                entry.values().forEach(v -> v.setMaxRetryForUnresponsiveNode(it));
            }
        });
        this.mutePeriod = BACKOFF_MINUTES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BACKOFF_MINUTES, it -> {
            this.mutePeriod = it;
            Iterator<Map<String, BackPressureRouting>> iter = backpressureMuter.values().iterator();
            while (iter.hasNext()) {
                Map<String, BackPressureRouting> entry = iter.next();
                entry.values().forEach(v -> v.setMutePeriod(it));
            }
        });

        this.clock = clock;
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

    @Override
    public void clear(String detectorId) {
        Map<String, BackPressureRouting> routingMap = backpressureMuter.get(detectorId);
        if (routingMap != null) {
            routingMap.clear();
            backpressureMuter.remove(detectorId);
        }
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
}
