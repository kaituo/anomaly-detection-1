package org.opensearch.timeseries;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.ClientUtil;

public abstract class TimeSeriesNodeStateManager<NodeStateType extends ExpiringState> implements MaintenanceState, CleanState, ExceptionRecorder {
    public static final String NO_ERROR = "no_error";

    protected ConcurrentHashMap<String, NodeStateType> states;
    protected Client client;
    protected NamedXContentRegistry xContentRegistry;
    protected ClientUtil clientUtil;
    protected final Clock clock;
    protected final Duration stateTtl;

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
        ClusterService clusterService
    ) {
        this.states = new ConcurrentHashMap<>();
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.clientUtil = clientUtil;
        this.clock = clock;
        this.stateTtl = stateTtl;
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
    }

    public abstract void getConfig(String id, ActionListener<Optional<? extends Config>> listener);

    public abstract Optional<? extends Config> getConfigIfPresent(String id);
}
