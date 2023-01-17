/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_BACKOFF_MINUTES;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.ClientUtil;

public class ForecastNodeStateManager extends NodeStateManager<NodeState> {
    private static final Logger LOG = LogManager.getLogger(ForecastNodeStateManager.class);

    /**
     * Constructor
     *
     * @param client Client to make calls to OpenSearch
     * @param xContentRegistry OS named content registry
     * @param settings OS settings
     * @param clientUtil AD Client utility
     * @param clock A UTC clock
     * @param stateTtl Max time to keep state in memory
     * @param clusterService Cluster service accessor
     */
    public ForecastNodeStateManager(
        Client client,
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        ClientUtil clientUtil,
        Clock clock,
        Duration stateTtl,
        ClusterService clusterService
    ) {
        super(
            client,
            xContentRegistry,
            settings,
            clientUtil,
            clock,
            stateTtl,
            clusterService,
            FORECAST_MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            FORECAST_BACKOFF_MINUTES
        );
    }

    @Override
    protected BiCheckedFunction<XContentParser, ? extends Config, String, IOException> getConfigParser() {
        return Forecaster::parse;
    }

    @Override
    protected NodeState createNodeState(String configId, Clock clock) {
        return new NodeState(configId, clock);
    }
}
