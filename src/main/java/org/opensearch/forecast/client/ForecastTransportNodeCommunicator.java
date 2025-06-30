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

package org.opensearch.forecast.client;

import org.opensearch.ad.client.TransportNodeCommunicator;
import org.opensearch.forecast.transport.DeleteForecastModelAction;
import org.opensearch.forecast.transport.EntityForecastResultAction;
import org.opensearch.forecast.transport.ForecastEntityProfileAction;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.forecast.transport.ForecastSingleStreamResultAction;
import org.opensearch.forecast.transport.ForecastStatsNodesAction;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.Client;

/**
 * Forecast-specific transport-based implementation of NodeCommunicator.
 * Uses the Forecast profile transport action for profile requests.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public class ForecastTransportNodeCommunicator extends TransportNodeCommunicator implements ForecastNodeCommunicator {

    public ForecastTransportNodeCommunicator(Client client, DiscoveryNodeSelector nodeFilter) {
        super(
            client,
            nodeFilter,
            ForecastProfileAction.INSTANCE,
            ForecastEntityProfileAction.INSTANCE,
            EntityForecastResultAction.NAME,
            ForecastSingleStreamResultAction.NAME,
            DeleteForecastModelAction.INSTANCE,
            ForecastStatsNodesAction.INSTANCE
        );
    }
}
