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

package org.opensearch.ad.client;

import org.opensearch.ad.transport.ADEntityProfileAction;
import org.opensearch.ad.transport.ADHCImputeAction;
import org.opensearch.ad.transport.ADHCImputeNodesResponse;
import org.opensearch.ad.transport.ADHCImputeRequest;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.ADSingleStreamResultAction;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.ad.transport.DeleteADModelAction;
import org.opensearch.ad.transport.EntityADResultAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.Client;

/**
 * AD-specific transport-based implementation of NodeCommunicator.
 * Uses the AD profile transport action for profile requests.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public class ADTransportNodeCommunicator extends TransportNodeCommunicator implements ADNodeCommunicator {

    public ADTransportNodeCommunicator(Client client, DiscoveryNodeSelector nodeFilter) {
        super(
            client,
            nodeFilter,
            ADProfileAction.INSTANCE,
            ADEntityProfileAction.INSTANCE,
            EntityADResultAction.NAME,
            ADSingleStreamResultAction.NAME,
            DeleteADModelAction.INSTANCE,
            ADStatsNodesAction.INSTANCE
        );
    }

    @Override
    public void imputeHC(ADHCImputeRequest request, ActionListener<ADHCImputeNodesResponse> listener) {
        client.execute(ADHCImputeAction.INSTANCE, request, listener);
    }
}
