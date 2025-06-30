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

import org.opensearch.ad.transport.ADBatchAnomalyResultRequest;
import org.opensearch.ad.transport.ADBatchAnomalyResultResponse;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import org.opensearch.ad.transport.ADEntityProfileAction;
import org.opensearch.ad.transport.ADHCImputeAction;
import org.opensearch.ad.transport.ADHCImputeNodesResponse;
import org.opensearch.ad.transport.ADHCImputeRequest;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.ADSingleStreamResultAction;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.ad.transport.DeleteADModelAction;
import org.opensearch.ad.transport.EntityADResultAction;
import org.opensearch.ad.transport.ForwardADTaskAction;
import org.opensearch.ad.transport.ForwardADTaskRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
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

    @Override
    @SuppressForbidden(reason = "TransportService#sendRequest usage: only in single-tenant.")
    public void forwardADTask(
        DiscoveryNode node,
        ForwardADTaskRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<JobResponse> responseHandler,
        TransportService transportService
    ) {
        if (request == null) {
            responseHandler.handleException(new TransportException("Forward AD task request is missing"));
            return;
        }
        if (node == null) {
            responseHandler.handleException(new TransportException("Forward AD task node is missing"));
            return;
        }
        try {
            transportService.sendRequest(node, ForwardADTaskAction.NAME, request, options, responseHandler);
        } catch (TransportException e) {
            responseHandler.handleException(e);
        }
    }

    @Override
    @SuppressForbidden(reason = "TransportService#sendRequest usage: only in single-tenant.")
    public void executeBatchTask(
        DiscoveryNode node,
        ADBatchAnomalyResultRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<ADBatchAnomalyResultResponse> responseHandler,
        TransportService transportService
    ) {
        if (request == null) {
            responseHandler.handleException(new TransportException("Batch task execution request is missing"));
            return;
        }
        if (node == null) {
            responseHandler.handleException(new TransportException("Batch task execution node is missing"));
            return;
        }
        try {
            transportService.sendRequest(node, ADBatchTaskRemoteExecutionAction.NAME, request, options, responseHandler);
        } catch (TransportException e) {
            responseHandler.handleException(e);
        }
    }
}
