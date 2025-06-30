/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.client;

import org.opensearch.ad.transport.ADBatchAnomalyResultRequest;
import org.opensearch.ad.transport.ADBatchAnomalyResultResponse;
import org.opensearch.ad.transport.ADHCImputeNodesResponse;
import org.opensearch.ad.transport.ADHCImputeRequest;
import org.opensearch.ad.transport.ForwardADTaskRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * DI (dependency injection) Marker interface for AD-specific node communication.
 * Wired via TimeSeriesAnalyticsPlugin.
 */
public interface ADNodeCommunicator extends NodeCommunicator {

    /**
     * Broadcast HC impute request to data nodes for imputing missing values in high cardinality detectors.
     *
     * @param request the HC impute request containing configId, tenantId, taskId, and time range
     * @param listener the listener to notify on completion
     */
    void imputeHC(ADHCImputeRequest request, ActionListener<ADHCImputeNodesResponse> listener);

    /**
     * Sends a historical task forwarding request to a specific lead or coordinating node.
     *
     * @param node the node to send the request to
     * @param request the historical task forwarding request
     * @param options the transport request options for transport mode
     * @param responseHandler response handler for the request
     * @param transportService transport service used by transport mode
     */
    void forwardADTask(
        DiscoveryNode node,
        ForwardADTaskRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<JobResponse> responseHandler,
        TransportService transportService
    );

    /**
     * Sends a historical batch task execution request to a worker node.
     *
     * @param node the worker node to send the request to
     * @param request the batch task execution request
     * @param options the transport request options for transport mode
     * @param responseHandler response handler for the request
     * @param transportService transport service used by transport mode
     */
    void executeBatchTask(
        DiscoveryNode node,
        ADBatchAnomalyResultRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<ADBatchAnomalyResultResponse> responseHandler,
        TransportService transportService
    );
}
