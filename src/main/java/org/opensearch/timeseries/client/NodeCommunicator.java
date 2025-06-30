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

package org.opensearch.timeseries.client;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.transport.DeleteModelRequest;
import org.opensearch.timeseries.transport.DeleteModelResponse;
import org.opensearch.timeseries.transport.EntityProfileRequest;
import org.opensearch.timeseries.transport.EntityProfileResponse;
import org.opensearch.timeseries.transport.EntityResultRequest;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.transport.SingleStreamResultRequest;
import org.opensearch.timeseries.transport.StatsNodesResponse;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Interface for node communication, abstracting transport and HTTP implementations.
 */
public interface NodeCommunicator {
    void profile(ProfileRequest request, ActionListener<ProfileResponse> listener);

    void entityProfile(EntityProfileRequest request, ActionListener<EntityProfileResponse> listener);

    /**
     * Sends an entity result request to a specific node.
     *
     * Even though the HTTP-based implementation doesn’t use it, we still need to pass in a TransportService
     * because the transport-based implementation depends on it. Outside of a transport action, there’s no
     * supported way to bind TransportService, so we can’t bind it in TimeSeriesAnalyticsPlugin and then
     * inject it into the constructors of NodeCommunicator implementations.
     * 
     * @param node The node to send the request to.
     * @param request The entity result request to send.
     * @param options The transport request options.
     * @param responseHandler The response handler for the request.
     * @param transportService The transport service to use.
     */
    void entityResult(
        DiscoveryNode node,
        EntityResultRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<AcknowledgedResponse> responseHandler,
        TransportService transportService
    );

    /**
     * Sends a single stream result request to a specific node.
     *
     * Even though the HTTP-based implementation doesn’t use it, we still need to pass in a TransportService
     * because the transport-based implementation depends on it. Outside of a transport action, there’s no
     * supported way to bind TransportService, so we can’t bind it in TimeSeriesAnalyticsPlugin and then
     * inject it into the constructors of NodeCommunicator implementations.
     * 
     * @param node The node to send the request to.
     * @param request The single stream result request to send.
     * @param options The transport request options.
     * @param responseHandler The response handler for the request.
     * @param transportService The transport service to use.
     */
    void singleStreamResult(
        DiscoveryNode node,
        SingleStreamResultRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<AcknowledgedResponse> responseHandler,
        TransportService transportService
    );

    void deleteModel(DeleteModelRequest request, ActionListener<DeleteModelResponse> listener);

    void stat(StatsRequest request, ActionListener<StatsNodesResponse> listener);
}
