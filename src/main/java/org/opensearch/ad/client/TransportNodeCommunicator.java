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

import java.util.Objects;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.NodeCommunicator;
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
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Abstract transport-based implementation of NodeCommunicator.
 * Subclasses provide the concrete ActionType for profile requests.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public abstract class TransportNodeCommunicator implements NodeCommunicator {
    protected final Client client;
    protected final DiscoveryNodeSelector nodeFilter;
    protected final ActionType<ProfileResponse> profileAction;
    protected final ActionType<EntityProfileResponse> entityProfileAction;
    protected final String entityResultAction;
    protected final String singleStreamResultAction;
    protected final ActionType<DeleteModelResponse> deleteModelAction;
    protected final ActionType<StatsNodesResponse> statsAction;

    public TransportNodeCommunicator(
        Client client,
        DiscoveryNodeSelector nodeFilter,
        ActionType<ProfileResponse> profileAction,
        ActionType<EntityProfileResponse> entityProfileAction,
        String entityResultAction,
        String singleStreamResultAction,
        ActionType<DeleteModelResponse> deleteModelAction,
        ActionType<StatsNodesResponse> statsAction
    ) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.nodeFilter = Objects.requireNonNull(nodeFilter, "nodeFilter must not be null");
        this.profileAction = Objects.requireNonNull(profileAction, "profileAction must not be null");
        this.entityProfileAction = Objects.requireNonNull(entityProfileAction, "entityProfileAction must not be null");
        this.entityResultAction = Objects.requireNonNull(entityResultAction, "entityResultAction must not be null");
        this.singleStreamResultAction = Objects.requireNonNull(singleStreamResultAction, "singleStreamResultAction must not be null");
        this.deleteModelAction = Objects.requireNonNull(deleteModelAction, "deleteModelAction must not be null");
        this.statsAction = Objects.requireNonNull(statsAction, "statsAction must not be null");
    }

    @Override
    public void profile(ProfileRequest request, ActionListener<ProfileResponse> listener) {
        client.execute(profileAction, request, listener);
    }

    @Override
    public void entityProfile(EntityProfileRequest request, ActionListener<EntityProfileResponse> listener) {
        client.execute(entityProfileAction, request, listener);
    }

    @Override
    @SuppressForbidden(reason = "TransportService#sendRequest usage: only in single-tenant.")
    public void entityResult(
        DiscoveryNode node,
        EntityResultRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<AcknowledgedResponse> responseHandler,
        TransportService transportService
    ) {
        if (request == null) {
            responseHandler.handleException(new TransportException("Entity result request is missing"));
            return;
        }
        if (node == null) {
            responseHandler.handleException(new TransportException("Entity result node is missing"));
            return;
        }
        try {
            transportService.sendRequest(node, entityResultAction, request, options, responseHandler);
        } catch (TransportException e) {
            responseHandler.handleException(e);
        }
    }

    @Override
    @SuppressForbidden(reason = "TransportService#sendRequest usage: only in single-tenant.")
    public void singleStreamResult(
        DiscoveryNode node,
        SingleStreamResultRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<AcknowledgedResponse> responseHandler,
        TransportService transportService
    ) {
        if (request == null) {
            responseHandler.handleException(new TransportException("Single stream result request is missing"));
            return;
        }
        if (node == null) {
            responseHandler.handleException(new TransportException("Single stream result node is missing"));
            return;
        }
        try {
            transportService.sendRequest(node, singleStreamResultAction, request, options, responseHandler);
        } catch (TransportException e) {
            responseHandler.handleException(e);
        }
    }

    @Override
    public void deleteModel(DeleteModelRequest request, ActionListener<DeleteModelResponse> listener) {
        if (request == null) {
            listener.onFailure(new IllegalArgumentException("Delete model request is missing"));
            return;
        }
        try {
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            DeleteModelRequest modelDeleteRequest = new DeleteModelRequest(request.getAdID(), request.getTenantId(), dataNodes);
            client.execute(deleteModelAction, modelDeleteRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void stat(StatsRequest request, ActionListener<StatsNodesResponse> listener) {
        if (request == null) {
            listener.onFailure(new IllegalArgumentException("Stats request is missing"));
            return;
        }
        client.execute(statsAction, request, listener);
    }
}
