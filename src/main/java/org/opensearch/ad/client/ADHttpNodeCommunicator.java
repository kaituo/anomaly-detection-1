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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.action.FailedNodeException;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.rest.RestAnomalyDetectorNodeProfileAction;
import org.opensearch.ad.transport.ADHCImputeNodeResponse;
import org.opensearch.ad.transport.ADHCImputeNodesResponse;
import org.opensearch.ad.transport.ADHCImputeRequest;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.util.RestHandlerUtils;

/**
 * AD-specific HTTP-based implementation of NodeCommunicator.
 * Uses the AD detector REST path for profile requests.
 */
public class ADHttpNodeCommunicator extends HttpNodeCommunicator implements ADNodeCommunicator {

    public ADHttpNodeCommunicator(HashRing hashRing) {
        super(TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, ADCommonName.AD_CLUSTER_NAME, hashRing);
    }

    @Override
    protected String buildProfilePath(String configId, String typeStr) {
        StringBuilder path = new StringBuilder();
        path
            .append(TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI)
            .append("/")
            .append(configId)
            .append("/")
            .append(RestAnomalyDetectorNodeProfileAction.NODE_PROFILE);
        if (typeStr != null && !typeStr.isEmpty()) {
            path.append("/").append(typeStr);
        }
        return path.toString();
    }

    @Override
    public void imputeHC(ADHCImputeRequest request, ActionListener<ADHCImputeNodesResponse> listener) {
        DiscoveryNode[] nodes = nodeFilter.getEligibleDataNodes();
        if (nodes == null || nodes.length == 0) {
            listener.onResponse(new ADHCImputeNodesResponse(new ClusterName(clusterName), Collections.emptyList(), Collections.emptyList()));
            return;
        }

        List<ADHCImputeNodeResponse> responses = Collections.synchronizedList(new ArrayList<>());
        List<FailedNodeException> failures = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger pending = new AtomicInteger(nodes.length);

        for (DiscoveryNode node : nodes) {
            String endpoint = getNodeEndpoint(node);
            if (endpoint == null) {
                failures.add(new FailedNodeException(node == null ? "" : node.getId(), "Missing node endpoint", null));
                if (pending.decrementAndGet() == 0) {
                    listener.onResponse(new ADHCImputeNodesResponse(new ClusterName(clusterName), responses, failures));
                }
                continue;
            }

            RestClient restClient = RestClientProvider.getRestClient(endpoint);
            Request restRequest;
            try {
                restRequest = buildHCImputeRequest(request);
            } catch (IOException e) {
                failures.add(new FailedNodeException(node.getId(), "Failed to build HC impute request", e));
                if (pending.decrementAndGet() == 0) {
                    listener.onResponse(new ADHCImputeNodesResponse(new ClusterName(clusterName), responses, failures));
                }
                continue;
            }

            restClient.performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    // We don't need to parse the full response for HC impute - just record success
                    responses.add(new ADHCImputeNodeResponse(node, null));
                    if (pending.decrementAndGet() == 0) {
                        listener.onResponse(new ADHCImputeNodesResponse(new ClusterName(clusterName), responses, failures));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failures.add(new FailedNodeException(node.getId(), "Failed to call HC impute API", e));
                    if (pending.decrementAndGet() == 0) {
                        listener.onResponse(new ADHCImputeNodesResponse(new ClusterName(clusterName), responses, failures));
                    }
                }
            });
        }
    }

    private Request buildHCImputeRequest(ADHCImputeRequest request) throws IOException {
        String path = String.format(
            Locale.ROOT,
            "%s/%s/%s",
            TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI,
            request.getConfigId(),
            RestHandlerUtils.HC_IMPUTE
        );
        Request restRequest = new Request("POST", path);

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            if (request.getTenantId() != null) {
                builder.field(CommonName.TENANT_ID_FIELD, request.getTenantId());
            }
            if (request.getTaskId() != null) {
                builder.field("task_id", request.getTaskId());
            }
            builder.field("data_start_millis", request.getDataStartMillis());
            builder.field("data_end_millis", request.getDataEndMillis());
            builder.endObject();

            restRequest.setJsonEntity(BytesReference.bytes(builder).utf8ToString());
        }

        if (request.getTenantId() != null) {
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader(CommonName.TENANT_ID_HEADER, request.getTenantId());
            restRequest.setOptions(optionsBuilder);
        }

        return restRequest;
    }

}
