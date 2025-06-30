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

package org.opensearch.timeseries.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.cluster.ClusterMembershipReader;
import org.opensearch.timeseries.rest.handler.store.spi.DefaultTenantEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.spi.TenantEndpointResolver;

/**
 * Node filter for SDK-backed multi-tenancy.
 */
public class SDKNodeFilter implements DiscoveryNodeSelector {
    private static final Logger LOG = LogManager.getLogger(SDKNodeFilter.class);

    private final TenantEndpointResolver endpointResolver;

    public SDKNodeFilter() {
        this.endpointResolver = ServiceLoader.load(TenantEndpointResolver.class).findFirst().orElseGet(DefaultTenantEndpointResolver::new);
    }

    @Override
    public DiscoveryNode[] getEligibleDataNodes() {
        DiscoveryNodes cachedNodes = ClusterMembershipReader.getCachedDiscoveryNodes();
        if (cachedNodes == null) {
            return new DiscoveryNode[0];
        }
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (DiscoveryNode node : cachedNodes) {
            nodes.add(node);
        }
        return nodes.toArray(new DiscoveryNode[0]);
    }

    // We only store data nodes in ClusterMembershipReader, so we always return true.
    @Override
    public boolean isEligibleDataNode(DiscoveryNode node) {
        return true;
    }

    @Override
    public boolean isEligibleNode(DiscoveryNode node) {
        return true;
    }

    @Override
    public boolean nodeExists(String nodeId) {
        DiscoveryNodes cachedNodes = ClusterMembershipReader.getCachedDiscoveryNodes();
        if (cachedNodes == null) {
            return false;
        }
        return cachedNodes.nodeExists(nodeId);
    }

    @Override
    public void hasGlobalBlock(ActionListener<Boolean> listener) {
        try {
            // Use a default tenant ID for cluster-wide operations
            String endpoint = endpointResolver.resolve(null);
            RestClient restClient = RestClientProvider.getRestClient(endpoint);

            Request request = new Request("GET", "/_cluster/state/blocks");
            restClient.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        boolean hasBlock = parseGlobalBlockResponse(response);
                        listener.onResponse(hasBlock);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void hasIndicesBlock(ClusterBlockLevel level, String[] indices, ActionListener<Boolean> listener) {
        try {
            // Use a default tenant ID for cluster-wide operations
            String endpoint = endpointResolver.resolve(null);
            RestClient restClient = RestClientProvider.getRestClient(endpoint);

            Request request = new Request("GET", "/_cluster/state/blocks");
            restClient.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        boolean hasBlock = parseIndicesBlockResponse(response, level, indices);
                        listener.onResponse(hasBlock);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean parseGlobalBlockResponse(Response response) throws Exception {
        String responseBody = org.apache.hc.core5.http.io.entity.EntityUtils.toString(response.getEntity());
        Map<String, Object> map = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), responseBody, false);

        // Check for global blocks
        Map<String, Object> blocks = (Map<String, Object>) map.get("blocks");
        if (blocks == null) {
            return false;
        }

        Map<String, Object> global = (Map<String, Object>) blocks.get("global");
        if (global == null || global.isEmpty()) {
            return false;
        }

        // Check if any global block affects read or write
        for (Object blockObj : global.values()) {
            Map<String, Object> block = (Map<String, Object>) blockObj;
            List<String> levels = (List<String>) block.get("levels");
            if (levels != null && (levels.contains("read") || levels.contains("write"))) {
                return true;
            }
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    private boolean parseIndicesBlockResponse(Response response, ClusterBlockLevel level, String[] indices) throws Exception {
        String responseBody = org.apache.hc.core5.http.io.entity.EntityUtils.toString(response.getEntity());
        Map<String, Object> map = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), responseBody, false);

        // Check for index blocks
        Map<String, Object> blocks = (Map<String, Object>) map.get("blocks");
        if (blocks == null) {
            return false;
        }

        Map<String, Object> indicesBlocks = (Map<String, Object>) blocks.get("indices");
        if (indicesBlocks == null || indicesBlocks.isEmpty()) {
            return false;
        }

        String levelStr = level.name().toLowerCase(Locale.ROOT);

        // Check each requested index for blocks
        for (String index : indices) {
            Map<String, Object> indexBlocks = (Map<String, Object>) indicesBlocks.get(index);
            if (indexBlocks != null) {
                for (Object blockObj : indexBlocks.values()) {
                    Map<String, Object> block = (Map<String, Object>) blockObj;
                    List<String> levels = (List<String>) block.get("levels");
                    if (levels != null && levels.contains(levelStr)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
