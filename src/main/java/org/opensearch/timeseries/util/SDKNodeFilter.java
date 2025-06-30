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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.ResponseListener;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.timeseries.client.DataPlaneClientFactory;
import org.opensearch.timeseries.client.DataPlaneClientFactoryContext;
import org.opensearch.timeseries.client.UnsignedClientFactory;
import org.opensearch.timeseries.cluster.ClusterMembershipReader;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.endpoint.EndpointResolverFactoryLoader;

/**
 * Node filter for SDK-backed multi-tenancy.
 */
public class SDKNodeFilter implements DiscoveryNodeSelector {
    private static final Logger LOG = LogManager.getLogger(SDKNodeFilter.class);

    private final DataSourceEndpointResolver endpointResolver;
    private final DataPlaneClientFactory dataPlaneClientFactory;
    private final boolean aossDataPlane;

    public SDKNodeFilter() {
        this(Settings.EMPTY);
    }

    public SDKNodeFilter(Settings settings) {
        this.endpointResolver = EndpointResolverFactoryLoader.loadDataSourceEndpointResolver(settings, getClass().getClassLoader());
        this.dataPlaneClientFactory = new UnsignedClientFactory(endpointResolver);
        this.aossDataPlane = DataPlaneServiceUtils.isAossDataPlane(settings);
    }

    public SDKNodeFilter(Settings settings, DataPlaneClientFactory dataPlaneClientFactory) {
        this.endpointResolver = EndpointResolverFactoryLoader.loadDataSourceEndpointResolver(settings, getClass().getClassLoader());
        this.dataPlaneClientFactory = java.util.Objects.requireNonNull(dataPlaneClientFactory, "dataPlaneClientFactory must not be null");
        this.aossDataPlane = DataPlaneServiceUtils.isAossDataPlane(settings);
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
    public void hasGlobalBlock(String tenantId, ActionListener<Boolean> listener) {
        if (aossDataPlane) {
            LOG.debug("Skipping cluster block API for AOSS tenant {}; treating as no global block", tenantId);
            listener.onResponse(false);
            return;
        }
        try {
            Request request = new Request("GET", "/_cluster/state/blocks");
            performRequestAsync(tenantId, request, new ResponseListener() {
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
                    // See isUnsupportedClusterBlocksApi: AOSS Serverless does not expose
                    // /_cluster/* APIs (rejected with 404/403/400+security_exception). The
                    // cluster-block concept doesn't exist on Serverless; any actual
                    // unavailability surfaces on the underlying operation, so treat the
                    // missing API as "no global block" and let the downstream request decide.
                    if (isUnsupportedClusterBlocksApi(e)) {
                        LOG.debug("Cluster block API is unavailable for tenant {}; treating as no global block", tenantId, e);
                        listener.onResponse(false);
                        return;
                    }
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void hasIndicesBlock(String tenantId, ClusterBlockLevel level, String[] indices, ActionListener<Boolean> listener) {
        if (aossDataPlane) {
            LOG.debug("Skipping cluster block API for AOSS tenant {}; treating indices {} as unblocked", tenantId, indices);
            listener.onResponse(false);
            return;
        }
        try {
            Request request = new Request("GET", "/_cluster/state/blocks");
            performRequestAsync(tenantId, request, new ResponseListener() {
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
                    if (isUnsupportedClusterBlocksApi(e)) {
                        LOG.debug("Cluster block API is unavailable for tenant {}; treating indices {} as unblocked", tenantId, indices, e);
                        listener.onResponse(false);
                        return;
                    }
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void performRequestAsync(String tenantId, Request request, ResponseListener listener) {
        DataPlaneClientFactory.RequestContext requestContext = dataPlaneClientFactory.getOrCreateRequestContext(tenantId, null);
        Releasable requestPreparation = requestContext.prepareRequest(request);
        try {
            requestContext.restClient().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        DataPlaneClientFactoryContext.runWithRequestContext(requestContext, () -> listener.onSuccess(response));
                    } finally {
                        releaseRequestContext(requestPreparation);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        DataPlaneClientFactoryContext.runWithRequestContext(requestContext, () -> listener.onFailure(e));
                    } finally {
                        releaseRequestContext(requestPreparation);
                    }
                }
            });
        } catch (RuntimeException e) {
            releaseRequestContext(requestPreparation);
            throw e;
        }
    }

    private void releaseRequestContext(Releasable requestPreparation) {
        try {
            requestPreparation.close();
        } catch (Exception e) {
            LOG.warn("Failed to release REST data-plane request context", e);
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

    /**
     * Returns true if the failure indicates that the data plane will not serve the
     * cluster-blocks API and we should treat the call as "no block" rather than
     * propagating the error.
     *
     * AOSS Serverless does not expose any /_cluster/* route. Depending on the
     * collection's data-access policy and the route's presence in the
     * supported-operations table, the data plane can surface this as:
     *   - 404 Not Found        -- route is not registered.
     *   - 403 Forbidden        -- route is not permitted by the data-access policy
     *                             or supported-operations table (AccessDeniedException).
     *   - 400 Bad Request with a security_exception / "not supported" body --
     *     the security plugin or AOSS gateway short-circuits the request.
     *
     * In all of these cases the cluster-block concept does not apply, so returning
     * "no block" is correct: any actual unavailability will surface as a real error
     * on the underlying read/write operation, which is handled at the call site.
     */
    private boolean isUnsupportedClusterBlocksApi(Exception e) {
        ResponseException responseException = findResponseException(e);
        if (responseException == null) {
            return false;
        }
        int statusCode = responseException.getResponse().getStatusLine().getStatusCode();
        if (statusCode == 404 || statusCode == 403) {
            return true;
        }
        if (statusCode == 400) {
            String message = responseException.getMessage();
            if (message != null) {
                String lower = message.toLowerCase(Locale.ROOT);
                return lower.contains("security_exception")
                    || lower.contains("accessdeniedexception")
                    || lower.contains("unsupported_operation")
                    || lower.contains("not supported");
            }
        }
        return false;
    }

    private static ResponseException findResponseException(Throwable t) {
        while (t != null) {
            if (t instanceof ResponseException) {
                return (ResponseException) t;
            }
            t = t.getCause();
        }
        return null;
    }
}
