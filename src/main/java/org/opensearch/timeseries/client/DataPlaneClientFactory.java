/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;
import java.util.function.Function;

import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.common.lease.Releasable;

/**
 * Factory for RestClient instances that reach the data plane for a tenant.
 */
public interface DataPlaneClientFactory {
    RestClient getClient(String tenantId, String dataSourceId);

    /**
     * Resolve the endpoint and client through the same factory path.
     *
     * @param tenantId tenant whose data-plane client is needed
     * @param dataSourceId data source whose data-plane client is needed
     * @return resolved client plus the endpoint used to create it
     */
    default ResolvedClient getResolvedClient(String tenantId, String dataSourceId) {
        return new ResolvedClient(getClient(tenantId, dataSourceId), null);
    }

    /**
     * Capture the data-plane client state needed for one request flow.
     *
     * @param tenantId tenant whose data-plane client is needed
     * @param dataSourceId data source whose data-plane client is needed
     * @return request-scoped client context
     */
    default RequestContext createRequestContext(String tenantId, String dataSourceId) {
        return new RequestContext(tenantId, dataSourceId, getResolvedClient(tenantId, dataSourceId), this::prepareRequest);
    }

    /**
     * Return the current request-scoped context when it matches the tenant; otherwise capture a new one.
     *
     * @param tenantId tenant whose data-plane client is needed
     * @param dataSourceId data source whose data-plane client is needed
     * @return request-scoped client context
     */
    default RequestContext getOrCreateRequestContext(String tenantId, String dataSourceId) {
        RequestContext currentContext = DataPlaneClientFactoryContext.getCurrentRequestContext();
        if (currentContext != null
            && Objects.equals(currentContext.tenantId(), tenantId)
            && Objects.equals(currentContext.dataSourceId(), dataSourceId)) {
            return currentContext;
        }
        return createRequestContext(tenantId, dataSourceId);
    }

    /**
     * Prepare a single outbound REST request before it crosses the async HTTP client boundary.
     *
     * @param request request to prepare
     * @return releasable request-scoped state; callers must close it after the async request completes
     */
    default Releasable prepareRequest(Request request) {
        return () -> {};
    }

    record ResolvedClient(RestClient restClient, String endpoint) {
        public ResolvedClient {
            Objects.requireNonNull(restClient, "restClient must not be null");
        }
    }

    record RequestContext(String tenantId, String dataSourceId, ResolvedClient resolvedClient,
        Function<Request, Releasable> requestPreparer) {
        public RequestContext {
            Objects.requireNonNull(resolvedClient, "resolvedClient must not be null");
            Objects.requireNonNull(requestPreparer, "requestPreparer must not be null");
        }

        public RestClient restClient() {
            return resolvedClient.restClient();
        }

        public String endpoint() {
            return resolvedClient.endpoint();
        }

        public Releasable prepareRequest(Request request) {
            return requestPreparer.apply(request);
        }
    }
}
