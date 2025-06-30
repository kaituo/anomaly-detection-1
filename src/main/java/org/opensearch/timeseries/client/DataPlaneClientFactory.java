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
    /**
     * Distinguishes interactive traffic from batch traffic so they can be served by separate
     * RestClient instances (and therefore separate HTTP connection pools) to the same data-plane
     * endpoint.
     *
     * <p>All data-plane traffic to a given AOSS collection shares one cached RestClient keyed by
     * endpoint|region|service|credentials. Because foreground and background callers resolve the
     * same "default" credential key, they would otherwise collapse onto a single client. That
     * client is configured by {@link RestClientPoolConfig}. In the normal data-plane path one
     * RestClient is built for one endpoint, so one AOSS collection is usually one route. With the
     * current 50-connection per-route cap, only 50 requests can be in flight to that collection from
     * that client at once. Under heavy background load (e.g. ~200 concurrent detector jobs) those
     * slots stay perpetually busy, and interactive foreground requests (suggest / create-detector)
     * block waiting for a free connection until the 5s connection-request timeout fires.
     *
     * <p>Adding the traffic class to the client cache key gives {@link #FOREGROUND} requests their
     * own dedicated client and pool, isolated from {@link #BACKGROUND} batch traffic, so background
     * jobs can never starve interactive requests of connections.
     */
    enum TrafficClass {
        FOREGROUND,
        BACKGROUND
    }

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
