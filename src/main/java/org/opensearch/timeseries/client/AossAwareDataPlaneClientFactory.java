/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.timeseries.client.AwsSigV4ThreadContext.RequestSigningMaterial;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DataPlaneServiceUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;

/**
 * Chooses SigV4 signing for AOSS data-plane endpoints and plain HTTP clients otherwise.
 *
 * This is the default/API-aware data-plane factory. Background SQS job execution installs
 * {@link AossDirectSigningClientFactory} as a ThreadContext override instead.
 */
public class AossAwareDataPlaneClientFactory implements DataPlaneClientFactory {
    private final String region;
    private final DataSourceEndpointResolver endpointResolver;
    private final boolean signAllRequests;
    private final ThreadContext threadContext;
    private final AwsSigV4ThreadContext sigV4ThreadContext;
    private final String signingServiceName;
    private final boolean endpointRegionSigningEnabled;

    public AossAwareDataPlaneClientFactory(
        String region,
        DataSourceEndpointResolver endpointResolver,
        boolean signAllRequests,
        ThreadContext threadContext,
        AwsSigV4ThreadContext sigV4ThreadContext,
        String signingServiceName,
        boolean endpointRegionSigningEnabled
    ) {
        this.region = region;
        this.endpointResolver = Objects.requireNonNull(endpointResolver, "endpointResolver must not be null");
        this.signAllRequests = signAllRequests;
        this.threadContext = threadContext;
        this.signingServiceName = normalize(signingServiceName);
        this.endpointRegionSigningEnabled = endpointRegionSigningEnabled;
        if (threadContext != null) {
            this.sigV4ThreadContext = Objects
                .requireNonNull(sigV4ThreadContext, "sigV4ThreadContext must not be null when threadContext is provided");
        } else {
            this.sigV4ThreadContext = sigV4ThreadContext;
        }
    }

    @Override
    public RestClient getClient(String tenantId, String dataSourceId) {
        return getResolvedClient(tenantId, dataSourceId).restClient();
    }

    @Override
    public ResolvedClient getResolvedClient(String tenantId, String dataSourceId) {
        String endpoint = resolveEndpoint(tenantId, dataSourceId);
        RequestSigningMaterial requestSigningMaterial = resolveRequestSigningMaterial(false);
        return resolveClient(endpoint, requestSigningMaterial);
    }

    @Override
    public RequestContext createRequestContext(String tenantId, String dataSourceId) {
        String endpoint = resolveEndpoint(tenantId, dataSourceId);
        RequestSigningMaterial requestSigningMaterial = resolveRequestSigningMaterial(true);
        return new RequestContext(
            tenantId,
            dataSourceId,
            resolveClient(endpoint, requestSigningMaterial),
            request -> requestSigningMaterial == null
                ? () -> {}
                : AwsSigV4RequestInterceptor.attachRequestSigningMaterial(request, requestSigningMaterial)
        );
    }

    @Override
    public Releasable prepareRequest(Request request) {
        Objects.requireNonNull(request, "request must not be null");
        if (threadContext == null || sigV4ThreadContext == null) {
            return () -> {};
        }
        RequestSigningMaterial requestSigningMaterial = sigV4ThreadContext.resolve(threadContext);
        if (requestSigningMaterial == null) {
            return () -> {};
        }
        return AwsSigV4RequestInterceptor.attachRequestSigningMaterial(request, requestSigningMaterial);
    }

    private ResolvedClient resolveClient(String endpoint, RequestSigningMaterial requestSigningMaterial) {
        if (shouldSign(endpoint, requestSigningMaterial)) {
            return new ResolvedClient(
                SigningRestClientProvider.getRestClient(endpoint, resolveRegion(endpoint), signingServiceName),
                endpoint
            );
        }
        return new ResolvedClient(RestClientProvider.getRestClient(endpoint), endpoint);
    }

    private boolean shouldSign(String endpoint, RequestSigningMaterial requestSigningMaterial) {
        return signAllRequests || requestSigningMaterial != null || DataPlaneServiceUtils.isAossEndpoint(endpoint);
    }

    private RequestSigningMaterial resolveRequestSigningMaterial(boolean logMissingMaterial) {
        if (sigV4ThreadContext == null || threadContext == null) {
            return null;
        }
        return sigV4ThreadContext.resolve(threadContext, logMissingMaterial);
    }

    private String resolveRegion(String endpoint) {
        String resolvedRegion = endpointRegionSigningEnabled
            ? EndpointRegionResolver.resolveRegionFromEndpointOrDefault(endpoint, region)
            : region;
        return requireRegion(resolvedRegion);
    }

    private String requireRegion(String resolvedRegion) {
        if (resolvedRegion == null || resolvedRegion.isBlank()) {
            throw new IllegalStateException(TimeSeriesSettings.REGION.getKey() + " must be configured for AOSS data-plane signing.");
        }
        return resolvedRegion;
    }

    private String resolveEndpoint(String tenantId, String dataSourceId) {
        String applicationId = tenantId == null ? null : TenantAwareHelper.parseTenantId(tenantId).applicationId();
        return endpointResolver.resolve(applicationId, dataSourceId);
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
