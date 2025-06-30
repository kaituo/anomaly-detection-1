/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.client.RestClient;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DataPlaneServiceUtils;

/**
 * Chooses SigV4 signing for AOSS data-plane endpoints and plain HTTP clients otherwise.
 */
public class AossAwareDataPlaneClientFactory implements DataPlaneClientFactory {
    private final String region;
    private final DataSourceEndpointResolver endpointResolver;
    private final boolean signAllRequests;
    private final ThreadContext threadContext;
    private final AwsSigV4ThreadContext sigV4ThreadContext;

    public AossAwareDataPlaneClientFactory(String region, DataSourceEndpointResolver endpointResolver, boolean signAllRequests) {
        this(region, endpointResolver, signAllRequests, null, null);
    }

    public AossAwareDataPlaneClientFactory(
        String region,
        DataSourceEndpointResolver endpointResolver,
        boolean signAllRequests,
        ThreadContext threadContext,
        AwsSigV4ThreadContext sigV4ThreadContext
    ) {
        this.region = region;
        this.endpointResolver = Objects.requireNonNull(endpointResolver, "endpointResolver must not be null");
        this.signAllRequests = signAllRequests;
        this.threadContext = threadContext;
        if (threadContext != null) {
            this.sigV4ThreadContext = Objects
                .requireNonNull(sigV4ThreadContext, "sigV4ThreadContext must not be null when threadContext is provided");
        } else {
            this.sigV4ThreadContext = sigV4ThreadContext;
        }
    }

    @Override
    public RestClient getClient(String tenantId) {
        String endpoint = endpointResolver.resolve(tenantId);
        if (shouldSign(endpoint)) {
            return SigningRestClientProvider.getRestClient(endpoint, resolveRegion(), threadContext, sigV4ThreadContext);
        }
        return RestClientProvider.getRestClient(endpoint);
    }

    @Override
    public boolean supportsInjectedSecurityHeaders() {
        if (signAllRequests || hasRequestSigningMaterial()) {
            return false;
        }
        try {
            return DataPlaneServiceUtils.isAossEndpoint(endpointResolver.resolve((String) null)) == false;
        } catch (RuntimeException e) {
            return true;
        }
    }

    private boolean shouldSign(String endpoint) {
        return signAllRequests || hasRequestSigningMaterial() || DataPlaneServiceUtils.isAossEndpoint(endpoint);
    }

    private boolean hasRequestSigningMaterial() {
        return sigV4ThreadContext != null && sigV4ThreadContext.hasRequestSigningMaterial(threadContext);
    }

    private String resolveRegion() {
        if (sigV4ThreadContext != null) {
            String requestRegion = sigV4ThreadContext.region(threadContext);
            if (requestRegion != null && requestRegion.isBlank() == false) {
                return requestRegion;
            }
        }
        return requireRegion();
    }

    private String requireRegion() {
        if (region == null || region.isBlank()) {
            throw new IllegalStateException(TimeSeriesSettings.REGION.getKey() + " must be configured for AOSS data-plane signing.");
        }
        return region;
    }
}
