/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.client.RestClient;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;

/**
 * SigV4-signed client factory for direct AOSS data-plane calls from background jobs.
 */
public class AossDirectSigningClientFactory implements DataPlaneClientFactory {
    private final String region;
    private final DataSourceEndpointResolver endpointResolver;

    public AossDirectSigningClientFactory(String region, DataSourceEndpointResolver endpointResolver) {
        this.region = Objects.requireNonNull(region, "region must not be null");
        if (this.region.isBlank()) {
            throw new IllegalArgumentException("region must not be blank");
        }
        this.endpointResolver = Objects.requireNonNull(endpointResolver, "endpointResolver must not be null");
    }

    @Override
    public RestClient getClient(String tenantId) {
        return SigningRestClientProvider.getRestClient(endpointResolver.resolve(tenantId), region);
    }

    @Override
    public boolean supportsInjectedSecurityHeaders() {
        return false;
    }
}
