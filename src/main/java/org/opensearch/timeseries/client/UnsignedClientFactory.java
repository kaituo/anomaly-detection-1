/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.client.RestClient;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;

/**
 * Plain unsigned client factory for paths where the hosting environment handles downstream auth.
 */
public class UnsignedClientFactory implements DataPlaneClientFactory {
    private final DataSourceEndpointResolver endpointResolver;

    public UnsignedClientFactory(DataSourceEndpointResolver endpointResolver) {
        this.endpointResolver = Objects.requireNonNull(endpointResolver, "endpointResolver must not be null");
    }

    @Override
    public RestClient getClient(String tenantId) {
        return RestClientProvider.getRestClient(endpointResolver.resolve(tenantId));
    }
}
