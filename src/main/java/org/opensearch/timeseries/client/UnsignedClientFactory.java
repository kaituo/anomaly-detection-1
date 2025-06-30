/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.client.RestClient;
import org.opensearch.timeseries.rest.handler.store.endpoint.CachedDataSourceEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.util.TenantAwareHelper;

/**
 * Plain unsigned client factory for paths where the hosting environment handles downstream auth.
 */
public class UnsignedClientFactory implements DataPlaneClientFactory {
    private final DataSourceEndpointResolver endpointResolver;
    private final DataPlaneClientFactory.TrafficClass trafficClass;

    public UnsignedClientFactory(DataSourceEndpointResolver endpointResolver) {
        this(endpointResolver, DataPlaneClientFactory.TrafficClass.FOREGROUND);
    }

    public static UnsignedClientFactory background(DataSourceEndpointResolver endpointResolver) {
        return new UnsignedClientFactory(endpointResolver, DataPlaneClientFactory.TrafficClass.BACKGROUND);
    }

    private UnsignedClientFactory(
        DataSourceEndpointResolver endpointResolver,
        DataPlaneClientFactory.TrafficClass trafficClass
    ) {
        this.endpointResolver = CachedDataSourceEndpointResolver
            .wrap(Objects.requireNonNull(endpointResolver, "endpointResolver must not be null"));
        this.trafficClass = Objects.requireNonNull(trafficClass, "trafficClass must not be null");
    }

    @Override
    public RestClient getClient(String tenantId, String dataSourceId) {
        return getResolvedClient(tenantId, dataSourceId).restClient();
    }

    @Override
    public ResolvedClient getResolvedClient(String tenantId, String dataSourceId) {
        String endpoint = resolveEndpoint(tenantId, dataSourceId);
        return new ResolvedClient(RestClientProvider.getRestClientForTrafficClass(endpoint, trafficClass), endpoint);
    }

    private String resolveEndpoint(String tenantId, String dataSourceId) {
        String applicationId = tenantId == null ? null : TenantAwareHelper.parseTenantId(tenantId).applicationId();
        return endpointResolver.resolve(applicationId, dataSourceId);
    }
}
