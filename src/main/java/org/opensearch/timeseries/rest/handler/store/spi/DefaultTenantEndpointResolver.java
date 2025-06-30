/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.spi;

/**
 * Default resolver that always returns the local OpenSearch HTTP endpoint.
 */
public class DefaultTenantEndpointResolver implements TenantEndpointResolver {
    private static final String DEFAULT_ENDPOINT = "http://localhost:9201";
    private static final String ENDPOINT_PROPERTY = "opensearch.tenant.endpoint";
    private static final String ENDPOINT_ENV = "OPENSEARCH_TENANT_ENDPOINT";

    @Override
    public String resolve(String tenantId) {
        String configuredEndpoint = System.getProperty(ENDPOINT_PROPERTY);
        if (configuredEndpoint == null || configuredEndpoint.isBlank()) {
            configuredEndpoint = System.getenv(ENDPOINT_ENV);
        }
        return configuredEndpoint == null || configuredEndpoint.isBlank() ? DEFAULT_ENDPOINT : configuredEndpoint;
    }
}
