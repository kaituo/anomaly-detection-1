/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.spi;

/**
 * Default resolver that always returns the local OpenSearch HTTP endpoint.
 */
public class DefaultTenantEndpointResolver implements TenantEndpointResolver {
    private static final String DEFAULT_ENDPOINT = "http://localhost:9201";

    @Override
    public String resolve(String tenantId) {
        return DEFAULT_ENDPOINT;
    }
}
