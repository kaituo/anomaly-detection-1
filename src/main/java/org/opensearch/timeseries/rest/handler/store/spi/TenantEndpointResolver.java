/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.spi;

/**
 * Resolves the endpoint to contact for a given tenant when issuing HTTP requests to check index/alias existence.
 */
public interface TenantEndpointResolver {
    /**
     * Translate a tenant id to an HTTP endpoint (host:port or URL).
     *
     * @param tenantId tenant identifier; may be {@code null} when tenant is not specified
     * @return endpoint string
     */
    String resolve(String tenantId);
}
