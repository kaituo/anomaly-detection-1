/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.client.RestClient;

/**
 * Factory for RestClient instances that reach the data plane for a tenant.
 */
public interface DataPlaneClientFactory {
    RestClient getClient(String tenantId);

    /**
     * Whether requests sent through this factory can carry OpenSearch Security plugin injected-user headers.
     * Direct AOSS requests reject those local-cluster headers.
     */
    default boolean supportsInjectedSecurityHeaders() {
        return true;
    }
}
