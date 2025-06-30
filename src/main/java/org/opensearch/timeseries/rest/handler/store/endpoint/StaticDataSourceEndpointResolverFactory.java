/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import java.util.Objects;

import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/**
 * Builds a resolver that always returns the configured data-plane endpoint.
 */
public class StaticDataSourceEndpointResolverFactory implements DataSourceEndpointResolverFactory {
    @Override
    public DataSourceEndpointResolver create(Settings settings) {
        String endpoint = TimeSeriesSettings.DATA_PLANE_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException(TimeSeriesSettings.DATA_PLANE_ENDPOINT.getKey() + " must be configured");
        }
        return new StaticEndpointResolver(endpoint);
    }

    static class StaticEndpointResolver implements DataSourceEndpointResolver {
        private final String endpoint;

        StaticEndpointResolver(String endpoint) {
            this.endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        }

        @Override
        public String resolve(String tenantId) {
            return endpoint;
        }

        @Override
        public String resolve(String applicationId, String dataSourceId) {
            return endpoint;
        }
    }
}
