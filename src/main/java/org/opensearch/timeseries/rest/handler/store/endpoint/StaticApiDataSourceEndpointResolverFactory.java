/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/**
 * Builds a resolver that always returns the configured API-path data-plane endpoint.
 */
public class StaticApiDataSourceEndpointResolverFactory implements DataSourceEndpointResolverFactory {
    @Override
    public DataSourceEndpointResolver create(Settings settings) {
        String endpoint = TimeSeriesSettings.API_DATA_PLANE_ENDPOINT.get(settings);
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException(TimeSeriesSettings.API_DATA_PLANE_ENDPOINT.getKey() + " must be configured");
        }
        return new StaticDataSourceEndpointResolverFactory.StaticEndpointResolver(endpoint);
    }
}
