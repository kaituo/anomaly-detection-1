/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import org.opensearch.common.settings.Settings;

/**
 * Factory for repository-specific {@link DataSourceEndpointResolver} implementations.
 */
public interface DataSourceEndpointResolverFactory {
    DataSourceEndpointResolver create(Settings settings);
}
