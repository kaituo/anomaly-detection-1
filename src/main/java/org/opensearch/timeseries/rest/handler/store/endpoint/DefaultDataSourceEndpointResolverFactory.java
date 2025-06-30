/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import org.opensearch.common.settings.Settings;

/**
 * Default factory that builds the data-source-backed endpoint resolver.
 */
public class DefaultDataSourceEndpointResolverFactory implements DataSourceEndpointResolverFactory {
    @Override
    public DataSourceEndpointResolver create(Settings settings) {
        return new DefaultDataSourceEndpointResolver();
    }
}
