/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/**
 * Builds ThreadContextEndpointResolver for API data-plane calls.
 */
public class ThreadContextEndpointResolverFactory implements ThreadPoolAwareDataSourceEndpointResolverFactory {
    @Override
    public DataSourceEndpointResolver create(Settings settings) {
        throw new IllegalStateException("ThreadContextEndpointResolverFactory requires ThreadPool");
    }

    @Override
    public DataSourceEndpointResolver create(Settings settings, ThreadPool threadPool) {
        return new ThreadContextEndpointResolver(threadPool, TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.get(settings));
    }
}
