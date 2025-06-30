/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.util.ConfiguredFactoryLoader;

/**
 * Loader for endpoint resolver factories configured through plugin settings.
 */
public final class EndpointResolverFactoryLoader {
    private EndpointResolverFactoryLoader() {}

    public static DataSourceEndpointResolver loadDataSourceEndpointResolver(Settings settings, ClassLoader classLoader) {
        DataSourceEndpointResolverFactory factory = ConfiguredFactoryLoader
            .load(
                settings,
                AnomalyDetectorSettings.DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS,
                "data source endpoint resolver factory",
                DataSourceEndpointResolverFactory.class,
                DefaultDataSourceEndpointResolverFactory.class,
                DefaultDataSourceEndpointResolverFactory::new,
                classLoader
            );
        return factory.create(settings);
    }

    public static DataSourceEndpointResolver loadApiDataSourceEndpointResolver(
        Settings settings,
        ThreadPool threadPool,
        ClassLoader classLoader
    ) {
        DataSourceEndpointResolverFactory factory = ConfiguredFactoryLoader
            .load(
                settings,
                AnomalyDetectorSettings.API_DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS,
                "API data source endpoint resolver factory",
                DataSourceEndpointResolverFactory.class,
                DefaultDataSourceEndpointResolverFactory.class,
                DefaultDataSourceEndpointResolverFactory::new,
                classLoader
            );
        if (factory instanceof ThreadPoolAwareDataSourceEndpointResolverFactory) {
            return ((ThreadPoolAwareDataSourceEndpointResolverFactory) factory).create(settings, threadPool);
        }
        return factory.create(settings);
    }
}
