/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

/**
 * Endpoint resolver factory variant for resolvers that need ThreadPool / ThreadContext access.
 */
public interface ThreadPoolAwareDataSourceEndpointResolverFactory extends DataSourceEndpointResolverFactory {
    DataSourceEndpointResolver create(Settings settings, ThreadPool threadPool);
}
