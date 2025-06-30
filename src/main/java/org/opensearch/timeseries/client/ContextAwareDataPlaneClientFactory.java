/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.common.lease.Releasable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Chooses a ThreadContext override when present; otherwise delegates to the default API client factory.
 */
public class ContextAwareDataPlaneClientFactory implements DataPlaneClientFactory {
    private final ThreadPool threadPool;
    private final DataPlaneClientFactory defaultFactory;

    public ContextAwareDataPlaneClientFactory(ThreadPool threadPool, DataPlaneClientFactory defaultFactory) {
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
        this.defaultFactory = Objects.requireNonNull(defaultFactory, "defaultFactory must not be null");
    }

    @Override
    public RestClient getClient(String tenantId, String dataSourceId) {
        return getResolvedClient(tenantId, dataSourceId).restClient();
    }

    @Override
    public ResolvedClient getResolvedClient(String tenantId, String dataSourceId) {
        return currentFactory().getResolvedClient(tenantId, dataSourceId);
    }

    @Override
    public RequestContext createRequestContext(String tenantId, String dataSourceId) {
        return currentFactory().createRequestContext(tenantId, dataSourceId);
    }

    @Override
    public Releasable prepareRequest(Request request) {
        return currentFactory().prepareRequest(request);
    }

    private DataPlaneClientFactory currentFactory() {
        DataPlaneClientFactory currentFactory = DataPlaneClientFactoryContext.getCurrentFactory(threadPool.getThreadContext());
        return currentFactory == null ? defaultFactory : currentFactory;
    }
}
