/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import java.util.Objects;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.threadpool.ThreadPool;

/**
 * Resolves the API-path data-plane endpoint from a ThreadContext transient.
 */
public class ThreadContextEndpointResolver implements DataSourceEndpointResolver {
    private final ThreadPool threadPool;
    private final String contextKey;

    public ThreadContextEndpointResolver(ThreadPool threadPool, String contextKey) {
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
        this.contextKey = Objects.requireNonNull(contextKey, "contextKey must not be null");
        if (this.contextKey.isBlank()) {
            throw new IllegalArgumentException("contextKey must not be blank");
        }
    }

    @Override
    public String resolve(String applicationId, String dataSourceId) {
        return resolveFromThreadContext("applicationId=" + applicationId + ", dataSourceId=" + dataSourceId);
    }

    private String resolveFromThreadContext(String targetDescription) {
        String endpoint = threadPool.getThreadContext().getTransient(contextKey);
        if (endpoint == null || endpoint.isBlank()) {
            throw new OpenSearchStatusException(
                "Missing data plane endpoint in ThreadContext key [" + contextKey + "] for " + targetDescription,
                RestStatus.INTERNAL_SERVER_ERROR
            );
        }
        return endpoint;
    }
}
