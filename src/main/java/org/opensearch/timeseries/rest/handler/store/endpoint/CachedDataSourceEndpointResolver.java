/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store.endpoint;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Caches endpoint lookups for data sources whose endpoint is stable during detector execution.
 */
public final class CachedDataSourceEndpointResolver implements DataSourceEndpointResolver {
    private static final long CACHE_TTL_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static final int MAX_CACHED_ENDPOINTS = 10_000;

    private final DataSourceEndpointResolver delegate;
    private final Cache<EndpointCacheKey, String> endpoints;

    public CachedDataSourceEndpointResolver(DataSourceEndpointResolver delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.endpoints = CacheBuilder
            .newBuilder()
            .maximumSize(MAX_CACHED_ENDPOINTS)
            .expireAfterWrite(CACHE_TTL_MILLIS, TimeUnit.MILLISECONDS)
            .build();
    }

    public static DataSourceEndpointResolver wrap(DataSourceEndpointResolver resolver) {
        Objects.requireNonNull(resolver, "resolver must not be null");
        if (resolver instanceof CachedDataSourceEndpointResolver || resolver instanceof ThreadContextEndpointResolver) {
            return resolver;
        }
        return new CachedDataSourceEndpointResolver(resolver);
    }

    @Override
    public String resolve(String applicationId, String dataSourceId) {
        EndpointCacheKey key = new EndpointCacheKey(applicationId, dataSourceId);
        try {
            return endpoints.get(key, () -> delegate.resolve(applicationId, dataSourceId));
        } catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw propagate(e.getCause());
        }
    }

    private RuntimeException propagate(Throwable cause) {
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        throw new RuntimeException("Failed to resolve data source endpoint", cause);
    }

    private static final class EndpointCacheKey {
        private final String applicationId;
        private final String dataSourceId;

        private EndpointCacheKey(String applicationId, String dataSourceId) {
            this.applicationId = applicationId;
            this.dataSourceId = dataSourceId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            EndpointCacheKey other = (EndpointCacheKey) obj;
            return Objects.equals(applicationId, other.applicationId) && Objects.equals(dataSourceId, other.dataSourceId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(applicationId, dataSourceId);
        }
    }
}
