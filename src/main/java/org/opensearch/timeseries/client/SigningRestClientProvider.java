/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.RestClient;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Provides pooled SigV4-signed RestClient instances keyed by endpoint and region.
 */
public final class SigningRestClientProvider {
    private static final Logger LOG = LogManager.getLogger(SigningRestClientProvider.class);
    private static final long REST_CLIENT_TTL_MILLIS = TimeUnit.MINUTES.toMillis(10);
    private static final int DEFAULT_TIMEOUT_MILLIS = 60_000;

    private static final Cache<String, RestClient> REST_CLIENTS = CacheBuilder
        .newBuilder()
        .maximumSize(TimeSeriesSettings.MAX_REST_CLIENTS)
        .expireAfterAccess(REST_CLIENT_TTL_MILLIS, TimeUnit.MILLISECONDS)
        .removalListener(notification -> {
            RestClient toClose = (RestClient) notification.getValue();
            if (toClose != null) {
                try {
                    toClose.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close signing RestClient", e);
                }
            }
        })
        .build();

    private SigningRestClientProvider() {}

    public static RestClient getRestClient(String endpoint, String region) {
        return getRestClient(endpoint, region, null, null);
    }

    public static RestClient getRestClient(
        String endpoint,
        String region,
        ThreadContext threadContext,
        AwsSigV4ThreadContext sigV4ThreadContext
    ) {
        String normalizedEndpoint = normalizeEndpoint(endpoint);
        String normalizedRegion = region == null ? "" : region.trim();
        String cacheKey = buildCacheKey(normalizedEndpoint, normalizedRegion, threadContext);
        try {
            return REST_CLIENTS.get(cacheKey, () -> {
                Timeout timeout = Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MILLIS);
                return RestClient
                    .builder(HttpHost.create(normalizedEndpoint))
                    .setRequestConfigCallback(requestConfig -> requestConfig.setConnectTimeout(timeout).setResponseTimeout(timeout))
                    .setHttpClientConfigCallback(
                        builder -> builder.addRequestInterceptorLast(createInterceptor(normalizedRegion, threadContext, sigV4ThreadContext))
                    )
                    .build();
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to create signing RestClient for endpoint " + normalizedEndpoint, e);
        }
    }

    private static AwsSigV4RequestInterceptor createInterceptor(
        String region,
        ThreadContext threadContext,
        AwsSigV4ThreadContext sigV4ThreadContext
    ) {
        if (threadContext == null) {
            return new AwsSigV4RequestInterceptor(region);
        }
        return new AwsSigV4RequestInterceptor(region, threadContext, sigV4ThreadContext);
    }

    private static String buildCacheKey(String endpoint, String region, ThreadContext threadContext) {
        String contextKey = threadContext == null ? "service" : "request-" + System.identityHashCode(threadContext);
        return endpoint + "|" + region + "|" + contextKey;
    }

    public static void closeAll() {
        REST_CLIENTS.invalidateAll();
        REST_CLIENTS.cleanUp();
    }

    private static String normalizeEndpoint(String endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint must not be null");
        }
        return endpoint.startsWith("http") ? endpoint : "https://" + endpoint;
    }
}
