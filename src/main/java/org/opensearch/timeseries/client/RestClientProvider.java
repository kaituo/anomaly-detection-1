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
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Provides pooled {@link RestClient} instances keyed by endpoint.
 * Connections are reused across callers to avoid repeated DNS lookups and TLS handshakes.
 */
public final class RestClientProvider {
    private static final Logger LOG = LogManager.getLogger(RestClientProvider.class);
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
                    LOG.warn("Failed to close RestClient", e);
                }
            }
        })
        .build();

    private RestClientProvider() {}

    /**
     * Returns a pooled {@link RestClient} for the given endpoint.
     * The endpoint must include host (and optionally scheme/port). If no scheme is provided, http is assumed.
     *
     * @param endpoint target endpoint host
     * @return pooled RestClient
     */
    public static RestClient getRestClient(String endpoint) {
        String normalizedEndpoint = normalizeEndpoint(endpoint);
        try {
            return REST_CLIENTS.get(normalizedEndpoint, () -> {
                Timeout timeout = Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MILLIS);
                return RestClient
                    .builder(HttpHost.create(normalizedEndpoint))
                    .setRequestConfigCallback(requestConfig -> requestConfig.setConnectTimeout(timeout).setResponseTimeout(timeout))
                    .build();
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to create RestClient for endpoint " + normalizedEndpoint, e);
        }
    }

    public static RestClient getRestClient(String endpoint, int socketTimeoutMillis) {
        String normalizedEndpoint = normalizeEndpoint(endpoint);
        String cacheKey = normalizedEndpoint + "|socket_timeout_millis=" + socketTimeoutMillis;
        try {
            return REST_CLIENTS.get(cacheKey, () -> {
                Timeout socketTimeout = Timeout.ofMilliseconds(socketTimeoutMillis);
                return RestClient
                    .builder(HttpHost.create(normalizedEndpoint))
                    .setRequestConfigCallback(
                        requestConfig -> requestConfig.setConnectTimeout(socketTimeout).setResponseTimeout(socketTimeout)
                    )
                    .build();
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to create RestClient for endpoint " + normalizedEndpoint, e);
        }
    }

    private static String normalizeEndpoint(String endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint must not be null");
        }
        return endpoint.startsWith("http") ? endpoint : "http://" + endpoint;
    }
}
