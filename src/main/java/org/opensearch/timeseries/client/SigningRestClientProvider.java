/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.RestClient;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Provides pooled SigV4-signed RestClient instances.
 */
public final class SigningRestClientProvider {
    private static final Logger LOG = LogManager.getLogger(SigningRestClientProvider.class);
    private static final long REST_CLIENT_TTL_MILLIS = TimeUnit.MINUTES.toMillis(10);

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

    public static RestClient getRestClient(String endpoint, String region, String serviceName) {
        return getRestClient(endpoint, region, null, serviceName);
    }

    public static RestClient getRestClient(String endpoint, String region, AwsCredentialsProvider credentialsProvider, String serviceName) {
        return getRestClientForTrafficClass(
            endpoint,
            region,
            credentialsProvider,
            serviceName,
            DataPlaneClientFactory.TrafficClass.FOREGROUND
        );
    }

    public static RestClient getRestClientForTrafficClass(
        String endpoint,
        String region,
        AwsCredentialsProvider credentialsProvider,
        String serviceName,
        DataPlaneClientFactory.TrafficClass trafficClass
    ) {
        String normalizedEndpoint = normalizeEndpoint(endpoint);
        String normalizedRegion = region == null ? "" : region.trim();
        String normalizedServiceName = serviceName == null ? "" : serviceName.trim();
        DataPlaneClientFactory.TrafficClass normalizedTrafficClass = normalizeTrafficClass(trafficClass);
        String cacheKey = buildCacheKey(normalizedEndpoint, normalizedRegion, normalizedServiceName, credentialsProvider, normalizedTrafficClass);
        try {
            return REST_CLIENTS.get(cacheKey, () -> {
                return RestClient
                    .builder(HttpHost.create(normalizedEndpoint))
                    .setRequestConfigCallback(
                        requestConfig -> RestClientPoolConfig
                            .configureRequestTimeouts(requestConfig, RestClientPoolConfig.REQUEST_TIMEOUT)
                    )
                    .setHttpClientConfigCallback(
                        builder -> RestClientPoolConfig
                            .configureConnectionPool(builder)
                            .addRequestInterceptorLast(
                                credentialsProvider == null
                                    ? new AwsSigV4RequestInterceptor(normalizedRegion, normalizedServiceName)
                                    : new AwsSigV4RequestInterceptor(normalizedRegion, normalizedServiceName, credentialsProvider)
                            )
                    )
                    .build();
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to create signing RestClient for endpoint " + normalizedEndpoint, e);
        }
    }

    private static String buildCacheKey(
        String endpoint,
        String region,
        String serviceName,
        AwsCredentialsProvider credentialsProvider,
        DataPlaneClientFactory.TrafficClass trafficClass
    ) {
        /*
         * The RestClient owns the Apache HTTP client, and the Apache HTTP client owns the
         * AwsSigV4RequestInterceptor created in getRestClient(...). That interceptor captures
         * two pieces of signing behavior at construction time: the SigV4 signing service name
         * and the credentials provider. Reusing a RestClient across either of those dimensions
         * silently signs future requests with the wrong SigV4 scope or principal.
         *
         * endpoint:
         *   The target collection/domain host, for example
         *   https://abc.us-west-2.aoss.amazonaws.com. A client for one endpoint must not be
         *   reused for another endpoint.
         *
         * region:
         *   Part of the SigV4 credential scope, for example us-west-2 in
         *   Credential=AKIA.../20260514/us-west-2/aoss/aws4_request. Reusing a us-east-1 client
         *   for a us-west-2 collection produces signatures scoped to the wrong region.
         *
         * signing service name:
         *   Also part of the SigV4 credential scope. AOSS collection requests use "aoss"
         *   (Credential=.../us-west-2/aoss/aws4_request), while managed OpenSearch Service
         *   domains can use "es" (Credential=.../us-west-2/es/aws4_request). The same endpoint
         *   and region with a different service name requires a different interceptor.
         *
         * credentials provider identity:
         *   The provider is the object the interceptor calls for credentials on every request.
         *   Examples are the default service/task-role provider, or an assume-role provider for
         *   a customer role such as arn:aws:iam::222222222222:role/MyTargetRole. We key by the
         *   provider object's identity rather than by current access key because temporary
         *   credentials refresh over time; the same provider should keep reusing its client,
         *   but a different provider must get a different client so it signs as the intended
         *   principal.
         */
        String credentialsKey = credentialsProvider == null ? "default" : "provider@" + System.identityHashCode(credentialsProvider);
        String baseKey = endpoint + "|" + region + "|" + serviceName + "|" + credentialsKey;
        return trafficClass == DataPlaneClientFactory.TrafficClass.FOREGROUND ? baseKey : baseKey + "|traffic=" + trafficClass;
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

    private static DataPlaneClientFactory.TrafficClass normalizeTrafficClass(DataPlaneClientFactory.TrafficClass trafficClass) {
        return trafficClass == null ? DataPlaneClientFactory.TrafficClass.FOREGROUND : trafficClass;
    }
}
