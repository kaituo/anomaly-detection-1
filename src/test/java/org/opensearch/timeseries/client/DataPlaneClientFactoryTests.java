/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.rest.handler.store.endpoint.ThreadContextEndpointResolver;

public class DataPlaneClientFactoryTests extends OpenSearchTestCase {
    public void testContextAwareFactoryUsesDefaultWhenNoOverridePresent() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        RestClient defaultClient = mock(RestClient.class);
        ContextAwareDataPlaneClientFactory factory = new ContextAwareDataPlaneClientFactory(threadPool, tenantId -> defaultClient);

        assertSame(defaultClient, factory.getClient("tenant"));
    }

    public void testContextAwareFactoryUsesThreadContextOverride() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        RestClient defaultClient = mock(RestClient.class);
        RestClient overrideClient = mock(RestClient.class);
        ContextAwareDataPlaneClientFactory factory = new ContextAwareDataPlaneClientFactory(threadPool, tenantId -> defaultClient);

        DataPlaneClientFactoryContext.setCurrentFactory(threadContext, tenantId -> overrideClient);

        assertSame(overrideClient, factory.getClient("tenant"));
    }

    public void testAossAwareFactorySuppressesSecurityHeadersForAossMode() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-west-2",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            true
        );

        assertFalse(factory.supportsInjectedSecurityHeaders());
    }

    public void testAossAwareFactorySuppressesSecurityHeadersForResolvedAossEndpoint() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-west-2.aoss.amazonaws.com");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-west-2",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false
        );

        assertFalse(factory.supportsInjectedSecurityHeaders());
    }

    public void testAossAwareFactoryAllowsSecurityHeadersForNonAossEndpoint() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "http://localhost:9201");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-west-2",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false
        );

        assertTrue(factory.supportsInjectedSecurityHeaders());
    }

    public void testAossAwareFactorySuppressesSecurityHeadersWhenRequestSigningContextPresent() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://search-domain.us-east-1.es.amazonaws.com");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SESSION_TOKEN_CONTEXT_KEY, "user-token");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_REGION_CONTEXT_KEY, "us-east-1");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SERVICE_NAME_CONTEXT_KEY, "es");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            threadContext,
            new AwsSigV4ThreadContext("endpoint-key")
        );

        try {
            assertFalse(factory.supportsInjectedSecurityHeaders());
            assertNotNull(factory.getClient("tenant"));
        } finally {
            SigningRestClientProvider.closeAll();
        }
    }

    public void testAossAwareFactoryRequiresRegionWhenSigningResolvedAossEndpoint() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-west-2.aoss.amazonaws.com");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false
        );

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> factory.getClient("tenant"));
        assertTrue(exception.getMessage().contains("plugins.timeseries.region"));
    }

    public void testAossAwareFactoryUsesRequestRegionWhenConfiguredRegionMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-east-1.aoss.amazonaws.com");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_REGION_CONTEXT_KEY, "us-east-1");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            threadContext,
            new AwsSigV4ThreadContext("endpoint-key")
        );

        try {
            assertNotNull(factory.getClient("tenant"));
        } finally {
            SigningRestClientProvider.closeAll();
        }
    }

    public void testThreadContextEndpointResolverReturnsConfiguredTransient() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-west-2.aoss.amazonaws.com");

        ThreadContextEndpointResolver resolver = new ThreadContextEndpointResolver(threadPool, "endpoint-key");

        assertEquals("https://collection.us-west-2.aoss.amazonaws.com", resolver.resolve("tenant"));
        assertEquals("https://collection.us-west-2.aoss.amazonaws.com", resolver.resolve("app", "data-source"));
    }

    public void testThreadContextEndpointResolverThrowsWhenTransientMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        ThreadContextEndpointResolver resolver = new ThreadContextEndpointResolver(threadPool, "endpoint-key");

        assertThrows(OpenSearchStatusException.class, () -> resolver.resolve("tenant"));
    }
}
