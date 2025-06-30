/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.endpoint.ThreadContextEndpointResolver;

public class DataPlaneClientFactoryTests extends OpenSearchTestCase {
    private static final String TENANT_ID = "account-1:application-1:workspace-1";
    private static final String APPLICATION_ID = "application-1";
    private static final String DATA_SOURCE_ID = "data-source-1";

    @Override
    public void tearDown() throws Exception {
        SigningRestClientProvider.closeAll();
        RestClientProvider.closeAll();
        super.tearDown();
    }

    public void testContextAwareFactoryUsesDefaultWhenNoOverridePresent() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        RestClient defaultClient = mock(RestClient.class);
        ContextAwareDataPlaneClientFactory factory = new ContextAwareDataPlaneClientFactory(
            threadPool,
            (tenantId, dataSourceId) -> defaultClient
        );

        assertSame(defaultClient, factory.getClient(TENANT_ID, DATA_SOURCE_ID));
    }

    public void testContextAwareFactoryUsesThreadContextOverride() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        RestClient defaultClient = mock(RestClient.class);
        RestClient overrideClient = mock(RestClient.class);
        ContextAwareDataPlaneClientFactory factory = new ContextAwareDataPlaneClientFactory(
            threadPool,
            (tenantId, dataSourceId) -> defaultClient
        );

        DataPlaneClientFactoryContext.setCurrentFactory(threadContext, (tenantId, dataSourceId) -> overrideClient);

        assertSame(overrideClient, factory.getClient(TENANT_ID, DATA_SOURCE_ID));
    }

    public void testAossAwareFactoryUsesSigningClientWhenRequestSigningContextPresent() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://search-domain.us-east-1.es.amazonaws.com");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SESSION_TOKEN_CONTEXT_KEY, "user-token");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SERVICE_NAME_CONTEXT_KEY, "es");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-east-1",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            threadContext,
            new AwsSigV4ThreadContext("endpoint-key"),
            null,
            true
        );

        try {
            assertNotNull(factory.getClient(TENANT_ID, DATA_SOURCE_ID));
        } finally {
            SigningRestClientProvider.closeAll();
        }
    }

    public void testEndpointRegionResolverExtractsAossAndDomainRegions() {
        assertEquals("us-east-1", EndpointRegionResolver.extractRegion("https://id.us-east-1.aoss.amazonaws.com").get());
        assertEquals("us-east-1", EndpointRegionResolver.extractRegion("https://id.beta-us-east-1.aoss.amazonaws.com").get());
        assertEquals("us-west-2", EndpointRegionResolver.extractRegion("https://search-domain.us-west-2.es.amazonaws.com").get());
        assertEquals("us-west-2", EndpointRegionResolver.extractRegion("collection.us-west-2.aoss.amazonaws.com").get());
        assertFalse(EndpointRegionResolver.extractRegion("http://localhost:9200").isPresent());
    }

    public void testAossAwareFactoryUsesEndpointRegionForAossEndpoint() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-west-2.aoss.amazonaws.com");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-east-1",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            null,
            null,
            null,
            true
        );

        RestClient resolvedClient = factory.getClient(TENANT_ID, DATA_SOURCE_ID);
        RestClient westRegionClient = SigningRestClientProvider
            .getRestClient("https://collection.us-west-2.aoss.amazonaws.com", "us-west-2", null);
        RestClient eastRegionClient = SigningRestClientProvider
            .getRestClient("https://collection.us-west-2.aoss.amazonaws.com", "us-east-1", null);

        assertSame(westRegionClient, resolvedClient);
        assertNotSame(eastRegionClient, resolvedClient);
    }

    public void testAossAwareFactoryUsesConfiguredRegionWhenEndpointRegionSigningDisabled() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-west-2.aoss.amazonaws.com");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-east-1",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            null,
            null,
            null,
            false
        );

        RestClient resolvedClient = factory.getClient(TENANT_ID, DATA_SOURCE_ID);
        RestClient eastRegionClient = SigningRestClientProvider
            .getRestClient("https://collection.us-west-2.aoss.amazonaws.com", "us-east-1", null);
        RestClient westRegionClient = SigningRestClientProvider
            .getRestClient("https://collection.us-west-2.aoss.amazonaws.com", "us-west-2", null);

        assertSame(eastRegionClient, resolvedClient);
        assertNotSame(westRegionClient, resolvedClient);
    }

    public void testAossAwareFactoryRequiresRegionWhenSigningEndpointHasNoRegion() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.aoss.amazonaws.com");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            null,
            null,
            null,
            true
        );

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> factory.getClient(TENANT_ID, DATA_SOURCE_ID));
        assertTrue(exception.getMessage().contains("plugins.timeseries.region"));
    }

    public void testAossAwareFactoryUsesEndpointRegionWhenRequestSigningContextPresent() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://search-domain.us-west-2.es.amazonaws.com");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SESSION_TOKEN_CONTEXT_KEY, "user-token");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SERVICE_NAME_CONTEXT_KEY, "es");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            threadContext,
            new AwsSigV4ThreadContext("endpoint-key"),
            null,
            true
        );

        RestClient resolvedClient = factory.getClient(TENANT_ID, DATA_SOURCE_ID);
        RestClient westRegionClient = SigningRestClientProvider
            .getRestClient("https://search-domain.us-west-2.es.amazonaws.com", "us-west-2", null);
        RestClient eastRegionClient = SigningRestClientProvider
            .getRestClient("https://search-domain.us-west-2.es.amazonaws.com", "us-east-1", null);

        assertSame(westRegionClient, resolvedClient);
        assertNotSame(eastRegionClient, resolvedClient);
    }

    public void testAossAwareFactoryRequiresRegionWhenRequestSigningEndpointHasNoRegion() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://search-domain.es.amazonaws.com");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SESSION_TOKEN_CONTEXT_KEY, "user-token");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SERVICE_NAME_CONTEXT_KEY, "es");

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            threadContext,
            new AwsSigV4ThreadContext("endpoint-key"),
            null,
            true
        );

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> factory.getClient(TENANT_ID, DATA_SOURCE_ID));
        assertTrue(exception.getMessage().contains("plugins.timeseries.region"));
    }

    public void testAossAwareFactoryFailsFastWhenThreadContextEndpointMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-east-1",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            threadContext,
            new AwsSigV4ThreadContext("endpoint-key"),
            null,
            true
        );

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> factory.getClient(TENANT_ID, DATA_SOURCE_ID)
        );
        assertTrue(exception.getMessage().contains("Missing data plane endpoint in ThreadContext key [endpoint-key]"));
    }

    public void testThreadContextEndpointResolverReturnsConfiguredTransient() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-west-2.aoss.amazonaws.com");

        ThreadContextEndpointResolver resolver = new ThreadContextEndpointResolver(threadPool, "endpoint-key");

        assertEquals("https://collection.us-west-2.aoss.amazonaws.com", resolver.resolve(APPLICATION_ID, DATA_SOURCE_ID));
    }

    public void testThreadContextEndpointResolverThrowsWhenTransientMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        ThreadContextEndpointResolver resolver = new ThreadContextEndpointResolver(threadPool, "endpoint-key");

        assertThrows(OpenSearchStatusException.class, () -> resolver.resolve(APPLICATION_ID, DATA_SOURCE_ID));
    }

    public void testAossDirectSigningFactoryUsesEndpointRegionForAossEndpoint() {
        DataSourceEndpointResolver resolver = mock(DataSourceEndpointResolver.class);
        when(resolver.resolve(APPLICATION_ID, DATA_SOURCE_ID)).thenReturn("https://collection.us-west-2.aoss.amazonaws.com");
        AossDirectSigningClientFactory factory = new AossDirectSigningClientFactory("us-east-1", resolver, null, null, true);

        DataPlaneClientFactory.ResolvedClient resolvedClient = factory.getResolvedClient(TENANT_ID, DATA_SOURCE_ID);
        RestClient westRegionClient = SigningRestClientProvider
            .getRestClientForTrafficClass(
                "https://collection.us-west-2.aoss.amazonaws.com",
                "us-west-2",
                null,
                null,
                DataPlaneClientFactory.TrafficClass.BACKGROUND
            );
        RestClient eastRegionClient = SigningRestClientProvider
            .getRestClientForTrafficClass(
                "https://collection.us-west-2.aoss.amazonaws.com",
                "us-east-1",
                null,
                null,
                DataPlaneClientFactory.TrafficClass.BACKGROUND
            );

        assertEquals("https://collection.us-west-2.aoss.amazonaws.com", resolvedClient.endpoint());
        assertSame(westRegionClient, resolvedClient.restClient());
        assertNotSame(eastRegionClient, resolvedClient.restClient());
    }

    public void testAossDirectSigningFactoryUsesConfiguredRegionWhenEndpointRegionSigningDisabled() {
        DataSourceEndpointResolver resolver = mock(DataSourceEndpointResolver.class);
        when(resolver.resolve(APPLICATION_ID, DATA_SOURCE_ID)).thenReturn("https://collection.us-west-2.aoss.amazonaws.com");
        AossDirectSigningClientFactory factory = new AossDirectSigningClientFactory("us-east-1", resolver, null, null, false);

        DataPlaneClientFactory.ResolvedClient resolvedClient = factory.getResolvedClient(TENANT_ID, DATA_SOURCE_ID);
        RestClient eastRegionClient = SigningRestClientProvider
            .getRestClientForTrafficClass(
                "https://collection.us-west-2.aoss.amazonaws.com",
                "us-east-1",
                null,
                null,
                DataPlaneClientFactory.TrafficClass.BACKGROUND
            );
        RestClient westRegionClient = SigningRestClientProvider
            .getRestClientForTrafficClass(
                "https://collection.us-west-2.aoss.amazonaws.com",
                "us-west-2",
                null,
                null,
                DataPlaneClientFactory.TrafficClass.BACKGROUND
            );

        assertEquals("https://collection.us-west-2.aoss.amazonaws.com", resolvedClient.endpoint());
        assertSame(eastRegionClient, resolvedClient.restClient());
        assertNotSame(westRegionClient, resolvedClient.restClient());
    }

    public void testForegroundAndBackgroundSigningFactoriesUseSeparateClientsForSameEndpoint() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-west-2.aoss.amazonaws.com");

        AossAwareDataPlaneClientFactory foregroundFactory = new AossAwareDataPlaneClientFactory(
            "us-east-1",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            null,
            null,
            null,
            true
        );
        DataSourceEndpointResolver resolver = mock(DataSourceEndpointResolver.class);
        when(resolver.resolve(APPLICATION_ID, DATA_SOURCE_ID)).thenReturn("https://collection.us-west-2.aoss.amazonaws.com");
        AossDirectSigningClientFactory backgroundFactory = new AossDirectSigningClientFactory("us-east-1", resolver, null, null, true);

        RestClient foregroundClient = foregroundFactory.getClient(TENANT_ID, DATA_SOURCE_ID);
        RestClient backgroundClient = backgroundFactory.getClient(TENANT_ID, DATA_SOURCE_ID);

        assertNotSame(foregroundClient, backgroundClient);
    }

    public void testAossDirectSigningFactoryCachesResolvedEndpoint() {
        DataSourceEndpointResolver resolver = mock(DataSourceEndpointResolver.class);
        when(resolver.resolve(APPLICATION_ID, DATA_SOURCE_ID)).thenReturn("https://collection.us-west-2.aoss.amazonaws.com");
        AossDirectSigningClientFactory factory = new AossDirectSigningClientFactory("us-east-1", resolver, null, null, true);

        DataPlaneClientFactory.ResolvedClient firstClient = factory.getResolvedClient(TENANT_ID, DATA_SOURCE_ID);
        DataPlaneClientFactory.ResolvedClient secondClient = factory.getResolvedClient(TENANT_ID, DATA_SOURCE_ID);

        assertEquals(firstClient.endpoint(), secondClient.endpoint());
        assertSame(firstClient.restClient(), secondClient.restClient());
        verify(resolver).resolve(APPLICATION_ID, DATA_SOURCE_ID);
    }

    public void testAossAwareFactoryDoesNotCacheThreadContextEndpoint() {
        ThreadContext firstThreadContext = new ThreadContext(Settings.EMPTY);
        firstThreadContext.putTransient("endpoint-key", "https://collection-1.us-west-2.aoss.amazonaws.com");
        ThreadContext secondThreadContext = new ThreadContext(Settings.EMPTY);
        secondThreadContext.putTransient("endpoint-key", "https://collection-2.us-west-2.aoss.amazonaws.com");
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(firstThreadContext, secondThreadContext);
        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-east-1",
            new ThreadContextEndpointResolver(threadPool, "endpoint-key"),
            false,
            null,
            null,
            null,
            true
        );

        DataPlaneClientFactory.ResolvedClient firstClient = factory.getResolvedClient(TENANT_ID, DATA_SOURCE_ID);
        DataPlaneClientFactory.ResolvedClient secondClient = factory.getResolvedClient(TENANT_ID, DATA_SOURCE_ID);

        assertEquals("https://collection-1.us-west-2.aoss.amazonaws.com", firstClient.endpoint());
        assertEquals("https://collection-2.us-west-2.aoss.amazonaws.com", secondClient.endpoint());
    }

    public void testModelNodeRestClientUsesDedicatedPool() {
        String endpoint = "localhost:19203";

        RestClient genericClient = RestClientProvider.getRestClient(endpoint);
        RestClient modelNodeClient = RestClientProvider.getModelNodeRestClient(endpoint);
        RestClient sameModelNodeClient = RestClientProvider.getModelNodeRestClient(endpoint);

        assertNotSame(genericClient, modelNodeClient);
        assertSame(modelNodeClient, sameModelNodeClient);
    }

    public void testUnsignedFactoryResolvesApplicationIdFromTenant() throws Exception {
        DataSourceEndpointResolver resolver = mock(DataSourceEndpointResolver.class);
        when(resolver.resolve(APPLICATION_ID, DATA_SOURCE_ID)).thenReturn("localhost:19201");
        UnsignedClientFactory factory = new UnsignedClientFactory(resolver);

        DataPlaneClientFactory.ResolvedClient resolvedClient = factory.getResolvedClient(TENANT_ID, DATA_SOURCE_ID);
        try {
            assertEquals("localhost:19201", resolvedClient.endpoint());
            assertNotNull(resolvedClient.restClient());
        } finally {
            resolvedClient.restClient().close();
        }
    }

    public void testUnsignedFactoryResolvesNullApplicationWhenTenantMissing() throws Exception {
        DataSourceEndpointResolver resolver = mock(DataSourceEndpointResolver.class);
        when(resolver.resolve(null, DATA_SOURCE_ID)).thenReturn("localhost:19202");
        UnsignedClientFactory factory = new UnsignedClientFactory(resolver);

        DataPlaneClientFactory.ResolvedClient resolvedClient = factory.getResolvedClient(null, DATA_SOURCE_ID);
        try {
            assertEquals("localhost:19202", resolvedClient.endpoint());
            assertNotNull(resolvedClient.restClient());
        } finally {
            resolvedClient.restClient().close();
        }
    }
}
