/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.List;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Before;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.ml.ADCheckpointStore;
import org.opensearch.ad.ml.ADCheckpointStoreFactory;
import org.opensearch.ad.ml.ADCheckpointStoreFactoryContext;
import org.opensearch.ad.ml.ADS3CheckpointStoreFactory;
import org.opensearch.ad.ml.DefaultADCheckpointStoreFactory;
import org.opensearch.ad.ml.HybridThresholdingModel;
import org.opensearch.ad.ml.ThresholdingModel;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.client.ConfigDocumentStore;
import org.opensearch.timeseries.client.ConfigDocumentStoreFactory;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.DataPlaneClientFactory;
import org.opensearch.timeseries.client.DefaultDataAccess;
import org.opensearch.timeseries.client.RemoteMetadataConfigDocumentStore;
import org.opensearch.timeseries.client.RemoteMetadataConfigDocumentStoreFactory;
import org.opensearch.timeseries.client.SdkDataAccess;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.client.TransportConfigDocumentStore;
import org.opensearch.timeseries.ml.CheckpointCodec;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolverFactory;
import org.opensearch.timeseries.rest.handler.store.endpoint.DefaultDataSourceEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.endpoint.ThreadContextEndpointResolverFactory;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.google.gson.Gson;

import io.protostuff.Schema;

public class TimeSeriesAnalyticsPluginTests extends OpenSearchTestCase {
    private TimeSeriesAnalyticsPlugin plugin;
    private Client client;
    private ClusterService clusterService;
    private SdkClient sdkClient;

    @Before
    public void setUpPlugin() {
        plugin = new TimeSeriesAnalyticsPlugin();
        client = mock(Client.class);
        clusterService = mock(ClusterService.class);
        sdkClient = mock(SdkClient.class);
    }

    public void testCreateConfigDocumentStoreUsesDefaultFactoryWhenSettingMissing() {
        ConfigDocumentStore store = invokeCreateConfigDocumentStore(Settings.EMPTY);
        assertTrue(store instanceof RemoteMetadataConfigDocumentStore);
    }

    public void testCreateConfigDocumentStoreUsesConfiguredFactoryWhenSettingPresent() {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.CONFIG_DOCUMENT_STORE_FACTORY_CLASS.getKey(), TestConfigDocumentStoreFactory.class.getName())
            .build();

        ConfigDocumentStore store = invokeCreateConfigDocumentStore(settings);

        assertTrue(store instanceof TestConfigDocumentStore);
    }

    public void testCreateConfigDocumentStoreSupportsConfiguredRemoteMetadataFactory() {
        Settings settings = Settings
            .builder()
            .put(
                AnomalyDetectorSettings.CONFIG_DOCUMENT_STORE_FACTORY_CLASS.getKey(),
                RemoteMetadataConfigDocumentStoreFactory.class.getName()
            )
            .build();

        ConfigDocumentStore store = invokeCreateConfigDocumentStore(settings);

        assertTrue(store instanceof RemoteMetadataConfigDocumentStore);
        assertSame(sdkClient, getFieldValue(store, "sdkClient"));
    }

    public void testCreateConfigDocumentStoreFailsForUnknownFactoryClass() {
        String className = "org.opensearch.timeseries.client.DoesNotExistFactory";
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.CONFIG_DOCUMENT_STORE_FACTORY_CLASS.getKey(), className).build();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> invokeCreateConfigDocumentStore(settings));

        assertTrue(exception.getMessage().contains(className));
        assertNotNull(exception.getCause());
        assertEquals(ClassNotFoundException.class, exception.getCause().getClass());
    }

    public void testCreateConfigDocumentStoreFailsWhenFactoryClassHasWrongType() {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.CONFIG_DOCUMENT_STORE_FACTORY_CLASS.getKey(), String.class.getName())
            .build();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> invokeCreateConfigDocumentStore(settings));

        assertTrue(exception.getMessage().contains("must implement ConfigDocumentStoreFactory"));
        assertTrue(exception.getMessage().contains(String.class.getName()));
    }

    public void testLoadADCheckpointStoreFactoryUsesDefaultFactoryWhenSettingMissing() {
        ADCheckpointStoreFactory factory = invokeLoadADCheckpointStoreFactory(Settings.EMPTY);
        assertTrue(factory instanceof DefaultADCheckpointStoreFactory);
    }

    public void testCreateADCheckpointStoreUsesConfiguredFactoryWhenSettingPresent() {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.CHECKPOINT_STORE_FACTORY_CLASS.getKey(), TestADCheckpointStoreFactory.class.getName())
            .build();

        ADCheckpointStore checkpointStore = invokeCreateADCheckpointStore(settings);

        assertTrue(checkpointStore instanceof TestADCheckpointStore);
    }

    public void testLoadADCheckpointStoreFactorySupportsConfiguredDefaultFactory() {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.CHECKPOINT_STORE_FACTORY_CLASS.getKey(), DefaultADCheckpointStoreFactory.class.getName())
            .build();

        ADCheckpointStoreFactory factory = invokeLoadADCheckpointStoreFactory(settings);

        assertTrue(factory instanceof DefaultADCheckpointStoreFactory);
    }

    public void testLoadADCheckpointStoreFactorySupportsConfiguredS3Factory() {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.CHECKPOINT_STORE_FACTORY_CLASS.getKey(), ADS3CheckpointStoreFactory.class.getName())
            .build();

        ADCheckpointStoreFactory factory = invokeLoadADCheckpointStoreFactory(settings);

        assertTrue(factory instanceof ADS3CheckpointStoreFactory);
    }

    public void testLoadADCheckpointStoreFactoryFailsForUnknownFactoryClass() {
        String className = "org.opensearch.ad.ml.DoesNotExistCheckpointStoreFactory";
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.CHECKPOINT_STORE_FACTORY_CLASS.getKey(), className).build();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> invokeLoadADCheckpointStoreFactory(settings));

        assertTrue(exception.getMessage().contains("Failed to load AD checkpoint store factory"));
        assertTrue(exception.getMessage().contains(className));
        assertEquals(ClassNotFoundException.class, exception.getCause().getClass());
    }

    public void testLoadADCheckpointStoreFactoryFailsWhenFactoryClassHasWrongType() {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.CHECKPOINT_STORE_FACTORY_CLASS.getKey(), String.class.getName())
            .build();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> invokeLoadADCheckpointStoreFactory(settings));

        assertTrue(exception.getMessage().contains("must implement ADCheckpointStoreFactory"));
        assertTrue(exception.getMessage().contains(String.class.getName()));
    }

    public void testCreateDataSourceEndpointResolverUsesDefaultFactoryWhenSettingMissing() {
        DataSourceEndpointResolver resolver = invokeCreateDataSourceEndpointResolver(Settings.EMPTY);
        assertTrue(resolver instanceof DefaultDataSourceEndpointResolver);
    }

    public void testCreateDataSourceEndpointResolverUsesConfiguredFactoryWhenSettingPresent() {
        Settings settings = Settings
            .builder()
            .put(
                AnomalyDetectorSettings.DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS.getKey(),
                TestDataSourceEndpointResolverFactory.class.getName()
            )
            .build();

        DataSourceEndpointResolver resolver = invokeCreateDataSourceEndpointResolver(settings);

        assertTrue(resolver instanceof TestDataSourceEndpointResolver);
        assertEquals("http://datasource-endpoint:9201", resolver.resolve(null));
    }

    public void testCreateDataSourceEndpointResolverFailsForUnknownFactoryClass() {
        String className = "org.opensearch.timeseries.rest.handler.store.spi.DoesNotExistDataSourceEndpointResolverFactory";
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS.getKey(), className)
            .build();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> invokeCreateDataSourceEndpointResolver(settings));

        assertTrue(exception.getMessage().contains("Failed to load data source endpoint resolver factory"));
        assertTrue(exception.getMessage().contains(className));
        assertEquals(ClassNotFoundException.class, exception.getCause().getClass());
    }

    public void testCreateDataSourceEndpointResolverFailsWhenFactoryClassHasWrongType() {
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS.getKey(), String.class.getName())
            .build();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> invokeCreateDataSourceEndpointResolver(settings));

        assertTrue(exception.getMessage().contains("must implement DataSourceEndpointResolverFactory"));
        assertTrue(exception.getMessage().contains(String.class.getName()));
    }

    public void testDefaultDataSourceEndpointResolverAlwaysReturnsDefaultEndpoint() {
        DefaultDataSourceEndpointResolver resolver = new DefaultDataSourceEndpointResolver();
        assertEquals(DefaultDataSourceEndpointResolver.DEFAULT_ENDPOINT, resolver.resolve(null));
        assertEquals(DefaultDataSourceEndpointResolver.DEFAULT_ENDPOINT, resolver.resolve("application-id", "data-source-id"));
    }

    public void testApiDataPlaneFactoryDirectSignsWhenAossModeUsesThreadContextEndpointResolver() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME.getKey(), "aoss")
            .put(TimeSeriesSettings.REGION.getKey(), "us-west-2")
            .put(
                AnomalyDetectorSettings.API_DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS.getKey(),
                ThreadContextEndpointResolverFactory.class.getName()
            )
            .put(TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.getKey(), "endpoint-key")
            .build();

        DataPlaneClientFactory factory = invokeGetOrCreateDataPlaneClientFactory(settings, threadPool, true);

        assertFalse(factory.supportsInjectedSecurityHeaders());
    }

    public void testApiDataPlaneFactoryDirectSignsResolvedAossEndpoint() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "https://collection.us-west-2.aoss.amazonaws.com");

        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.REGION.getKey(), "us-west-2")
            .put(
                AnomalyDetectorSettings.API_DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS.getKey(),
                ThreadContextEndpointResolverFactory.class.getName()
            )
            .put(TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.getKey(), "endpoint-key")
            .build();

        DataPlaneClientFactory factory = invokeGetOrCreateDataPlaneClientFactory(settings, threadPool, true);

        assertFalse(factory.supportsInjectedSecurityHeaders());
    }

    public void testApiDataPlaneFactoryKeepsSecurityHeadersForResolvedNonAossEndpoint() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        threadContext.putTransient("endpoint-key", "http://localhost:9201");

        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.REGION.getKey(), "us-west-2")
            .put(
                AnomalyDetectorSettings.API_DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS.getKey(),
                ThreadContextEndpointResolverFactory.class.getName()
            )
            .put(TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.getKey(), "endpoint-key")
            .build();

        DataPlaneClientFactory factory = invokeGetOrCreateDataPlaneClientFactory(settings, threadPool, true);

        assertTrue(factory.supportsInjectedSecurityHeaders());
    }

    public void testCreateDataAccessUsesProvidedConfigDocumentStoreForMultiTenancy() {
        ConfigDocumentStore configDocumentStore = mock(ConfigDocumentStore.class);
        Settings settings = Settings.EMPTY;
        SdkStateManager stateManager = mock(SdkStateManager.class);

        DataAccess dataAccess = invokeCreateDataAccess(settings, true, configDocumentStore, stateManager);

        assertTrue(dataAccess instanceof SdkDataAccess);
        assertSame(configDocumentStore, getFieldValue(dataAccess, "configDocumentStore"));
    }

    public void testCreateDataAccessUsesTransportConfigDocumentStoreForSingleTenant() {
        ConfigDocumentStore configDocumentStore = mock(ConfigDocumentStore.class);

        DataAccess dataAccess = invokeCreateDataAccess(Settings.EMPTY, false, configDocumentStore, null);

        assertTrue(dataAccess instanceof DefaultDataAccess);
        assertTrue(getFieldValue(dataAccess, "configDocumentStore") instanceof TransportConfigDocumentStore);
        assertNotSame(configDocumentStore, getFieldValue(dataAccess, "configDocumentStore"));
    }

    public void testShouldInitializeEventBridgeHandler() {
        assertTrue(TimeSeriesAnalyticsPlugin.shouldInitializeEventBridgeHandler(true, List.of(TimeSeriesSettings.MASTER_ROLE)));
        assertTrue(TimeSeriesAnalyticsPlugin.shouldInitializeEventBridgeHandler(true, List.of(TimeSeriesSettings.COORDINATOR_ROLE)));
        assertTrue(
            TimeSeriesAnalyticsPlugin
                .shouldInitializeEventBridgeHandler(true, List.of(TimeSeriesSettings.MASTER_ROLE, TimeSeriesSettings.COORDINATOR_ROLE))
        );
        assertFalse(TimeSeriesAnalyticsPlugin.shouldInitializeEventBridgeHandler(true, List.of(TimeSeriesSettings.MODEL_ROLE)));
        assertFalse(TimeSeriesAnalyticsPlugin.shouldInitializeEventBridgeHandler(false, List.of(TimeSeriesSettings.MASTER_ROLE)));
    }

    private ConfigDocumentStore invokeCreateConfigDocumentStore(Settings settings) {
        setFieldValue(plugin, "pluginSettings", settings);
        setFieldValue(plugin, "clusterService", clusterService);
        return (ConfigDocumentStore) invokePrivateMethod(
            plugin,
            "createConfigDocumentStore",
            new Class<?>[] { SdkClient.class },
            sdkClient
        );
    }

    private ADCheckpointStoreFactory invokeLoadADCheckpointStoreFactory(Settings settings) {
        setFieldValue(plugin, "pluginSettings", settings);
        return (ADCheckpointStoreFactory) invokePrivateMethod(plugin, "loadADCheckpointStoreFactory", new Class<?>[0]);
    }

    private ADCheckpointStore invokeCreateADCheckpointStore(Settings settings) {
        setFieldValue(plugin, "pluginSettings", settings);
        return (ADCheckpointStore) invokePrivateMethod(
            plugin,
            "createADCheckpointStore",
            new Class<?>[] { ADCheckpointStoreFactoryContext.class },
            createCheckpointStoreFactoryContext(settings)
        );
    }

    private DataSourceEndpointResolver invokeCreateDataSourceEndpointResolver(Settings settings) {
        setFieldValue(plugin, "pluginSettings", settings);
        return (DataSourceEndpointResolver) invokePrivateMethod(plugin, "createDataSourceEndpointResolver", new Class<?>[0]);
    }

    private DataPlaneClientFactory invokeGetOrCreateDataPlaneClientFactory(
        Settings settings,
        ThreadPool threadPool,
        boolean multiTenancyEnabled
    ) {
        return (DataPlaneClientFactory) invokePrivateMethod(
            plugin,
            "getOrCreateDataPlaneClientFactory",
            new Class<?>[] { Settings.class, ThreadPool.class, boolean.class },
            settings,
            threadPool,
            multiTenancyEnabled
        );
    }

    private DataAccess invokeCreateDataAccess(
        Settings settings,
        boolean multiTenancyEnabled,
        ConfigDocumentStore configDocumentStore,
        SdkStateManager stateManager
    ) {
        setFieldValue(plugin, "pluginSettings", settings);
        setFieldValue(plugin, "client", client);
        setFieldValue(plugin, "clusterService", clusterService);
        setFieldValue(plugin, "securityClientUtil", mock(SecurityClientUtil.class));
        return (DataAccess) invokePrivateMethod(
            plugin,
            "createDataAccess",
            new Class<?>[] {
                boolean.class,
                SdkClient.class,
                ConfigDocumentStore.class,
                SdkStateManager.class,
                IndexNameExpressionResolver.class,
                DataPlaneClientFactory.class, },
            multiTenancyEnabled,
            sdkClient,
            configDocumentStore,
            stateManager,
            mock(IndexNameExpressionResolver.class),
            mock(DataPlaneClientFactory.class)
        );
    }

    @SuppressWarnings("unchecked")
    private ADCheckpointStoreFactoryContext createCheckpointStoreFactoryContext(Settings settings) {
        return new ADCheckpointStoreFactoryContext(
            settings,
            clusterService,
            client,
            mock(ClientUtil.class),
            new Gson(),
            mock(RandomCutForestMapper.class),
            mock(V1JsonToV3StateConverter.class),
            mock(ThresholdedRandomCutForestMapper.class),
            mock(Schema.class),
            HybridThresholdingModel.class,
            mock(org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement.class),
            1024,
            mock(GenericObjectPool.class),
            1024,
            0.01d,
            Clock.systemUTC()
        );
    }

    private Object invokePrivateMethod(Object target, String methodName, Class<?>[] parameterTypes, Object... args) {
        try {
            Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new AssertionError(cause);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private void setFieldValue(Object target, String fieldName, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private Object getFieldValue(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public static class TestConfigDocumentStoreFactory implements ConfigDocumentStoreFactory {
        @Override
        public ConfigDocumentStore create(Settings settings, ClusterService clusterService) {
            return new TestConfigDocumentStore();
        }
    }

    public static class TestADCheckpointStoreFactory implements ADCheckpointStoreFactory {
        @Override
        public ADCheckpointStore create(ADCheckpointStoreFactoryContext context) {
            return new TestADCheckpointStore();
        }
    }

    public static class TestDataSourceEndpointResolverFactory implements DataSourceEndpointResolverFactory {
        @Override
        public DataSourceEndpointResolver create(Settings settings) {
            return new TestDataSourceEndpointResolver();
        }
    }

    public static class TestConfigDocumentStore implements ConfigDocumentStore {
        @Override
        public void search(SearchRequest request, TenantContext tenantContext, ActionListener<SearchResponse> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void get(GetRequest request, TenantContext tenantContext, ActionListener<GetResponse> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void index(IndexRequest request, TenantContext tenantContext, ActionListener<IndexResponse> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void update(UpdateRequest request, TenantContext tenantContext, ActionListener<UpdateResponse> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(DeleteRequest request, TenantContext tenantContext, ActionListener<DeleteResponse> listener) {
            throw new UnsupportedOperationException();
        }
    }

    public static class TestDataSourceEndpointResolver implements DataSourceEndpointResolver {
        @Override
        public String resolve(String tenantId) {
            return "http://datasource-endpoint:9201";
        }

        @Override
        public String resolve(String applicationId, String dataSourceId) {
            return "http://datasource-endpoint:9201";
        }
    }

    public static class TestADCheckpointStore implements ADCheckpointStore {
        @Override
        public void deleteModelCheckpoint(Config config, String modelId, ActionListener<Void> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void batchWrite(BulkRequest request, ActionListener<BulkResponse> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void batchRead(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteModelCheckpointByConfigId(String tenantId, String configId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointCodec<ThresholdedRandomCutForest> getCodec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTRCFCheckpoint(String modelId, ThresholdedRandomCutForest forest, ActionListener<Void> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putThresholdCheckpoint(String modelId, ThresholdingModel threshold, ActionListener<Void> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getTRCFModel(String modelId, ActionListener<Optional<ThresholdedRandomCutForest>> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getThresholdModel(String modelId, ActionListener<Optional<ThresholdingModel>> listener) {
            throw new UnsupportedOperationException();
        }
    }
}
