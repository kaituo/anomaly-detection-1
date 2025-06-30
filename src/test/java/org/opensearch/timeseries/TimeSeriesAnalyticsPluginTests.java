/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Before;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.client.ConfigDocumentStore;
import org.opensearch.timeseries.client.ConfigDocumentStoreFactory;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RemoteMetadataConfigDocumentStore;
import org.opensearch.timeseries.client.SdkDataAccess;
import org.opensearch.timeseries.client.TenantContext;

public class TimeSeriesAnalyticsPluginTests extends OpenSearchTestCase {
    private TimeSeriesAnalyticsPlugin plugin;
    private ClusterService clusterService;
    private SdkClient sdkClient;

    @Before
    public void setUpPlugin() {
        plugin = new TimeSeriesAnalyticsPlugin();
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

    public void testCreateDataAccessUsesProvidedConfigDocumentStoreForMultiTenancy() {
        ConfigDocumentStore configDocumentStore = mock(ConfigDocumentStore.class);
        Settings settings = Settings.EMPTY;
        SdkStateManager stateManager = mock(SdkStateManager.class);

        DataAccess dataAccess = invokeCreateDataAccess(settings, configDocumentStore, stateManager);

        assertTrue(dataAccess instanceof SdkDataAccess);
        assertSame(configDocumentStore, getFieldValue(dataAccess, "configDocumentStore"));
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

    private DataAccess invokeCreateDataAccess(Settings settings, ConfigDocumentStore configDocumentStore, SdkStateManager stateManager) {
        setFieldValue(plugin, "pluginSettings", settings);
        setFieldValue(plugin, "clusterService", clusterService);
        return (DataAccess) invokePrivateMethod(
            plugin,
            "createDataAccess",
            new Class<?>[] {
                boolean.class,
                SdkClient.class,
                ConfigDocumentStore.class,
                SdkStateManager.class,
                IndexNameExpressionResolver.class, },
            true,
            sdkClient,
            configDocumentStore,
            stateManager,
            mock(IndexNameExpressionResolver.class)
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
        public ConfigDocumentStore create(SdkClient sdkClient, Settings settings, ClusterService clusterService) {
            return new TestConfigDocumentStore();
        }
    }

    public static class TestConfigDocumentStore implements ConfigDocumentStore {
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
}
