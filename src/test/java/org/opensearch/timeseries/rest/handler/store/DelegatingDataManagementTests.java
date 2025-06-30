/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.TimeSeriesIndex;

public class DelegatingDataManagementTests extends OpenSearchTestCase {

    private static final Setting<Boolean> MULTI_TENANCY_ENABLED = Setting
        .boolSetting("plugins.timeseries.test.multi_tenancy_enabled", false, Setting.Property.NodeScope);

    private enum TestIndex implements TimeSeriesIndex {
        CONFIG;

        @Override
        public String getIndexName() {
            return "test-config";
        }

        @Override
        public boolean isAlias() {
            return false;
        }

        @Override
        public String getMapping() {
            return "{}";
        }

        @Override
        public boolean isConfigIndex() {
            return true;
        }

        @Override
        public boolean isResultIndex() {
            return false;
        }
    }

    public void testUsesIndexStoreWhenMultiTenancyDisabled() {
        DataManagement<TestIndex> indexStore = mockStore();
        DataManagement<TestIndex> sdkStore = mockStore();
        when(indexStore.doesConfigIndexExist()).thenReturn(true);

        DelegatingDataManagement<TestIndex> dataManagement = newDataManagement(false, indexStore, sdkStore);

        assertTrue(dataManagement.doesConfigIndexExist());
        verify(indexStore).doesConfigIndexExist();
        verifyNoInteractions(sdkStore);
    }

    public void testUsesSdkStoreWhenMultiTenancyEnabledAndSdkStorePresent() {
        DataManagement<TestIndex> indexStore = mockStore();
        DataManagement<TestIndex> sdkStore = mockStore();
        when(sdkStore.doesConfigIndexExist()).thenReturn(true);

        PlainActionFuture<CreateIndexResponse> createIndexFuture = PlainActionFuture.newFuture();
        PlainActionFuture<String> validationFuture = PlainActionFuture.newFuture();
        ExecutorFunction function = () -> {};
        List<String> roles = List.of("role-a");

        DelegatingDataManagement<TestIndex> dataManagement = newDataManagement(true, indexStore, sdkStore);

        assertTrue(dataManagement.doesConfigIndexExist());
        dataManagement.initCustomResultIndexDirectly("custom-result", createIndexFuture, "tenant-a", null);
        dataManagement.validateDefaultResultIndexForBackendJob("config-id", "user", roles, function, validationFuture);
        dataManagement
            .validateCustomIndexForBackendJob(
                "custom-result",
                "config-id",
                "user",
                roles,
                function,
                validationFuture,
                "tenant-a",
                "data-source-a"
            );

        verify(sdkStore).doesConfigIndexExist();
        verify(sdkStore).initCustomResultIndexDirectly("custom-result", createIndexFuture, "tenant-a", null);
        verify(sdkStore).validateDefaultResultIndexForBackendJob("config-id", "user", roles, function, validationFuture);
        verify(sdkStore)
            .validateCustomIndexForBackendJob(
                "custom-result",
                "config-id",
                "user",
                roles,
                function,
                validationFuture,
                "tenant-a",
                "data-source-a"
            );
        verifyNoInteractions(indexStore);
    }

    public void testDataManagementDataSourceDefaultsDelegateToTenantOverloads() {
        DataManagement<TestIndex> store = mockStoreWithRealDefaultMethods();
        ActionListener<Boolean> booleanListener = mock(ActionListener.class);
        PlainActionFuture<CreateIndexResponse> createIndexFuture = PlainActionFuture.newFuture();
        PlainActionFuture<String> validationFuture = PlainActionFuture.newFuture();
        ExecutorFunction function = () -> {};
        List<String> roles = List.of("role-a");

        store.doesResultIndexExists("result-index", booleanListener, "tenant-a", "data-source-a");
        store.doesResultAliasExists("result-alias", booleanListener, "tenant-a", "data-source-a");
        store.validateResultIndexAndExecute("result-index", function, true, validationFuture, "tenant-a", "data-source-a");
        store.initCustomResultIndexAndExecute("custom-result", function, validationFuture, "tenant-a", "data-source-a");
        store.initFlattenedResultIndex("flattened-result", createIndexFuture, "tenant-a", "data-source-a");
        store
            .validateCustomIndexForBackendJob(
                "custom-result",
                "config-id",
                "user",
                roles,
                function,
                validationFuture,
                "tenant-a",
                "data-source-a"
            );

        verify(store).doesResultIndexExists("result-index", booleanListener, "tenant-a");
        verify(store).doesResultAliasExists("result-alias", booleanListener, "tenant-a");
        verify(store).validateResultIndexAndExecute("result-index", function, true, validationFuture, "tenant-a");
        verify(store).initCustomResultIndexAndExecute("custom-result", function, validationFuture, "tenant-a");
        verify(store).initFlattenedResultIndex("flattened-result", createIndexFuture, "tenant-a");
        verify(store).validateCustomIndexForBackendJob("custom-result", "config-id", "user", roles, function, validationFuture, "tenant-a");
    }

    public void testSdkDelegateHandlesIndexLifecycleMethodsWhenMultiTenancyEnabled() {
        DataManagement<TestIndex> indexStore = mockStore();
        DataManagement<TestIndex> sdkStore = mockStore();
        ActionListener<Boolean> booleanListener = mock(ActionListener.class);
        PlainActionFuture<CreateIndexResponse> createIndexFuture = PlainActionFuture.newFuture();
        PlainActionFuture<String> validationFuture = PlainActionFuture.newFuture();
        ExecutorFunction function = () -> {};

        when(sdkStore.doesStateIndexExist()).thenReturn(true);
        when(sdkStore.doesDefaultResultIndexExist()).thenReturn(true);
        when(sdkStore.doesCheckpointIndexExist()).thenReturn(true);
        when(sdkStore.doesJobIndexExist()).thenReturn(true);
        when(sdkStore.getSchemaVersion(TestIndex.CONFIG)).thenReturn(2);

        DelegatingDataManagement<TestIndex> dataManagement = newDataManagement(true, indexStore, sdkStore);

        assertTrue(dataManagement.doesStateIndexExist());
        dataManagement.initStateIndex(createIndexFuture);
        dataManagement.initConfigIndex(createIndexFuture);
        dataManagement.doesResultIndexExists("result-index", booleanListener, "tenant-a");
        dataManagement.doesResultIndexExists("result-index", booleanListener, "tenant-a", "data-source-a");
        assertTrue(dataManagement.doesDefaultResultIndexExist());
        dataManagement.initDefaultResultIndexDirectly(createIndexFuture);
        dataManagement.doesResultAliasExists("result-alias", booleanListener, "tenant-a");
        dataManagement.doesResultAliasExists("result-alias", booleanListener, "tenant-a", "data-source-a");
        dataManagement.doesResultIndexOrAliasExists("result-index", booleanListener, "tenant-a", "data-source-a");
        assertEquals(2, dataManagement.getSchemaVersion(TestIndex.CONFIG));
        dataManagement.validateResultIndexAndExecute("result-index", function, true, validationFuture, "tenant-a");
        dataManagement.validateResultIndexAndExecute("result-index", function, true, validationFuture, "tenant-a", "data-source-a");
        dataManagement.initCustomResultIndexAndExecute("custom-result", function, validationFuture, "tenant-a");
        dataManagement.initCustomResultIndexAndExecute("custom-result", function, validationFuture, "tenant-a", "data-source-a");
        dataManagement.validateResultIndexMapping("result-index", booleanListener, "tenant-a", "data-source-a");
        dataManagement.initFlattenedResultIndex("flattened-result", createIndexFuture, "tenant-a");
        dataManagement.initFlattenedResultIndex("flattened-result", createIndexFuture, "tenant-a", "data-source-a");
        assertTrue(dataManagement.doesCheckpointIndexExist());
        dataManagement.initCheckpointIndex(createIndexFuture);
        assertTrue(dataManagement.doesJobIndexExist());
        dataManagement.initJobIndex(createIndexFuture);
        dataManagement.update("tenant-a");

        verify(sdkStore).doesStateIndexExist();
        verify(sdkStore).initStateIndex(createIndexFuture);
        verify(sdkStore).initConfigIndex(createIndexFuture);
        verify(sdkStore).doesResultIndexExists("result-index", booleanListener, "tenant-a");
        verify(sdkStore).doesResultIndexExists("result-index", booleanListener, "tenant-a", "data-source-a");
        verify(sdkStore).doesDefaultResultIndexExist();
        verify(sdkStore).initDefaultResultIndexDirectly(createIndexFuture);
        verify(sdkStore).doesResultAliasExists("result-alias", booleanListener, "tenant-a");
        verify(sdkStore).doesResultAliasExists("result-alias", booleanListener, "tenant-a", "data-source-a");
        verify(sdkStore).doesResultIndexOrAliasExists("result-index", booleanListener, "tenant-a", "data-source-a");
        verify(sdkStore).getSchemaVersion(TestIndex.CONFIG);
        verify(sdkStore).validateResultIndexAndExecute("result-index", function, true, validationFuture, "tenant-a");
        verify(sdkStore).validateResultIndexAndExecute("result-index", function, true, validationFuture, "tenant-a", "data-source-a");
        verify(sdkStore).initCustomResultIndexAndExecute("custom-result", function, validationFuture, "tenant-a");
        verify(sdkStore).initCustomResultIndexAndExecute("custom-result", function, validationFuture, "tenant-a", "data-source-a");
        verify(sdkStore).validateResultIndexMapping("result-index", booleanListener, "tenant-a", "data-source-a");
        verify(sdkStore).initFlattenedResultIndex("flattened-result", createIndexFuture, "tenant-a");
        verify(sdkStore).initFlattenedResultIndex("flattened-result", createIndexFuture, "tenant-a", "data-source-a");
        verify(sdkStore).doesCheckpointIndexExist();
        verify(sdkStore).initCheckpointIndex(createIndexFuture);
        verify(sdkStore).doesJobIndexExist();
        verify(sdkStore).initJobIndex(createIndexFuture);
        verify(sdkStore).update("tenant-a");
        verifyNoInteractions(indexStore);
    }

    public void testThrowsWhenMultiTenancyEnabledWithoutSdkStore() {
        DataManagement<TestIndex> indexStore = mockStore();

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> newDataManagement(true, indexStore, null));
        assertEquals("Multi-tenancy is enabled but no SDK-backed config store is configured.", exception.getMessage());
    }

    @SuppressWarnings("unchecked")
    private DataManagement<TestIndex> mockStore() {
        return mock(DataManagement.class);
    }

    @SuppressWarnings("unchecked")
    private DataManagement<TestIndex> mockStoreWithRealDefaultMethods() {
        return mock(DataManagement.class, CALLS_REAL_METHODS);
    }

    private DelegatingDataManagement<TestIndex> newDataManagement(
        boolean multiTenancyEnabled,
        DataManagement<TestIndex> indexStore,
        DataManagement<TestIndex> sdkStore
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(MULTI_TENANCY_ENABLED.getKey(), multiTenancyEnabled).build();
        when(clusterService.getSettings()).thenReturn(settings);
        return new DelegatingDataManagement<>(indexStore, sdkStore, clusterService, MULTI_TENANCY_ENABLED);
    }

}
