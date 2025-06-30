/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store;

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
        dataManagement.initCustomResultIndexDirectly("custom-result", createIndexFuture, "tenant-a");
        dataManagement.validateDefaultResultIndexForBackendJob("config-id", "user", roles, function, validationFuture);

        verify(sdkStore).doesConfigIndexExist();
        verify(sdkStore).initCustomResultIndexDirectly("custom-result", createIndexFuture, "tenant-a");
        verify(sdkStore).validateDefaultResultIndexForBackendJob("config-id", "user", roles, function, validationFuture);
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
