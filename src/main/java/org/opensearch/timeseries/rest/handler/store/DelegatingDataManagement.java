/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.TimeSeriesIndex;

/**
 * Config store that delegates to either an index-backed store or SDK-backed store,
 * depending on whether multi-tenancy is enabled and an SDK store is available.
 *
 * @param <IndexType> the time series index enum type
 */
public class DelegatingDataManagement<IndexType extends Enum<IndexType> & TimeSeriesIndex> implements DataManagement<IndexType> {
    private static final Logger LOG = LogManager.getLogger(DelegatingDataManagement.class);

    private final DataManagement<IndexType> indexStore;
    private final DataManagement<IndexType> sdkStore;
    private final AtomicReference<DataManagement<IndexType>> current;
    private final AtomicBoolean missingSdkWarningLogged;
    private final boolean multiTenancyEnabled;

    public DelegatingDataManagement(
        DataManagement<IndexType> indexStore,
        DataManagement<IndexType> sdkStore,
        ClusterService clusterService,
        Setting<Boolean> multiTenancyEnabled
    ) {
        this.indexStore = Objects.requireNonNull(indexStore, "indexStore must not be null");
        this.sdkStore = sdkStore;
        this.multiTenancyEnabled = multiTenancyEnabled.get(clusterService.getSettings());
        this.current = new AtomicReference<>(delegateStore());
        this.missingSdkWarningLogged = new AtomicBoolean(false);
    }

    private DataManagement<IndexType> delegateStore() {
        if (multiTenancyEnabled) {
            if (sdkStore != null) {
                return sdkStore;
            }
            if (missingSdkWarningLogged.compareAndSet(false, true)) {
                LOG.warn("Multi-tenancy is enabled but no SDK-backed config store is configured; falling back to index-backed store.");
            }
        }
        return indexStore;
    }

    private DataManagement<IndexType> delegate() {
        return current.get();
    }

    @Override
    public boolean doesConfigIndexExist() {
        return delegate().doesConfigIndexExist();
    }

    @Override
    public boolean doesStateIndexExist() {
        return delegate().doesStateIndexExist();
    }

    @Override
    public void initStateIndex(ActionListener<CreateIndexResponse> listener) {
        delegate().initStateIndex(listener);
    }

    @Override
    public void initConfigIndex(ActionListener<CreateIndexResponse> listener) {
        delegate().initConfigIndex(listener);
    }

    @Override
    public boolean doesResultIndexExists(String indexName, String tenantId) {
        return delegate().doesResultIndexExists(indexName, tenantId);
    }

    @Override
    public boolean doesDefaultResultIndexExist() {
        return delegate().doesDefaultResultIndexExist();
    }

    @Override
    public void initDefaultResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) {
        delegate().initDefaultResultIndexDirectly(actionListener);
    }

    @Override
    public boolean doesResultAliasExists(String aliasName, String tenantId) {
        return delegate().doesResultAliasExists(aliasName, tenantId);
    }

    @Override
    public int getSchemaVersion(IndexType index) {
        return delegate().getSchemaVersion(index);
    }

    @Override
    public <T> void validateResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        boolean mappingValidated,
        ActionListener<T> listener,
        String tenantId
    ) {
        delegate().validateResultIndexAndExecute(resultIndexOrAlias, function, mappingValidated, listener, tenantId);
    }

    @Override
    public <T> void initCustomResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId
    ) {
        delegate().initCustomResultIndexAndExecute(resultIndexOrAlias, function, listener, tenantId);
    }

    @Override
    public void validateResultIndexMapping(String resultIndexOrAlias, ActionListener<Boolean> thenDo, String tenantId) {
        delegate().validateResultIndexMapping(resultIndexOrAlias, thenDo, tenantId);
    }

    @Override
    public void initCustomResultIndexDirectly(String resultIndex, ActionListener<CreateIndexResponse> actionListener, String tenantId) {
        delegate().initCustomResultIndexDirectly(resultIndex, actionListener, tenantId);
    }

    @Override
    public void initFlattenedResultIndex(
        String flattenedResultIndexAlias,
        ActionListener<CreateIndexResponse> actionListener,
        String tenantId
    ) {
        delegate().initFlattenedResultIndex(flattenedResultIndexAlias, actionListener, tenantId);
    }

    @Override
    public <T> void validateDefaultResultIndexForBackendJob(
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        delegate().validateDefaultResultIndexForBackendJob(configId, user, roles, function, listener);
    }

    @Override
    public boolean doesCheckpointIndexExist() {
        return delegate().doesCheckpointIndexExist();
    }

    @Override
    public void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener) {
        delegate().initCheckpointIndex(actionListener);
    }

    @Override
    public <T> void validateCustomIndexForBackendJob(
        String resultIndexOrAlias,
        String securityLogId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId
    ) {
        delegate().validateCustomIndexForBackendJob(resultIndexOrAlias, securityLogId, user, roles, function, listener, tenantId);
    }

    @Override
    public boolean doesJobIndexExist() {
        return delegate().doesJobIndexExist();
    }

    @Override
    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) {
        delegate().initJobIndex(actionListener);
    }

    @Override
    public void update(String tenantId) {
        delegate().update(tenantId);
    }
}
