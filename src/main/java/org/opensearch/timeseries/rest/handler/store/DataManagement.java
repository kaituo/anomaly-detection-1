/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler.store;

import java.util.List;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.TimeSeriesIndex;

/**
 * Abstraction for persisting and querying configuration resources.
 * Implementations can back the operations by OpenSearch indices,
 * remote metadata services, or any other storage mechanism.
 */
public interface DataManagement<IndexType extends Enum<IndexType> & TimeSeriesIndex> {

    /**
     * Check if the config index exists.
     *
     * @return true if the config index exists
     */
    boolean doesConfigIndexExist();

    void initConfigIndex(ActionListener<CreateIndexResponse> listener);

    boolean doesStateIndexExist();

    void initStateIndex(ActionListener<CreateIndexResponse> listener);

    void doesResultIndexExists(String indexName, ActionListener<Boolean> listener, String tenantId);

    default void doesResultIndexExists(String indexName, ActionListener<Boolean> listener, String tenantId, String dataSourceId) {
        doesResultIndexExists(indexName, listener, tenantId);
    }

    void doesResultAliasExists(String aliasName, ActionListener<Boolean> listener, String tenantId);

    default void doesResultAliasExists(String aliasName, ActionListener<Boolean> listener, String tenantId, String dataSourceId) {
        doesResultAliasExists(aliasName, listener, tenantId);
    }

    void doesResultIndexOrAliasExists(String indexOrAliasName, ActionListener<Boolean> listener, String tenantId, String dataSourceId);

    /**
     * no default result index in multi-tenancy environment since it is impossible to create a system index from external service.
     * Instead, we use custom result index in multi-tenancy environment.
     * 
     * @return true if default result index exists
     */
    boolean doesDefaultResultIndexExist();

    void initDefaultResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener);

    int getSchemaVersion(IndexType index);

    <T> void validateResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        boolean mappingValidated,
        ActionListener<T> listener,
        String tenantId
    );

    default <T> void validateResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        boolean mappingValidated,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId
    ) {
        validateResultIndexAndExecute(resultIndexOrAlias, function, mappingValidated, listener, tenantId);
    }

    <T> void initCustomResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId
    );

    default <T> void initCustomResultIndexAndExecute(
        String resultIndexOrAlias,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId
    ) {
        initCustomResultIndexAndExecute(resultIndexOrAlias, function, listener, tenantId);
    }

    void validateResultIndexMapping(String resultIndexOrAlias, ActionListener<Boolean> thenDo, String tenantId, String dataSourceId);

    void initCustomResultIndexDirectly(
        String resultIndex,
        ActionListener<CreateIndexResponse> actionListener,
        String tenantId,
        String dataSourceId
    );

    void initFlattenedResultIndex(String flattenedResultIndexAlias, ActionListener<CreateIndexResponse> actionListener, String tenantId);

    default void initFlattenedResultIndex(
        String flattenedResultIndexAlias,
        ActionListener<CreateIndexResponse> actionListener,
        String tenantId,
        String dataSourceId
    ) {
        initFlattenedResultIndex(flattenedResultIndexAlias, actionListener, tenantId);
    }

    boolean doesCheckpointIndexExist();

    void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener);

    <T> void validateDefaultResultIndexForBackendJob(
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener
    );

    <T> void validateCustomIndexForBackendJob(
        String resultIndexOrAlias,
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId
    );

    default <T> void validateCustomIndexForBackendJob(
        String resultIndexOrAlias,
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener,
        String tenantId,
        String dataSourceId
    ) {
        validateCustomIndexForBackendJob(resultIndexOrAlias, configId, user, roles, function, listener, tenantId);
    }

    boolean doesJobIndexExist();

    void initJobIndex(ActionListener<CreateIndexResponse> actionListener);

    void update(String tenantId);
}
