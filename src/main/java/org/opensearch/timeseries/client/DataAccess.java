/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.timeseries.AnalysisType;

/**
 * Abstraction for CRUD operations. Implementations can use the
 * transport client directly or delegate to the remote metadata SDK client
 * when multi-tenancy is enabled.
 */
public interface DataAccess {

    /**
     * Execute a search request with explicit tenant context.
     *
     * @param request search request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the search response
     */
    void search(SearchRequest request, TenantContext tenantContext, ActionListener<SearchResponse> listener);

    /**
     * Retrieve node info from the cluster.
     *
     * @param request nodes info request
     * @param listener listener receiving the nodes info response
     */
    void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);

    /**
     * Execute a search request with optional user security injection.
     *
     * @param request search request
     * @param user user performing the action; if null, executes without injecting security headers
     * @param tenantContext tenant scoping (required)
     * @param context analysis context for logging/security
     * @param listener listener receiving the search response
     */
    void searchWithInjectedSecurity(
        SearchRequest request,
        User user,
        TenantContext tenantContext,
        AnalysisType context,
        ActionListener<SearchResponse> listener
    );

    /**
     * Execute a search request with security injection based on a config id.
     *
     * @param request search request
     * @param configId config id whose stored user should be injected; if null, executes without injecting security headers
     * @param tenantContext tenant scoping (required)
     * @param context analysis context for logging/security
     * @param listener listener receiving the search response
     */
    void searchWithInjectedSecurity(
        SearchRequest request,
        String configId,
        TenantContext tenantContext,
        AnalysisType context,
        ActionListener<SearchResponse> listener
    );

    /**
     * Execute an update-by-query style operation for task documents.
     *
     * @param request update by query request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the bulk by scroll response
     */
    void updateByQuery(UpdateByQueryRequest request, TenantContext tenantContext, ActionListener<BulkByScrollResponse> listener);

    /**
     * Execute a delete-by-query request for task documents.
     *
     * @param request delete by query request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the bulk by scroll response
     */
    void deleteByQuery(DeleteByQueryRequest request, TenantContext tenantContext, ActionListener<BulkByScrollResponse> listener);

    /**
     * Execute an update request for a single task document.
     *
     * @param request update request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the update response
     */
    void update(UpdateRequest request, TenantContext tenantContext, ActionListener<UpdateResponse> listener);

    /**
     * Execute a delete request for a single task document.
     *
     * @param request delete request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the delete response
     */
    void delete(DeleteRequest request, TenantContext tenantContext, ActionListener<DeleteResponse> listener);

    /**
     * Index a task document.
     *
     * @param request index request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the index response
     */
    void index(IndexRequest request, TenantContext tenantContext, ActionListener<IndexResponse> listener);

    /**
     * Execute a bulk request using an explicit tenant context.
     *
     * @param request bulk request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the bulk response
     */
    void bulk(BulkRequest request, TenantContext tenantContext, ActionListener<BulkResponse> listener);

    /**
     * Execute a get request for a single task document.
     *
     * @param request get request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the get response
     */
    void get(GetRequest request, TenantContext tenantContext, ActionListener<GetResponse> listener);

    /**
     * Execute a get field mappings request.
     *
     * @param request get field mappings request
     * @param user user context
     * @param tenantContext tenant context
     * @param clusterName cluster name
     * @param context analysis type context
     * @param listener listener receiving the normalized field mappings view
     */
    void getFieldMappings(
        GetFieldMappingsRequest request,
        User user,
        TenantContext tenantContext,
        String clusterName,
        AnalysisType context,
        ActionListener<FieldMappingsView> listener
    );

    /**
     * Create or update an ingest pipeline.
     *
     * @param request put pipeline request
     * @param user user performing the action
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the acknowledgment response
     */
    void putPipeline(PutPipelineRequest request, User user, TenantContext tenantContext, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete an ingest pipeline.
     *
     * @param request delete pipeline request
     * @param user user performing the action
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the acknowledgment response
     */
    void deletePipeline(
        DeletePipelineRequest request,
        User user,
        TenantContext tenantContext,
        ActionListener<AcknowledgedResponse> listener
    );

    /**
     * Update index settings.
     *
     * @param request update settings request
     * @param user user performing the action
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the acknowledgment response
     */
    void updateSettings(
        UpdateSettingsRequest request,
        User user,
        TenantContext tenantContext,
        ActionListener<AcknowledgedResponse> listener
    );

    /**
     * Execute a multi-search request.
     *
     * @param request multi-search request
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the multi-search response
     */
    void multiSearch(MultiSearchRequest request, TenantContext tenantContext, ActionListener<MultiSearchResponse> listener);

    /**
     * Resolve index expressions (including wildcards and aliases) to concrete index names.
     *
     * @param indicesOptions options for resolving indices (e.g., ignore unavailable, allow no indices)
     * @param indexExpressions index expressions to resolve (can include wildcards like "logs-*")
     * @param tenantContext tenant scoping (required)
     * @param listener listener receiving the array of concrete index names
     */
    void concreteIndexNames(
        IndicesOptions indicesOptions,
        String[] indexExpressions,
        TenantContext tenantContext,
        ActionListener<String[]> listener
    );
}
