/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.bulk.BulkAction;
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
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.util.CrossClusterConfigUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

/**
 * Default data access backed by the transport client.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant; org.opensearch.cluster.service.ClusterService#state usage: Only meant to be used in single-tenant.")
public class DefaultDataAccess implements DataAccess {
    private final Client client;
    private final ClusterService clusterService;
    private final SecurityClientUtil clientUtil;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public DefaultDataAccess(
        Client client,
        ClusterService clusterService,
        SecurityClientUtil clientUtil,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.clusterService = Objects.requireNonNull(clusterService, "clusterService must not be null");
        this.clientUtil = Objects.requireNonNull(clientUtil, "clientUtil must not be null");
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver, "indexNameExpressionResolver must not be null");
    }

    @Override
    public void search(SearchRequest request, TenantContext tenantContext, ActionListener<SearchResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        searchWithInjectedSecurity(request, (User) null, tenantContext, null, listener);
    }

    @Override
    public void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener) {
        client.admin().cluster().nodesInfo(request, listener);
    }

    @Override
    public void searchWithInjectedSecurity(
        SearchRequest request,
        User user,
        TenantContext tenantContext,
        AnalysisType context,
        ActionListener<SearchResponse> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        String tenantId = tenantContext.getTenantId();
        if (user != null && context != null) {
            clientUtil.asyncRequestWithInjectedSecurity(request, client::search, user, tenantId, client, context, listener);
        } else {
            client.search(request, listener);
        }
    }

    @Override
    public void searchWithInjectedSecurity(
        SearchRequest request,
        String configId,
        TenantContext tenantContext,
        AnalysisType context,
        ActionListener<SearchResponse> listener
    ) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        String tenantId = tenantContext.getTenantId();
        clientUtil.asyncRequestWithInjectedSecurity(request, client::search, configId, tenantId, client, context, listener);
    }

    @Override
    public void updateByQuery(UpdateByQueryRequest request, TenantContext tenantContext, ActionListener<BulkByScrollResponse> listener) {
        client.execute(UpdateByQueryAction.INSTANCE, request, listener);
    }

    @Override
    public void update(UpdateRequest request, TenantContext tenantContext, ActionListener<UpdateResponse> listener) {
        client.update(request, listener);
    }

    @Override
    public void delete(DeleteRequest request, TenantContext tenantContext, ActionListener<DeleteResponse> listener) {
        client.delete(request, listener);
    }

    @Override
    public void index(IndexRequest request, TenantContext tenantContext, ActionListener<IndexResponse> listener) {
        client.index(request, listener);
    }

    @Override
    public void bulk(BulkRequest request, TenantContext tenantContext, ActionListener<BulkResponse> listener) {
        Objects.requireNonNull(tenantContext, "tenantContext must not be null");
        // Transport client is tenant-agnostic; honor provided context for API symmetry.
        client.execute(BulkAction.INSTANCE, request, listener);
    }

    @Override
    public void deleteByQuery(DeleteByQueryRequest request, TenantContext tenantContext, ActionListener<BulkByScrollResponse> listener) {
        client.execute(DeleteByQueryAction.INSTANCE, request, listener);
    }

    @Override
    public void get(GetRequest request, TenantContext tenantContext, ActionListener<GetResponse> listener) {
        client.get(request, listener);
    }

    @Override
    public void getFieldMappings(
        GetFieldMappingsRequest getMappingsRequestForIndex,
        User user,
        TenantContext tenantContext,
        String clusterName,
        AnalysisType context,
        ActionListener<GetFieldMappingsResponse> getMappingResponseListener
    ) {
        Client targetClusterClient = CrossClusterConfigUtils.getClientForCluster(clusterName, client, clusterService);
        clientUtil
            .executeWithInjectedSecurity(
                GetFieldMappingsAction.INSTANCE,
                getMappingsRequestForIndex,
                user,
                // DefaultDataAccess is used for single tenant, so tenantId is null
                null,
                targetClusterClient,
                context,
                getMappingResponseListener
            );
    }

    @Override
    public void putPipeline(
        PutPipelineRequest request,
        User user,
        TenantContext tenantContext,
        ActionListener<AcknowledgedResponse> listener
    ) {
        client.admin().cluster().putPipeline(request, listener);
    }

    @Override
    public void deletePipeline(
        DeletePipelineRequest request,
        User user,
        TenantContext tenantContext,
        ActionListener<AcknowledgedResponse> listener
    ) {
        client.admin().cluster().deletePipeline(request, listener);
    }

    @Override
    public void updateSettings(
        UpdateSettingsRequest request,
        User user,
        TenantContext tenantContext,
        ActionListener<AcknowledgedResponse> listener
    ) {
        client.admin().indices().updateSettings(request, listener);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, TenantContext tenantContext, ActionListener<MultiSearchResponse> listener) {
        client.multiSearch(request, listener);
    }

    @Override
    public void concreteIndexNames(
        IndicesOptions indicesOptions,
        String[] indexExpressions,
        TenantContext tenantContext,
        ActionListener<String[]> listener
    ) {
        try {
            String[] concreteIndices = indexNameExpressionResolver
                .concreteIndexNames(clusterService.state(), indicesOptions, indexExpressions);
            listener.onResponse(concreteIndices);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
