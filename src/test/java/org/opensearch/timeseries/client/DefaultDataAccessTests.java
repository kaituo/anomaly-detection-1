/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.commons.authuser.User;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class DefaultDataAccessTests extends OpenSearchTestCase {
    private Client client;
    private DefaultDataAccess dataAccess;

    @Before
    public void setUpDataAccess() {
        client = mock(Client.class);
        dataAccess = new DefaultDataAccess(
            client,
            mock(ClusterService.class),
            mock(SecurityClientUtil.class),
            mock(IndexNameExpressionResolver.class)
        );
    }

    public void testSearchConfigUsesTransportClient() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess.search(new SearchRequest(ADCommonName.CONFIG_INDEX), TenantContext.systemWide(), future);

        assertSame(searchResponse, future.actionGet());
        verify(client).search(any(SearchRequest.class), any());
    }

    public void testGetConfigUsesTransportClient() {
        GetResponse getResponse = mock(GetResponse.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        dataAccess.get(new GetRequest(ADCommonName.CONFIG_INDEX, "config-id"), TenantContext.systemWide(), future);

        assertSame(getResponse, future.actionGet());
        verify(client).get(any(GetRequest.class), any());
    }

    public void testIndexConfigUsesTransportClient() {
        IndexResponse indexResponse = mock(IndexResponse.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(indexResponse);
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
        dataAccess
            .index(
                new IndexRequest(ADCommonName.CONFIG_INDEX).id("config-id").source("name", "detector"),
                TenantContext.systemWide(),
                future
            );

        assertSame(indexResponse, future.actionGet());
        verify(client).index(any(IndexRequest.class), any());
    }

    public void testUpdateConfigUsesTransportClient() {
        UpdateResponse updateResponse = mock(UpdateResponse.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<UpdateResponse> listener = invocation.getArgument(1);
            listener.onResponse(updateResponse);
            return null;
        }).when(client).update(any(UpdateRequest.class), any());

        PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
        dataAccess
            .update(new UpdateRequest(ADCommonName.CONFIG_INDEX, "config-id").doc("name", "updated"), TenantContext.systemWide(), future);

        assertSame(updateResponse, future.actionGet());
        verify(client).update(any(UpdateRequest.class), any());
    }

    public void testDeleteConfigUsesTransportClient() {
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onResponse(deleteResponse);
            return null;
        }).when(client).delete(any(DeleteRequest.class), any());

        PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
        dataAccess.delete(new DeleteRequest(ADCommonName.CONFIG_INDEX, "config-id"), TenantContext.systemWide(), future);

        assertSame(deleteResponse, future.actionGet());
        verify(client).delete(any(DeleteRequest.class), any());
    }

    public void testSearchNonConfigUsesTransportClient() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess.search(new SearchRequest("user-index"), TenantContext.systemWide(), future);

        assertSame(searchResponse, future.actionGet());
        verify(client).search(any(SearchRequest.class), any());
    }

    public void testSearchWithInjectedSecurityConfigUsesTransportClient() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess
            .searchWithInjectedSecurity(
                new SearchRequest(ADCommonName.CONFIG_INDEX),
                (User) null,
                TenantContext.systemWide(),
                null,
                future
            );

        assertSame(searchResponse, future.actionGet());
        verify(client).search(any(SearchRequest.class), any());
    }

    public void testGetNonConfigUsesTransportClient() {
        GetResponse getResponse = mock(GetResponse.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.core.action.ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        dataAccess.get(new GetRequest("user-index", "doc-id"), TenantContext.systemWide(), future);

        assertSame(getResponse, future.actionGet());
        verify(client).get(any(GetRequest.class), any());
    }
}
