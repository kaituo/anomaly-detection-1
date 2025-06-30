/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.mockito.ArgumentMatchers.same;
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
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

public class TransportConfigDocumentStoreTests extends OpenSearchTestCase {
    private Client client;
    private TransportConfigDocumentStore store;

    @Before
    public void setUpStore() {
        client = mock(Client.class);
        store = new TransportConfigDocumentStore(client);
    }

    public void testSearchDelegatesToClient() {
        SearchRequest request = new SearchRequest("config-index");
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);

        store.search(request, TenantContext.user("tenant-1"), listener);

        verify(client).search(same(request), same(listener));
    }

    public void testGetDelegatesToClient() {
        GetRequest request = new GetRequest("config-index", "config-id");
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);

        store.get(request, TenantContext.user("tenant-1"), listener);

        verify(client).get(same(request), same(listener));
    }

    public void testIndexDelegatesToClient() {
        IndexRequest request = new IndexRequest("config-index").id("config-id").source("name", "detector");
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);

        store.index(request, TenantContext.user("tenant-1"), listener);

        verify(client).index(same(request), same(listener));
    }

    public void testUpdateDelegatesToClient() {
        UpdateRequest request = new UpdateRequest("config-index", "config-id").doc("name", "updated");
        @SuppressWarnings("unchecked")
        ActionListener<UpdateResponse> listener = mock(ActionListener.class);

        store.update(request, TenantContext.user("tenant-1"), listener);

        verify(client).update(same(request), same(listener));
    }

    public void testDeleteDelegatesToClient() {
        DeleteRequest request = new DeleteRequest("config-index", "config-id");
        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);

        store.delete(request, TenantContext.user("tenant-1"), listener);

        verify(client).delete(same(request), same(listener));
    }
}
