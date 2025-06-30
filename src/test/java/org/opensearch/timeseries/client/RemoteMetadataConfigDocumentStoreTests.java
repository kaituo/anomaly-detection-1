/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;

public class RemoteMetadataConfigDocumentStoreTests extends OpenSearchTestCase {
    private static final String TENANT_ID = "tenant-1";

    private SdkClient sdkClient;
    private RemoteMetadataConfigDocumentStore store;

    @Before
    public void setUpStore() {
        sdkClient = mock(SdkClient.class);
        store = new RemoteMetadataConfigDocumentStore(sdkClient);
    }

    public void testSearchUsesSdkClientAndCreatesSourceBuilderWhenMissing() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchDataObjectResponse sdkResponse = mock(SearchDataObjectResponse.class);
        when(sdkResponse.searchResponse()).thenReturn(searchResponse);
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(sdkResponse));

        SearchRequest request = new SearchRequest(ADCommonName.CONFIG_INDEX);
        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        store.search(request, TenantContext.user(TENANT_ID), future);

        assertSame(searchResponse, future.actionGet());

        ArgumentCaptor<SearchDataObjectRequest> requestCaptor = ArgumentCaptor.forClass(SearchDataObjectRequest.class);
        verify(sdkClient).searchDataObjectAsync(requestCaptor.capture());
        assertArrayEquals(new String[] { ADCommonName.CONFIG_INDEX }, requestCaptor.getValue().indices());
        assertEquals(TENANT_ID, requestCaptor.getValue().tenantId());
        assertNotNull(request.source());
        assertSame(request.source(), requestCaptor.getValue().searchSourceBuilder());
    }

    public void testSearchReturnsEmptyResponseOnEmptySdkParseFailure() {
        SearchDataObjectResponse sdkResponse = mock(SearchDataObjectResponse.class);
        when(sdkResponse.searchResponse()).thenThrow(new AssertionError("malformed empty search response total: 0"));
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(sdkResponse));

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        store.search(new SearchRequest(ADCommonName.CONFIG_INDEX), TenantContext.user(TENANT_ID), future);

        SearchResponse response = future.actionGet();
        assertNotNull(response);
        assertNotNull(response.getHits());
        assertEquals(0, response.getHits().getHits().length);
    }

    public void testSearchPropagatesNonEmptySdkParseFailure() {
        SearchDataObjectResponse sdkResponse = mock(SearchDataObjectResponse.class);
        when(sdkResponse.searchResponse()).thenThrow(new AssertionError("malformed non-empty search response total: 10"));
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(sdkResponse));

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        store
            .search(
                new SearchRequest(ADCommonName.CONFIG_INDEX).source(new SearchSourceBuilder().size(1)),
                TenantContext.user(TENANT_ID),
                future
            );

        OpenSearchStatusException exception = expectThrows(OpenSearchStatusException.class, future::actionGet);
        assertTrue(exception.getMessage().contains("Failed to parse search response"));
    }

    public void testSearchPropagatesSdkFailure() {
        RuntimeException failure = new RuntimeException("boom");
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class))).thenReturn(CompletableFuture.failedFuture(failure));

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        store.search(new SearchRequest(ADCommonName.CONFIG_INDEX), TenantContext.user(TENANT_ID), future);

        RuntimeException exception = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(exception.getMessage().contains("boom"));
    }

    public void testSearchReturnsEmptyResponseWhenIndexIsMissing() {
        RuntimeException failure = new RuntimeException(new IndexNotFoundException(ADCommonName.CONFIG_INDEX));
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class))).thenReturn(CompletableFuture.failedFuture(failure));

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        store.search(new SearchRequest(ADCommonName.CONFIG_INDEX), TenantContext.user(TENANT_ID), future);

        SearchResponse response = future.actionGet();
        assertNotNull(response);
        assertNotNull(response.getHits());
        assertEquals(0, response.getHits().getHits().length);
    }

    public void testIndexAddsTenantIdFieldToStoredDocument() {
        when(sdkClient.putDataObjectAsync(any(PutDataObjectRequest.class))).thenReturn(
            CompletableFuture.completedFuture(new org.opensearch.remote.metadata.client.PutDataObjectResponse(new IndexResponse(
                new ShardId(ADCommonName.CONFIG_INDEX, "_na_", 0),
                "config-id",
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                1,
                true
            )))
        );

        IndexRequest request = new IndexRequest(ADCommonName.CONFIG_INDEX).id("config-id").source("name", "detector");
        PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
        store.index(request, TenantContext.user(TENANT_ID), future);

        assertEquals("config-id", future.actionGet().getId());

        ArgumentCaptor<PutDataObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutDataObjectRequest.class);
        verify(sdkClient).putDataObjectAsync(requestCaptor.capture());
        assertEquals(TENANT_ID, requestCaptor.getValue().tenantId());
        assertEquals(TENANT_ID, requestAsMap(requestCaptor.getValue().dataObject()).get(CommonName.TENANT_ID_FIELD));
        assertEquals("detector", requestAsMap(requestCaptor.getValue().dataObject()).get("name"));
    }

    public void testUpdateAddsTenantIdFieldToStoredDocument() {
        UpdateResponse updateResponse = new UpdateResponse(
            new ShardId(ADCommonName.CONFIG_INDEX, "_na_", 0),
            "config-id",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            1,
            DocWriteResponse.Result.UPDATED
        );
        when(sdkClient.updateDataObjectAsync(any(UpdateDataObjectRequest.class)))
            .thenReturn(
                CompletableFuture.completedFuture(new org.opensearch.remote.metadata.client.UpdateDataObjectResponse(updateResponse))
            );

        UpdateRequest request = new UpdateRequest(ADCommonName.CONFIG_INDEX, "config-id").doc("name", "updated");
        PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
        store.update(request, TenantContext.user(TENANT_ID), future);

        assertEquals("config-id", future.actionGet().getId());

        ArgumentCaptor<UpdateDataObjectRequest> requestCaptor = ArgumentCaptor.forClass(UpdateDataObjectRequest.class);
        verify(sdkClient).updateDataObjectAsync(requestCaptor.capture());
        assertEquals(TENANT_ID, requestCaptor.getValue().tenantId());
        assertEquals(TENANT_ID, requestAsMap(requestCaptor.getValue().dataObject()).get(CommonName.TENANT_ID_FIELD));
        assertEquals("updated", requestAsMap(requestCaptor.getValue().dataObject()).get("name"));
    }

    private Map<String, Object> requestAsMap(ToXContentObject dataObject) {
        try {
            return TestHelpers.XContentBuilderToMap(dataObject.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS));
        } catch (Exception e) {
            throw new AssertionError("Failed to serialize SDK request payload", e);
        }
    }
}
