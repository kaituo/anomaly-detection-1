/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.apache.lucene.search.TotalHits;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.remote.metadata.client.BulkDataObjectRequest;
import org.opensearch.remote.metadata.client.BulkDataObjectResponse;
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest;
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest;
import org.opensearch.remote.metadata.client.UpdateDataObjectResponse;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.SdkStateManager;
import org.opensearch.timeseries.constant.CommonName;

public class SdkDataAccessTests extends AbstractTimeSeriesTest {
    private static final String TENANT_ID = "tenant-1";
    private static final String CONFIG_ID = "config-1";

    private SdkClient sdkClient;
    private ClusterService clusterService;
    private SdkStateManager stateManager;
    private ConfigDocumentStore configDocumentStore;
    private TestRestServer restServer;
    private String restEndpoint;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        sdkClient = mock(SdkClient.class);
        clusterService = mock(ClusterService.class);
        stateManager = mock(SdkStateManager.class);
        configDocumentStore = mock(ConfigDocumentStore.class);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (restServer != null) {
            restServer.close();
            restServer = null;
        }
        if (restEndpoint != null) {
            RestClientProvider.getRestClient(restEndpoint).close();
            restEndpoint = null;
        }
        super.tearDown();
    }

    public void testGetConfigUsesConfigStore() {
        GetResponse getResponse = mock(GetResponse.class);
        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(configDocumentStore).get(any(GetRequest.class), any(TenantContext.class), any());

        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        GetRequest request = new GetRequest(ADCommonName.CONFIG_INDEX, CONFIG_ID);
        dataAccess().get(request, TenantContext.user(TENANT_ID), future);

        assertSame(getResponse, future.actionGet());
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        ArgumentCaptor<TenantContext> tenantCaptor = ArgumentCaptor.forClass(TenantContext.class);
        verify(configDocumentStore).get(requestCaptor.capture(), tenantCaptor.capture(), any());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(CONFIG_ID, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, tenantCaptor.getValue().getTenantId());
        verifyNoInteractions(sdkClient);
    }

    public void testIndexConfigUsesConfigStore() {
        IndexResponse indexResponse = newIndexResponse(ADCommonName.CONFIG_INDEX, CONFIG_ID);
        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<IndexResponse> listener = invocation.getArgument(2);
            listener.onResponse(indexResponse);
            return null;
        }).when(configDocumentStore).index(any(IndexRequest.class), any(TenantContext.class), any());

        PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
        IndexRequest request = new IndexRequest(ADCommonName.CONFIG_INDEX).id(CONFIG_ID).source("name", "detector");
        dataAccess().index(request, TenantContext.user(TENANT_ID), future);

        IndexResponse response = future.actionGet();
        assertEquals(CONFIG_ID, response.getId());
        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        ArgumentCaptor<TenantContext> tenantCaptor = ArgumentCaptor.forClass(TenantContext.class);
        verify(configDocumentStore).index(requestCaptor.capture(), tenantCaptor.capture(), any());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(CONFIG_ID, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, tenantCaptor.getValue().getTenantId());
        verifyNoInteractions(sdkClient);
    }

    public void testUpdateConfigUsesConfigStore() {
        UpdateResponse updateResponse = new UpdateResponse(
            new ShardId(ADCommonName.CONFIG_INDEX, "uuid", 0),
            CONFIG_ID,
            1L,
            1L,
            1L,
            DocWriteResponse.Result.UPDATED
        );
        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<UpdateResponse> listener = invocation.getArgument(2);
            listener.onResponse(updateResponse);
            return null;
        }).when(configDocumentStore).update(any(UpdateRequest.class), any(TenantContext.class), any());

        PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
        UpdateRequest request = new UpdateRequest(ADCommonName.CONFIG_INDEX, CONFIG_ID).doc("name", "updated");
        dataAccess().update(request, TenantContext.user(TENANT_ID), future);

        UpdateResponse response = future.actionGet();
        assertEquals(CONFIG_ID, response.getId());
        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        ArgumentCaptor<TenantContext> tenantCaptor = ArgumentCaptor.forClass(TenantContext.class);
        verify(configDocumentStore).update(requestCaptor.capture(), tenantCaptor.capture(), any());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(CONFIG_ID, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, tenantCaptor.getValue().getTenantId());
        verifyNoInteractions(sdkClient);
    }

    public void testDeleteConfigUsesConfigStore() {
        DeleteResponse deleteResponse = newDeleteResponse(ADCommonName.CONFIG_INDEX, CONFIG_ID);
        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<DeleteResponse> listener = invocation.getArgument(2);
            listener.onResponse(deleteResponse);
            return null;
        }).when(configDocumentStore).delete(any(DeleteRequest.class), any(TenantContext.class), any());

        PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
        DeleteRequest request = new DeleteRequest(ADCommonName.CONFIG_INDEX, CONFIG_ID);
        dataAccess().delete(request, TenantContext.user(TENANT_ID), future);

        DeleteResponse response = future.actionGet();
        assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        ArgumentCaptor<TenantContext> tenantCaptor = ArgumentCaptor.forClass(TenantContext.class);
        verify(configDocumentStore).delete(requestCaptor.capture(), tenantCaptor.capture(), any());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(CONFIG_ID, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, tenantCaptor.getValue().getTenantId());
        verifyNoInteractions(sdkClient);
    }

    public void testDeleteUserIndexUsesRestPath() throws Exception {
        String body = toJson(newDeleteResponse("user-index", "doc-1"));
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(body, method, path);
        restServer.start();
        restEndpoint = restServer.endpoint();

        PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
        SdkDataAccess dataAccess = dataAccess(new org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver() {
            @Override
            public String resolve(String tenantId) {
                return restEndpoint;
            }

            @Override
            public String resolve(String applicationId, String dataSourceId) {
                return restEndpoint;
            }
        });
        dataAccess.delete(new DeleteRequest("user-index", "doc-1"), TenantContext.user(TENANT_ID), future);

        DeleteResponse response = future.actionGet();
        assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
        assertEquals("DELETE", method.get());
        assertEquals("/user-index/_doc/doc-1", path.get());
        verifyNoInteractions(sdkClient);
    }

    public void testDeleteSystemIndexUsesSdkClient() {
        DeleteResponse deleteResponse = newDeleteResponse(CommonName.JOB_INDEX, "job-1");
        when(sdkClient.deleteDataObjectAsync(any(DeleteDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(new DeleteDataObjectResponse(deleteResponse)));

        PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
        DeleteRequest request = new DeleteRequest(CommonName.JOB_INDEX, "job-1");
        dataAccess().delete(request, TenantContext.user(TENANT_ID), future);

        DeleteResponse response = future.actionGet();
        assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
        verify(sdkClient).deleteDataObjectAsync(any(DeleteDataObjectRequest.class));
    }

    public void testDeleteByQueryUsesSourceIndexForSingleSystemIndex() {
        SearchDataObjectResponse sdkSearchResponse = mockSdkSearchResponse("opendistro-anomaly-detection-state", "task-1");
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(sdkSearchResponse));

        AtomicReference<BulkDataObjectRequest> bulkRequestRef = new AtomicReference<>();
        when(sdkClient.bulkDataObjectAsync(any(BulkDataObjectRequest.class))).thenAnswer(invocation -> {
            bulkRequestRef.set(invocation.getArgument(0));
            return CompletableFuture.completedFuture(successfulBulkResponse(1));
        });

        DeleteByQueryRequest request = new DeleteByQueryRequest(ADCommonName.DETECTION_STATE_INDEX);
        request.setQuery(QueryBuilders.matchAllQuery());

        PlainActionFuture<BulkByScrollResponse> future = PlainActionFuture.newFuture();
        dataAccess().deleteByQuery(request, TenantContext.user(TENANT_ID), future);

        assertEquals(1L, future.actionGet().getDeleted());
        DeleteDataObjectRequest deleteRequest = (DeleteDataObjectRequest) bulkRequestRef.get().requests().get(0);
        assertEquals(ADCommonName.DETECTION_STATE_INDEX, deleteRequest.index());
    }

    public void testUpdateByQueryUsesSourceIndexForSingleSystemIndex() {
        SearchDataObjectResponse sdkSearchResponse = mockSdkSearchResponse("opendistro-anomaly-detection-state", "task-1");
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(sdkSearchResponse));

        AtomicReference<UpdateDataObjectRequest> updateRequestRef = new AtomicReference<>();
        UpdateResponse updateResponse = new UpdateResponse(
            new ShardId(ADCommonName.DETECTION_STATE_INDEX, "uuid", 0),
            "task-1",
            1L,
            1L,
            1L,
            DocWriteResponse.Result.UPDATED
        );
        when(sdkClient.updateDataObjectAsync(any(UpdateDataObjectRequest.class))).thenAnswer(invocation -> {
            updateRequestRef.set(invocation.getArgument(0));
            return CompletableFuture.completedFuture(new UpdateDataObjectResponse(updateResponse));
        });

        UpdateByQueryRequest request = new UpdateByQueryRequest(ADCommonName.DETECTION_STATE_INDEX);
        request.setQuery(QueryBuilders.matchAllQuery());
        request.setScript(new Script("ctx._source.latest=false;"));

        PlainActionFuture<BulkByScrollResponse> future = PlainActionFuture.newFuture();
        dataAccess().updateByQuery(request, TenantContext.user(TENANT_ID), future);

        assertEquals(1L, future.actionGet().getUpdated());
        assertEquals(ADCommonName.DETECTION_STATE_INDEX, updateRequestRef.get().index());
    }

    public void testSearchConfigUsesConfigStore() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(configDocumentStore).search(any(SearchRequest.class), any(TenantContext.class), any());

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess().search(new SearchRequest(ADCommonName.CONFIG_INDEX), TenantContext.user(TENANT_ID), future);

        assertSame(searchResponse, future.actionGet());
        verify(configDocumentStore).search(any(SearchRequest.class), any(TenantContext.class), any());
        verifyNoInteractions(sdkClient);
    }

    public void testNodesInfoReturnsOnlyReadyNodes() throws Exception {
        try (TcpAcceptServer readyServer = new TcpAcceptServer()) {
            readyServer.start();
            int unavailablePort;
            try (ServerSocket unavailableServer = new ServerSocket(0)) {
                unavailablePort = unavailableServer.getLocalPort();
            }

            PlainActionFuture<NodesInfoResponse> future = PlainActionFuture.newFuture();
            NodesInfoRequest request = new NodesInfoRequest();
            request.nodesIds("127.0.0.1:" + readyServer.port(), "127.0.0.1:" + unavailablePort);

            dataAccess().nodesInfo(request, future);

            NodesInfoResponse response = future.actionGet();
            assertEquals(1, response.getNodes().size());
            assertTrue(response.getNodesMap().containsKey("127.0.0.1:" + readyServer.port()));
            assertFalse(response.getNodesMap().containsKey("127.0.0.1:" + unavailablePort));
        }
    }

    private SdkDataAccess dataAccess() {
        return dataAccess(new org.opensearch.timeseries.rest.handler.store.endpoint.DefaultDataSourceEndpointResolver());
    }

    private SdkDataAccess dataAccess(org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver endpointResolver) {
        return new SdkDataAccess(sdkClient, clusterService, Settings.EMPTY, stateManager, configDocumentStore, endpointResolver);
    }

    private String toJson(ToXContent response) throws IOException {
        return response.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).toString();
    }

    private DeleteResponse newDeleteResponse(String index, String id) {
        DeleteResponse response = new DeleteResponse(new ShardId(index, "uuid", 0), id, 1, 1, 1, true);
        response.setShardInfo(new ShardInfo(1, 1));
        return response;
    }

    private IndexResponse newIndexResponse(String index, String id) {
        IndexResponse response = new IndexResponse(new ShardId(index, "uuid", 0), id, 1, 1, 1, true);
        response.setShardInfo(new ShardInfo(1, 1));
        return response;
    }

    private SearchDataObjectResponse mockSdkSearchResponse(String hitIndex, String hitId) {
        SearchHit hit = new SearchHit(0, hitId, null, null);
        hit.shard(new SearchShardTarget("node-0", new ShardId(hitIndex, "uuid", 0), null, OriginalIndices.NONE));

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f));

        SearchDataObjectResponse sdkSearchResponse = mock(SearchDataObjectResponse.class);
        when(sdkSearchResponse.parser()).thenReturn(mock(XContentParser.class));
        when(sdkSearchResponse.searchResponse()).thenReturn(searchResponse);
        return sdkSearchResponse;
    }

    private BulkDataObjectResponse successfulBulkResponse(int itemCount) {
        BulkResponse bulkResponse = mock(BulkResponse.class);
        BulkItemResponse[] items = new BulkItemResponse[itemCount];
        for (int i = 0; i < itemCount; i++) {
            items[i] = mock(BulkItemResponse.class);
            when(items[i].getFailure()).thenReturn(null);
        }
        when(bulkResponse.getItems()).thenReturn(items);
        when(bulkResponse.getTook()).thenReturn(TimeValue.ZERO);

        BulkDataObjectResponse sdkBulkResponse = mock(BulkDataObjectResponse.class);
        when(sdkBulkResponse.bulkResponse()).thenReturn(bulkResponse);
        return sdkBulkResponse;
    }

    private static final class TestRestServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final Thread thread;

        TestRestServer(String body, AtomicReference<String> method, AtomicReference<String> path) throws IOException {
            this.serverSocket = new ServerSocket(0);
            this.thread = new Thread(() -> serve(body, method, path), "sdk-data-access-test-server");
            this.thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        String endpoint() {
            return "http://127.0.0.1:" + serverSocket.getLocalPort();
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            thread.join(5000);
        }

        private void serve(String body, AtomicReference<String> method, AtomicReference<String> path) {
            try (Socket socket = serverSocket.accept()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                String requestLine = reader.readLine();
                assertNotNull(requestLine);
                String[] tokens = requestLine.split(" ");
                assertTrue(tokens.length >= 2);
                method.set(tokens[0]);
                path.set(tokens[1]);

                String headerLine;
                while ((headerLine = reader.readLine()) != null && headerLine.isEmpty() == false) {
                    // consume request headers
                }

                byte[] responseBytes = body.getBytes(StandardCharsets.UTF_8);
                byte[] headerBytes = ("HTTP/1.1 200 OK\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Content-Length: "
                    + responseBytes.length
                    + "\r\n"
                    + "Connection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
                OutputStream responseBody = socket.getOutputStream();
                responseBody.write(headerBytes);
                responseBody.write(responseBytes);
                responseBody.flush();
            } catch (IOException e) {
                if (serverSocket.isClosed() == false) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static final class TcpAcceptServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final Thread thread;

        private TcpAcceptServer() throws IOException {
            this.serverSocket = new ServerSocket(0);
            this.thread = new Thread(this::acceptLoop, "sdk-data-access-tcp-probe-server");
            this.thread.setDaemon(true);
        }

        private void start() {
            thread.start();
        }

        private int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            thread.join(5000);
        }

        private void acceptLoop() {
            while (serverSocket.isClosed() == false) {
                try (Socket socket = serverSocket.accept()) {
                    // Accept and close immediately. The readiness probe only needs a successful TCP connect.
                } catch (IOException e) {
                    if (serverSocket.isClosed() == false) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
