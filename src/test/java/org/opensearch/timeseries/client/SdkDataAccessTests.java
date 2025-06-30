/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.mockito.ArgumentMatchers.any;
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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest;
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.PutDataObjectResponse;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest;
import org.opensearch.remote.metadata.client.UpdateDataObjectResponse;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.SdkStateManager;
import org.opensearch.timeseries.constant.CommonName;

public class SdkDataAccessTests extends AbstractTimeSeriesTest {
    private static final String TENANT_ID = "tenant-1";
    private static final String CONFIG_ID = "config-1";

    private SdkClient sdkClient;
    private ClusterService clusterService;
    private SdkStateManager stateManager;
    private TestRestServer restServer;
    private String restEndpoint;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        sdkClient = mock(SdkClient.class);
        clusterService = mock(ClusterService.class);
        stateManager = mock(SdkStateManager.class);
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

    public void testGetConfigUsesSdkClient() {
        GetResponse getResponse = mock(GetResponse.class);
        GetDataObjectResponse sdkResponse = new GetDataObjectResponse(getResponse);
        when(sdkClient.getDataObjectAsync(any(GetDataObjectRequest.class))).thenReturn(CompletableFuture.completedFuture(sdkResponse));

        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        GetRequest request = new GetRequest(ADCommonName.CONFIG_INDEX, CONFIG_ID);
        dataAccess().get(request, TenantContext.user(TENANT_ID), future);

        assertSame(getResponse, future.actionGet());
        ArgumentCaptor<GetDataObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetDataObjectRequest.class);
        verify(sdkClient).getDataObjectAsync(requestCaptor.capture());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(CONFIG_ID, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, requestCaptor.getValue().tenantId());
    }

    public void testIndexConfigUsesSdkClient() {
        IndexResponse indexResponse = newIndexResponse(ADCommonName.CONFIG_INDEX, CONFIG_ID);
        when(sdkClient.putDataObjectAsync(any(PutDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(new PutDataObjectResponse(indexResponse)));

        PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
        IndexRequest request = new IndexRequest(ADCommonName.CONFIG_INDEX).id(CONFIG_ID).source("name", "detector");
        dataAccess().index(request, TenantContext.user(TENANT_ID), future);

        IndexResponse response = future.actionGet();
        assertEquals(CONFIG_ID, response.getId());
        ArgumentCaptor<PutDataObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutDataObjectRequest.class);
        verify(sdkClient).putDataObjectAsync(requestCaptor.capture());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(CONFIG_ID, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, requestCaptor.getValue().tenantId());
    }

    public void testUpdateConfigUsesSdkClient() {
        UpdateResponse updateResponse = new UpdateResponse(
            new ShardId(ADCommonName.CONFIG_INDEX, "uuid", 0),
            CONFIG_ID,
            1L,
            1L,
            1L,
            DocWriteResponse.Result.UPDATED
        );
        when(sdkClient.updateDataObjectAsync(any(UpdateDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(new UpdateDataObjectResponse(updateResponse)));

        PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
        UpdateRequest request = new UpdateRequest(ADCommonName.CONFIG_INDEX, CONFIG_ID).doc("name", "updated");
        dataAccess().update(request, TenantContext.user(TENANT_ID), future);

        UpdateResponse response = future.actionGet();
        assertEquals(CONFIG_ID, response.getId());
        ArgumentCaptor<UpdateDataObjectRequest> requestCaptor = ArgumentCaptor.forClass(UpdateDataObjectRequest.class);
        verify(sdkClient).updateDataObjectAsync(requestCaptor.capture());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(CONFIG_ID, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, requestCaptor.getValue().tenantId());
    }

    public void testDeleteConfigUsesSdkClient() {
        DeleteResponse deleteResponse = newDeleteResponse(ADCommonName.CONFIG_INDEX, CONFIG_ID);
        when(sdkClient.deleteDataObjectAsync(any(DeleteDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(new DeleteDataObjectResponse(deleteResponse)));

        PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
        DeleteRequest request = new DeleteRequest(ADCommonName.CONFIG_INDEX, CONFIG_ID);
        dataAccess().delete(request, TenantContext.user(TENANT_ID), future);

        DeleteResponse response = future.actionGet();
        assertEquals(DocWriteResponse.Result.DELETED, response.getResult());
        ArgumentCaptor<DeleteDataObjectRequest> requestCaptor = ArgumentCaptor.forClass(DeleteDataObjectRequest.class);
        verify(sdkClient).deleteDataObjectAsync(requestCaptor.capture());
        assertEquals(ADCommonName.CONFIG_INDEX, requestCaptor.getValue().index());
        assertEquals(CONFIG_ID, requestCaptor.getValue().id());
        assertEquals(TENANT_ID, requestCaptor.getValue().tenantId());
    }

    public void testDeleteUserIndexUsesRestPath() throws Exception {
        String body = toJson(newDeleteResponse("user-index", "doc-1"));
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(body, method, path);
        restServer.start();
        restEndpoint = restServer.endpoint();

        PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
        SdkDataAccess dataAccess = dataAccess(tenantId -> restEndpoint);
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

    public void testSearchTreatsEmptySdkParseFailureAsEmptyResponseForConfigIndex() {
        SearchDataObjectResponse sdkResponse = mock(SearchDataObjectResponse.class);
        when(sdkResponse.searchResponse()).thenThrow(new AssertionError("malformed empty search response total: 0"));
        when(sdkClient.searchDataObjectAsync(any())).thenReturn(CompletableFuture.completedFuture(sdkResponse));

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess().search(new SearchRequest(ADCommonName.CONFIG_INDEX), TenantContext.user(TENANT_ID), future);

        SearchResponse response = future.actionGet();
        assertNotNull(response.getHits());
        assertEquals(0, response.getHits().getHits().length);
        verify(sdkClient).searchDataObjectAsync(any());
    }

    public void testSearchFailsForNonEmptySdkParseFailure() {
        SearchDataObjectResponse sdkResponse = mock(SearchDataObjectResponse.class);
        when(sdkResponse.searchResponse()).thenThrow(new AssertionError("malformed search response"));
        when(sdkClient.searchDataObjectAsync(any())).thenReturn(CompletableFuture.completedFuture(sdkResponse));

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess().search(new SearchRequest(ADCommonName.CONFIG_INDEX), TenantContext.user(TENANT_ID), future);

        OpenSearchStatusException exception = expectThrows(OpenSearchStatusException.class, future::actionGet);
        assertTrue(exception.getMessage().contains("Failed to parse search response"));
        assertTrue(exception.getCause() instanceof AssertionError);
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
        return dataAccess(tenantId -> "http://localhost:9201");
    }

    private SdkDataAccess dataAccess(org.opensearch.timeseries.rest.handler.store.spi.TenantEndpointResolver endpointResolver) {
        return new SdkDataAccess(
            sdkClient,
            clusterService,
            Settings.EMPTY,
            stateManager,
            new RemoteMetadataConfigDocumentStore(sdkClient),
            endpointResolver
        );
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
