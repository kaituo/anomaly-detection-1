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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.TotalHits;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
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
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.PutDataObjectResponse;
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
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.SdkStateManager;
import org.opensearch.timeseries.constant.CommonName;

public class SdkDataAccessTests extends AbstractTimeSeriesTest {
    private static final String TENANT_ID = "account-1:application-1:workspace-1";
    private static final String DATA_SOURCE_ID = "data-source-1";
    private static final String CONFIG_ID = "config-1";
    private static final String AOSS_507_BODY =
        "{\"type\":\"internal_server_exception\",\"reason\":\"Internal error occurred while processing request\"}";

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
        RestClientProvider.closeAll();
        restEndpoint = null;
        DataPlaneClientFactoryContext.setCurrentRequestContext(null);
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

    public void testGetFieldMappingsFallsBackToLocalFieldCapsWithoutClusterPrefix() throws Exception {
        when(clusterService.getClusterName()).thenReturn(new ClusterName("multiTenantCoordinatorCluster"));

        String fieldCapsBody = "{"
            + "\"indices\":[\"server-metrics\"],"
            + "\"fields\":{\"timestamp\":{\"date\":{"
            + "\"type\":\"date\","
            + "\"metadata_field\":false,"
            + "\"searchable\":true,"
            + "\"aggregatable\":true"
            + "}}}"
            + "}";
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(404, "{\"error\":\"not_found\"}"),
            new RestResponseSpec(200, fieldCapsBody)
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        SdkDataAccess dataAccess = dataAccess(new org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver() {
            @Override
            public String resolve(String applicationId, String dataSourceId) {
                return restEndpoint;
            }
        });

        GetFieldMappingsRequest request = new GetFieldMappingsRequest();
        request.indices("server-metrics").fields("timestamp");
        request.indicesOptions(IndicesOptions.strictExpand());

        PlainActionFuture<FieldMappingsView> future = PlainActionFuture.newFuture();
        dataAccess
            .getFieldMappings(
                request,
                null,
                TenantContext.user(TENANT_ID),
                "multiTenantCoordinatorCluster#local",
                AnalysisType.AD,
                future
            );

        FieldMappingsView response = future.actionGet();
        Object timestampMapping = response
            .mappings()
            .get("server-metrics")
            .get("timestamp")
            .sourceAsMap()
            .get("timestamp");
        assertTrue(timestampMapping instanceof Map);
        assertEquals(CommonName.DATE_TYPE, ((Map<?, ?>) timestampMapping).get("type"));
        assertEquals(2, restServer.paths().size());
        assertEquals("GET", restServer.methods().get(0));
        assertEquals("GET", restServer.methods().get(1));
        assertTrue(restServer.paths().get(0).startsWith("/server-metrics/_mapping/field/timestamp"));
        assertTrue(restServer.paths().get(1).startsWith("/server-metrics/_field_caps"));
        assertFalse(restServer.paths().get(1).contains("multiTenantCoordinatorCluster:server-metrics"));
        assertTrue(restServer.paths().get(1).contains("fields=timestamp"));
        assertTrue(restServer.paths().get(1).contains("include_unmapped=true"));
    }

    public void testGetFieldMappingsUsesFieldCapsDirectlyForAossLocalCluster() throws Exception {
        when(clusterService.getClusterName()).thenReturn(new ClusterName("multiTenantCoordinatorCluster"));

        String fieldCapsBody = "{"
            + "\"indices\":[\"server-metrics\"],"
            + "\"fields\":{\"timestamp\":{\"date\":{"
            + "\"type\":\"date\","
            + "\"metadata_field\":false,"
            + "\"searchable\":true,"
            + "\"aggregatable\":true"
            + "}}}"
            + "}";
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(method, path, new RestResponseSpec(200, fieldCapsBody));
        restServer.start();
        restEndpoint = restServer.endpoint();

        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME.getKey(), "aoss")
            .build();
        SdkDataAccess dataAccess = dataAccess(settings, new org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver() {
            @Override
            public String resolve(String applicationId, String dataSourceId) {
                return restEndpoint;
            }
        });

        GetFieldMappingsRequest request = new GetFieldMappingsRequest();
        request.indices("server-metrics").fields("timestamp");
        request.indicesOptions(IndicesOptions.strictExpand());

        PlainActionFuture<FieldMappingsView> future = PlainActionFuture.newFuture();
        dataAccess
            .getFieldMappings(
                request,
                null,
                TenantContext.user(TENANT_ID),
                "multiTenantCoordinatorCluster#local",
                AnalysisType.AD,
                future
            );

        assertNotNull(future.actionGet().mappings().get("server-metrics").get("timestamp"));
        assertEquals(1, restServer.paths().size());
        assertEquals("GET", restServer.methods().get(0));
        assertTrue(restServer.paths().get(0).startsWith("/server-metrics/_field_caps"));
        assertFalse(restServer.paths().get(0).contains("_mapping/field"));
    }

    public void testAossFieldCapsRetriesInsufficientStorage() throws Exception {
        when(clusterService.getClusterName()).thenReturn(new ClusterName("multiTenantCoordinatorCluster"));

        String fieldCapsBody = "{"
            + "\"indices\":[\"server-metrics\"],"
            + "\"fields\":{\"timestamp\":{\"date\":{"
            + "\"type\":\"date\","
            + "\"metadata_field\":false,"
            + "\"searchable\":true,"
            + "\"aggregatable\":true"
            + "}}}"
            + "}";
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(507, AOSS_507_BODY),
            new RestResponseSpec(200, fieldCapsBody)
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        SdkDataAccess dataAccess = dataAccess(aossSettings(), endpointResolver(restEndpoint));

        GetFieldMappingsRequest request = new GetFieldMappingsRequest();
        request.indices("server-metrics").fields("timestamp");
        request.indicesOptions(IndicesOptions.strictExpand());

        PlainActionFuture<FieldMappingsView> future = PlainActionFuture.newFuture();
        dataAccess
            .getFieldMappings(
                request,
                null,
                TenantContext.user(TENANT_ID),
                "multiTenantCoordinatorCluster#local",
                AnalysisType.AD,
                future
            );

        assertNotNull(future.actionGet().mappings().get("server-metrics").get("timestamp"));
        assertEquals(2, restServer.paths().size());
        assertEquals("GET", restServer.methods().get(0));
        assertEquals("GET", restServer.methods().get(1));
        assertTrue(restServer.paths().get(0).startsWith("/server-metrics/_field_caps"));
        assertTrue(restServer.paths().get(1).startsWith("/server-metrics/_field_caps"));
    }

    public void testAossRestSearchRetriesInsufficientStorage() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(507, AOSS_507_BODY),
            new RestResponseSpec(200, searchRestBody("user-index", "doc-1"))
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        SdkDataAccess dataAccess = dataAccess(aossSettings(), endpointResolver(restEndpoint));

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess.search(new SearchRequest("user-index"), TenantContext.user(TENANT_ID), future);

        assertTrue(future.actionGet() instanceof RestDataPlaneResponse);
        assertEquals(2, restServer.paths().size());
        assertEquals("POST", restServer.methods().get(0));
        assertEquals("POST", restServer.methods().get(1));
        assertTrue(restServer.paths().get(0).startsWith("/user-index/_search"));
        assertTrue(restServer.paths().get(1).startsWith("/user-index/_search"));
        verifyNoInteractions(sdkClient);
    }

    public void testAossRestWritesOmitUnsupportedRefreshParameter() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(200, toJson(newUpdateResponse("user-index", "doc-1"))),
            new RestResponseSpec(201, toJson(newIndexResponse("user-index", "doc-2"))),
            new RestResponseSpec(200, toJson(newDeleteResponse("user-index", "doc-3"))),
            new RestResponseSpec(200, successfulBulkRestBody("index", "bulk-1", 201, "created"))
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        SdkDataAccess dataAccess = dataAccess(aossSettings(), endpointResolver(restEndpoint));

        UpdateRequest updateRequest = new UpdateRequest("user-index", "doc-1").doc("field", "value");
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        PlainActionFuture<UpdateResponse> updateFuture = PlainActionFuture.newFuture();
        dataAccess.update(updateRequest, TenantContext.user(TENANT_ID), updateFuture);
        assertEquals(DocWriteResponse.Result.UPDATED, updateFuture.actionGet().getResult());

        IndexRequest indexRequest = new IndexRequest("user-index").id("doc-2").source("field", "value");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        PlainActionFuture<IndexResponse> indexFuture = PlainActionFuture.newFuture();
        dataAccess.index(indexRequest, TenantContext.user(TENANT_ID), indexFuture);
        assertEquals(DocWriteResponse.Result.CREATED, indexFuture.actionGet().getResult());

        DeleteRequest deleteRequest = new DeleteRequest("user-index", "doc-3");
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        PlainActionFuture<DeleteResponse> deleteFuture = PlainActionFuture.newFuture();
        dataAccess.delete(deleteRequest, TenantContext.user(TENANT_ID), deleteFuture);
        assertEquals(DocWriteResponse.Result.DELETED, deleteFuture.actionGet().getResult());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        bulkRequest.add(new IndexRequest("user-index").id("bulk-1").source("field", "value"));
        PlainActionFuture<BulkResponse> bulkFuture = PlainActionFuture.newFuture();
        dataAccess.bulk(bulkRequest, TenantContext.user(TENANT_ID), bulkFuture);
        assertFalse(bulkFuture.actionGet().hasFailures());

        assertEquals(4, restServer.paths().size());
        for (String requestPath : restServer.paths()) {
            assertFalse(requestPath, requestPath.contains("refresh="));
        }
        verifyNoInteractions(sdkClient);
    }

    public void testAdResultRestWriteRequestsAddUnsignedPayloadHeaderInMultitenantMode() throws Exception {
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "tenant-a";
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(200, toJson(newUpdateResponse(resultIndex, "doc-1"))),
            new RestResponseSpec(201, toJson(newIndexResponse(resultIndex, "doc-2"))),
            new RestResponseSpec(200, successfulBulkRestBody("index", "bulk-1", 201, "created"))
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        SdkDataAccess dataAccess = dataAccess(aossSettings(), endpointResolver(restEndpoint));
        TenantContext tenantContext = TenantContext.user(TENANT_ID, DATA_SOURCE_ID);

        PlainActionFuture<UpdateResponse> updateFuture = PlainActionFuture.newFuture();
        dataAccess.update(new UpdateRequest(resultIndex, "doc-1").doc("field", "value"), tenantContext, updateFuture);
        assertEquals(DocWriteResponse.Result.UPDATED, updateFuture.actionGet().getResult());

        PlainActionFuture<IndexResponse> indexFuture = PlainActionFuture.newFuture();
        dataAccess.index(new IndexRequest(resultIndex).id("doc-2").source("field", "value"), tenantContext, indexFuture);
        assertEquals(DocWriteResponse.Result.CREATED, indexFuture.actionGet().getResult());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(resultIndex).id("bulk-1").source("field", "value"));
        PlainActionFuture<BulkResponse> bulkFuture = PlainActionFuture.newFuture();
        dataAccess.bulk(bulkRequest, tenantContext, bulkFuture);
        assertFalse(bulkFuture.actionGet().hasFailures());

        assertEquals(3, restServer.headers().size());
        for (Map<String, String> requestHeaders : restServer.headers()) {
            assertEquals(AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD, requestHeaders.get(AwsSigV4RequestHeaders.CONTENT_SHA256));
        }
        verifyNoInteractions(sdkClient);
    }

    public void testAossUpdateByQueryUsesSearchAndPerHitUpdate() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(200, searchRestBody("user-index", "doc-1")),
            new RestResponseSpec(200, toJson(newUpdateResponse("user-index", "doc-1")))
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        SdkDataAccess dataAccess = dataAccess(aossSettings(), endpointResolver(restEndpoint));
        UpdateByQueryRequest request = new UpdateByQueryRequest("user-index");
        request.setQuery(QueryBuilders.matchAllQuery());
        request.setRefresh(true);
        request.setScript(new Script("ctx._source.latest=false;"));

        PlainActionFuture<BulkByScrollResponse> future = PlainActionFuture.newFuture();
        dataAccess.updateByQuery(request, TenantContext.user(TENANT_ID), future);

        assertEquals(1L, future.actionGet().getUpdated());
        assertEquals(2, restServer.paths().size());
        assertTrue(restServer.paths().get(0).startsWith("/user-index/_search"));
        assertTrue(restServer.paths().get(1).startsWith("/user-index/_update/doc-1"));
        assertFalse(restServer.paths().get(1).contains("refresh="));
        assertTrue(restServer.bodies().get(1), restServer.bodies().get(1).contains("\"latest\":false"));
        verifyNoInteractions(sdkClient);
    }

    public void testAossUpdateByQueryReusesCapturedRequestContextForFollowUpRestCalls() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(200, searchRestBody("user-index", "doc-1")),
            new RestResponseSpec(200, toJson(newUpdateResponse("user-index", "doc-1")))
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        AtomicInteger contextCreations = new AtomicInteger();
        SdkDataAccess dataAccess = dataAccess(
            aossSettings(),
            endpointResolver(restEndpoint),
            countingDataPlaneClientFactory(contextCreations)
        );
        UpdateByQueryRequest request = new UpdateByQueryRequest("user-index");
        request.setQuery(QueryBuilders.matchAllQuery());
        request.setScript(new Script("ctx._source.latest=false;"));

        PlainActionFuture<BulkByScrollResponse> future = PlainActionFuture.newFuture();
        dataAccess.updateByQuery(request, TenantContext.user(TENANT_ID, "data-source-1"), future);

        assertEquals(1L, future.actionGet().getUpdated());
        assertEquals(2, restServer.paths().size());
        assertEquals(1, contextCreations.get());
        assertNull(DataPlaneClientFactoryContext.getCurrentRequestContext());
    }

    public void testAossDeleteByQueryUsesSearchAndPerHitDelete() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(200, searchRestBody("user-index", "doc-1")),
            new RestResponseSpec(200, toJson(newDeleteResponse("user-index", "doc-1")))
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        SdkDataAccess dataAccess = dataAccess(aossSettings(), endpointResolver(restEndpoint));
        DeleteByQueryRequest request = new DeleteByQueryRequest("user-index");
        request.setQuery(QueryBuilders.matchAllQuery());
        request.setRefresh(true);

        PlainActionFuture<BulkByScrollResponse> future = PlainActionFuture.newFuture();
        dataAccess.deleteByQuery(request, TenantContext.user(TENANT_ID), future);

        assertEquals(1L, future.actionGet().getDeleted());
        assertEquals(2, restServer.paths().size());
        assertTrue(restServer.paths().get(0).startsWith("/user-index/_search"));
        assertEquals("/user-index/_doc/doc-1", restServer.paths().get(1));
        assertFalse(restServer.paths().get(1).contains("refresh="));
        verifyNoInteractions(sdkClient);
    }

    public void testAossDeleteByQueryReusesCapturedRequestContextForFollowUpRestCalls() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(
            method,
            path,
            new RestResponseSpec(200, searchRestBody("user-index", "doc-1")),
            new RestResponseSpec(200, toJson(newDeleteResponse("user-index", "doc-1")))
        );
        restServer.start();
        restEndpoint = restServer.endpoint();

        AtomicInteger contextCreations = new AtomicInteger();
        SdkDataAccess dataAccess = dataAccess(
            aossSettings(),
            endpointResolver(restEndpoint),
            countingDataPlaneClientFactory(contextCreations)
        );
        DeleteByQueryRequest request = new DeleteByQueryRequest("user-index");
        request.setQuery(QueryBuilders.matchAllQuery());

        PlainActionFuture<BulkByScrollResponse> future = PlainActionFuture.newFuture();
        dataAccess.deleteByQuery(request, TenantContext.user(TENANT_ID, "data-source-1"), future);

        assertEquals(1L, future.actionGet().getDeleted());
        assertEquals(2, restServer.paths().size());
        assertEquals(1, contextCreations.get());
        assertNull(DataPlaneClientFactoryContext.getCurrentRequestContext());
    }

    public void testAossUpdateSettingsFiltersUnsupportedSettings() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        restServer = new TestRestServer(method, path, new RestResponseSpec(200, "{\"acknowledged\":true}"));
        restServer.start();
        restEndpoint = restServer.endpoint();

        SdkDataAccess dataAccess = dataAccess(aossSettings(), endpointResolver(restEndpoint));
        UpdateSettingsRequest request = new UpdateSettingsRequest("user-index")
            .settings(Settings.builder().put("index.number_of_replicas", 0).put("index.default_pipeline", "pipeline-1").build());

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();
        dataAccess.updateSettings(request, null, TenantContext.user(TENANT_ID), future);

        assertTrue(future.actionGet().isAcknowledged());
        assertEquals("PUT", restServer.methods().get(0));
        assertTrue(restServer.paths().get(0).startsWith("/user-index/_settings"));
        assertTrue(restServer.bodies().get(0), restServer.bodies().get(0).contains("default_pipeline"));
        assertFalse(restServer.bodies().get(0), restServer.bodies().get(0).contains("number_of_replicas"));
    }

    public void testAossUpdateSettingsAcknowledgesWhenNoSupportedSettingsRemain() {
        SdkDataAccess dataAccess = dataAccess(aossSettings(), endpointResolver("http://127.0.0.1:1"));
        UpdateSettingsRequest request = new UpdateSettingsRequest("user-index")
            .settings(Settings.builder().put("index.number_of_replicas", 0).build());

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();
        dataAccess.updateSettings(request, null, TenantContext.user(TENANT_ID), future);

        assertTrue(future.actionGet().isAcknowledged());
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

    public void testSdkSearchMarksRestDataPlaneResponse() {
        SearchDataObjectResponse sdkSearchResponse = mock(SearchDataObjectResponse.class);
        when(sdkSearchResponse.parser()).thenReturn(mock(XContentParser.class));
        when(sdkSearchResponse.searchResponse()).thenReturn(SdkSearchResponseUtils.emptySearchResponse());
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(sdkSearchResponse));

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess().search(new SearchRequest(ADCommonName.DETECTION_STATE_INDEX), TenantContext.user(TENANT_ID), future);

        assertTrue(future.actionGet() instanceof RestDataPlaneResponse);
    }

    public void testSdkSearchStateIndexNotFoundFails() {
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class))).thenReturn(failedSearchFuture());

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess().search(new SearchRequest(ADCommonName.DETECTION_STATE_INDEX), TenantContext.user(TENANT_ID), future);

        Exception exception = expectThrows(Exception.class, future::actionGet);
        assertTrue(exception.getMessage().contains("index_not_found_exception"));
    }

    public void testSdkSearchNonStateIndexNotFoundReturnsEmptyResponse() {
        when(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest.class))).thenReturn(failedSearchFuture());

        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        dataAccess().search(new SearchRequest(ADCommonName.CHECKPOINT_INDEX_NAME), TenantContext.user(TENANT_ID), future);

        assertEquals(0, future.actionGet().getHits().getHits().length);
    }

    public void testBindRoutingUsesBackgroundEndpointResolverAndRestoresPreviousContext() {
        AtomicReference<String> resolvedApplicationId = new AtomicReference<>();
        AtomicReference<String> resolvedDataSourceId = new AtomicReference<>();
        org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver endpointResolver = (
            applicationId,
            dataSourceId) -> {
            resolvedApplicationId.set(applicationId);
            resolvedDataSourceId.set(dataSourceId);
            restEndpoint = "http://127.0.0.1:9219";
            return restEndpoint;
        };
        DataPlaneClientFactory apiFactory = new DataPlaneClientFactory() {
            @Override
            public RestClient getClient(String tenantId, String dataSourceId) {
                throw new AssertionError("API data-plane factory should not be used for bound background routing");
            }
        };
        SdkDataAccess dataAccess = dataAccess(Settings.EMPTY, endpointResolver, apiFactory);
        DataPlaneClientFactory.RequestContext previousContext = new DataPlaneClientFactory.RequestContext(
            "previous-tenant",
            "previous-data-source",
            new DataPlaneClientFactory.ResolvedClient(mock(RestClient.class), "previous-endpoint"),
            request -> () -> {}
        );
        DataPlaneClientFactoryContext.setCurrentRequestContext(previousContext);

        try (Releasable ignored = dataAccess.bindRouting(TENANT_ID, DATA_SOURCE_ID)) {
            DataPlaneClientFactory.RequestContext currentContext = DataPlaneClientFactoryContext.getCurrentRequestContext();
            assertNotNull(currentContext);
            assertEquals(TENANT_ID, currentContext.tenantId());
            assertEquals(DATA_SOURCE_ID, currentContext.dataSourceId());
            assertEquals("http://127.0.0.1:9219", currentContext.endpoint());
            assertEquals("application-1", resolvedApplicationId.get());
            assertEquals(DATA_SOURCE_ID, resolvedDataSourceId.get());
        }

        assertSame(previousContext, DataPlaneClientFactoryContext.getCurrentRequestContext());
    }

    public void testSdkIndexPreservesBoundRequestContextForAsyncListener() {
        CompletableFuture<PutDataObjectResponse> sdkFuture = new CompletableFuture<>();
        when(sdkClient.putDataObjectAsync(any(PutDataObjectRequest.class))).thenReturn(sdkFuture);
        restEndpoint = "http://127.0.0.1:9229";
        SdkDataAccess dataAccess = dataAccess(endpointResolver(restEndpoint));

        PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
        AtomicReference<DataPlaneClientFactory.RequestContext> contextSeenByListener = new AtomicReference<>();
        IndexRequest request = new IndexRequest(ADCommonName.DETECTION_STATE_INDEX).id("task-1").source("field", "value");

        try (Releasable ignored = dataAccess.bindRouting(TENANT_ID, DATA_SOURCE_ID)) {
            dataAccess.index(request, TenantContext.user(TENANT_ID), ActionListener.wrap(response -> {
                contextSeenByListener.set(DataPlaneClientFactoryContext.getCurrentRequestContext());
                future.onResponse(response);
            }, future::onFailure));
        }
        assertNull(DataPlaneClientFactoryContext.getCurrentRequestContext());

        sdkFuture.complete(new PutDataObjectResponse(newIndexResponse(ADCommonName.DETECTION_STATE_INDEX, "task-1")));

        assertEquals("task-1", future.actionGet().getId());
        DataPlaneClientFactory.RequestContext capturedContext = contextSeenByListener.get();
        assertNotNull(capturedContext);
        assertEquals(TENANT_ID, capturedContext.tenantId());
        assertEquals(DATA_SOURCE_ID, capturedContext.dataSourceId());
        assertEquals(restEndpoint, capturedContext.endpoint());
        assertNull(DataPlaneClientFactoryContext.getCurrentRequestContext());
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

    private Settings aossSettings() {
        return Settings.builder().put(AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME.getKey(), "aoss").build();
    }

    private org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver endpointResolver(String endpoint) {
        return new org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver() {
            @Override
            public String resolve(String applicationId, String dataSourceId) {
                return endpoint;
            }
        };
    }

    private SdkDataAccess dataAccess(org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver endpointResolver) {
        return dataAccess(Settings.EMPTY, endpointResolver);
    }

    private SdkDataAccess dataAccess(
        Settings settings,
        org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver endpointResolver
    ) {
        return dataAccess(settings, endpointResolver, new UnsignedClientFactory(endpointResolver));
    }

    private SdkDataAccess dataAccess(
        Settings settings,
        org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver endpointResolver,
        DataPlaneClientFactory dataPlaneClientFactory
    ) {
        return new SdkDataAccess(
            sdkClient,
            clusterService,
            settings,
            stateManager,
            configDocumentStore,
            endpointResolver,
            dataPlaneClientFactory
        );
    }

    private CompletableFuture<SearchDataObjectResponse> failedSearchFuture() {
        CompletableFuture<SearchDataObjectResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("[index_not_found_exception] no such index"));
        return failedFuture;
    }

    private DataPlaneClientFactory countingDataPlaneClientFactory(AtomicInteger contextCreations) {
        return new DataPlaneClientFactory() {
            @Override
            public RestClient getClient(String tenantId, String dataSourceId) {
                return RestClientProvider.getRestClient(restEndpoint);
            }

            @Override
            public ResolvedClient getResolvedClient(String tenantId, String dataSourceId) {
                return new ResolvedClient(RestClientProvider.getRestClient(restEndpoint), restEndpoint);
            }

            @Override
            public RequestContext createRequestContext(String tenantId, String dataSourceId) {
                contextCreations.incrementAndGet();
                return DataPlaneClientFactory.super.createRequestContext(tenantId, dataSourceId);
            }
        };
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

    private UpdateResponse newUpdateResponse(String index, String id) {
        UpdateResponse response = new UpdateResponse(new ShardId(index, "uuid", 0), id, 1, 1, 1, DocWriteResponse.Result.UPDATED);
        response.setShardInfo(new ShardInfo(1, 1));
        return response;
    }

    private String searchRestBody(String index, String id) {
        return "{"
            + "\"took\":1,"
            + "\"timed_out\":false,"
            + "\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0},"
            + "\"hits\":{\"total\":{\"value\":1,\"relation\":\"eq\"},\"max_score\":1.0,\"hits\":[{"
            + "\"_index\":\""
            + index
            + "\",\"_id\":\""
            + id
            + "\",\"_score\":1.0,\"_source\":{\"field\":\"value\"}}]}}";
    }

    private String successfulBulkRestBody(String action, String id, int status, String result) {
        return "{"
            + "\"took\":1,"
            + "\"errors\":false,"
            + "\"items\":[{\""
            + action
            + "\":{\"_index\":\"user-index\",\"_id\":\""
            + id
            + "\",\"_version\":1,\"result\":\""
            + result
            + "\",\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0},\"status\":"
            + status
            + ",\"_seq_no\":1,\"_primary_term\":1}}]}";
    }

    private SearchDataObjectResponse mockSdkSearchResponse(String hitIndex, String hitId) {
        SearchHit hit = new SearchHit(0, hitId, null, null);
        hit.shard(new SearchShardTarget("node-0", new ShardId(hitIndex, "uuid", 0), null, OriginalIndices.NONE));

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits())
            .thenReturn(new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f));

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
        private final AtomicReference<String> method;
        private final AtomicReference<String> path;
        private final RestResponseSpec[] responses;
        private final List<String> methods = Collections.synchronizedList(new ArrayList<>());
        private final List<String> paths = Collections.synchronizedList(new ArrayList<>());
        private final List<String> bodies = Collections.synchronizedList(new ArrayList<>());
        private final List<Map<String, String>> headers = Collections.synchronizedList(new ArrayList<>());

        TestRestServer(String body, AtomicReference<String> method, AtomicReference<String> path) throws IOException {
            this(method, path, new RestResponseSpec(200, body));
        }

        TestRestServer(AtomicReference<String> method, AtomicReference<String> path, RestResponseSpec... responses) throws IOException {
            this.serverSocket = new ServerSocket(0);
            this.method = method;
            this.path = path;
            this.responses = responses;
            this.thread = new Thread(this::serve, "sdk-data-access-test-server");
            this.thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        String endpoint() {
            return "http://127.0.0.1:" + serverSocket.getLocalPort();
        }

        List<String> methods() {
            return methods;
        }

        List<String> paths() {
            return paths;
        }

        List<String> bodies() {
            return bodies;
        }

        List<Map<String, String>> headers() {
            return headers;
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            thread.join(5000);
        }

        private void serve() {
            for (RestResponseSpec response : responses) {
                if (serverSocket.isClosed()) {
                    return;
                }
                try (Socket socket = serverSocket.accept()) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    String requestLine = reader.readLine();
                    assertNotNull(requestLine);
                    String[] tokens = requestLine.split(" ");
                    assertTrue(tokens.length >= 2);
                    method.set(tokens[0]);
                    path.set(tokens[1]);
                    methods.add(tokens[0]);
                    paths.add(tokens[1]);

                    int contentLength = 0;
                    Map<String, String> requestHeaders = new HashMap<>();
                    String headerLine;
                    while ((headerLine = reader.readLine()) != null && headerLine.isEmpty() == false) {
                        int separator = headerLine.indexOf(':');
                        if (separator > 0) {
                            requestHeaders
                                .put(
                                    headerLine.substring(0, separator).trim().toLowerCase(Locale.ROOT),
                                    headerLine.substring(separator + 1).trim()
                                );
                        }
                        String lowerHeader = headerLine.toLowerCase(Locale.ROOT);
                        if (lowerHeader.startsWith("content-length:")) {
                            contentLength = Integer.parseInt(headerLine.substring(headerLine.indexOf(':') + 1).trim());
                        }
                    }
                    headers.add(requestHeaders);
                    StringBuilder requestBody = new StringBuilder();
                    for (int i = 0; i < contentLength; i++) {
                        int next = reader.read();
                        if (next < 0) {
                            break;
                        }
                        requestBody.append((char) next);
                    }
                    bodies.add(requestBody.toString());

                    writeResponse(socket, response);
                } catch (IOException e) {
                    if (serverSocket.isClosed() == false) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private void writeResponse(Socket socket, RestResponseSpec response) throws IOException {
            byte[] responseBytes = response.body.getBytes(StandardCharsets.UTF_8);
            byte[] headerBytes = ("HTTP/1.1 "
                + response.statusCode
                + " "
                + reasonPhrase(response.statusCode)
                + "\r\n"
                + "Content-Type: application/json\r\n"
                + "Content-Length: "
                + responseBytes.length
                + "\r\n"
                + "Connection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
            OutputStream responseBody = socket.getOutputStream();
            responseBody.write(headerBytes);
            responseBody.write(responseBytes);
            responseBody.flush();
        }

        private String reasonPhrase(int statusCode) {
            if (statusCode == 404) {
                return "Not Found";
            }
            if (statusCode == 507) {
                return "Insufficient Storage";
            }
            return "OK";
        }
    }

    private static final class RestResponseSpec {
        private final int statusCode;
        private final String body;

        private RestResponseSpec(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
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
