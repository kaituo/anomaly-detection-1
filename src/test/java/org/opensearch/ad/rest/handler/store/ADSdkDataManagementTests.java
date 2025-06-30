/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler.store;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.client.AwsSigV4RequestHeaders;
import org.opensearch.timeseries.client.ConfigDocumentStore;
import org.opensearch.timeseries.client.DataPlaneClientFactory;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.rest.handler.store.SDKDataManagement;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolverFactory;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.Client;

import com.google.common.cache.Cache;

public class ADSdkDataManagementTests extends AbstractTimeSeriesTest {
    private static final String ENDPOINT = "http://127.0.0.1:9201";
    private static final String TENANT_ID = "account-a:application-a:workspace-a";
    private static final String DATA_SOURCE_ID = "data-source-a";
    private static final String OTHER_TENANT_ID = "account-b:application-b:workspace-b";
    private static final String OTHER_DATA_SOURCE_ID = "data-source-b";
    private static final String RESULT_ALIAS = "opensearch-ad-plugin-result-sdk-branch";
    private static final AtomicReference<String> RESOLVED_APPLICATION_ID = new AtomicReference<>();
    private static final AtomicReference<String> RESOLVED_DATA_SOURCE_ID = new AtomicReference<>();

    private RestClient restClient;
    private ConfigDocumentStore configDocumentStore;
    private ClusterService clusterService;
    private ADSdkDataManagement dataManagement;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        System.setProperty("opensearch.tenant.endpoint", ENDPOINT);
        RESOLVED_APPLICATION_ID.set(null);
        RESOLVED_DATA_SOURCE_ID.set(null);

        restClient = mock(RestClient.class);
        configDocumentStore = mock(ConfigDocumentStore.class);
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TimeSeriesSettings.MAX_CONCURRENT_SDK_INDEX_MAPPING_UPDATES)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        cacheRestClient(ENDPOINT, restClient);
        bridgeAsyncRestClientToSyncStub();

        dataManagement = newDataManagement(dataManagementSettings());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        invalidateRestClient(ENDPOINT);
        System.clearProperty("opensearch.tenant.endpoint");
        super.tearDown();
    }

    public void testValidateResultIndexMappingUsesResolvedAlias() throws Exception {
        String alias = "result-alias";
        String concreteIndex = "result-concrete";

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            assertEquals("HEAD", request.getMethod());
            assertEquals("/_alias/" + alias, request.getEndpoint());
            return response(200);
        });
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/_alias/" + alias).equals(request.getEndpoint())) {
                listener.onSuccess(response(200, aliasResponseBody(alias, concreteIndex)));
            } else if (("/" + concreteIndex + "/_mapping").equals(request.getEndpoint())) {
                listener.onSuccess(response(200, validMappingResponseBody(concreteIndex)));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        dataManagement.validateResultIndexMapping(alias, future, TENANT_ID, null);

        assertTrue(future.actionGet());
    }

    public void testValidateResultIndexMappingFallsBackToAliasWhenAliasParsingFails() throws Exception {
        String alias = "result-alias";
        AtomicReference<String> mappingEndpoint = new AtomicReference<>();
        Response aliasExists = response(200);

        when(restClient.performRequest(any(Request.class))).thenReturn(aliasExists);
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/_alias/" + alias).equals(request.getEndpoint())) {
                listener.onSuccess(response(200, "{not-json"));
            } else if (("/" + alias + "/_mapping").equals(request.getEndpoint())) {
                mappingEndpoint.set(request.getEndpoint());
                listener.onSuccess(response(200, validMappingResponseBody(alias)));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        dataManagement.validateResultIndexMapping(alias, future, TENANT_ID, null);

        assertTrue(future.actionGet());
        assertEquals("/" + alias + "/_mapping", mappingEndpoint.get());
    }

    public void testValidateResultIndexMappingPropagatesAsyncFailure() throws IOException {
        ActionListener<Boolean> listener = mock(ActionListener.class);
        Response aliasMissing = response(404);

        when(restClient.performRequest(any(Request.class))).thenReturn(aliasMissing);
        doAnswer(invocation -> {
            ResponseListener responseListener = invocation.getArgument(1);
            responseListener.onFailure(new IllegalStateException("mapping failure"));
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        dataManagement.validateResultIndexMapping(RESULT_ALIAS, listener, TENANT_ID, null);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue().getCause().getMessage(), containsString("mapping failure"));
    }

    public void testValidateResultIndexMappingPassesTenantIdToAliasLookup() throws Exception {
        ADSdkDataManagement spy = spy(dataManagement);
        ArgumentCaptor<String> tenantIdCaptor = ArgumentCaptor.forClass(String.class);
        Response aliasMissing = response(404);

        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/_alias/" + RESULT_ALIAS).equals(request.getEndpoint())) {
                listener.onFailure(responseException(aliasMissing, request));
            } else if (("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint())) {
                listener.onSuccess(response(200, validMappingResponseBody(RESULT_ALIAS)));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        spy.validateResultIndexMapping(RESULT_ALIAS, future, TENANT_ID, null);

        assertTrue(future.actionGet());
        verify(spy).doesResultAliasExists(eq(RESULT_ALIAS), any(), tenantIdCaptor.capture(), any());
        assertEquals(TENANT_ID, tenantIdCaptor.getValue());
    }

    public void testValidateResultIndexAndExecuteReportsDeleteFailure() throws IOException {
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener responseListener = invocation.getArgument(1);
            if ("PUT".equals(request.getMethod())) {
                responseListener.onSuccess(response(201));
            } else if ("DELETE".equals(request.getMethod())) {
                responseListener.onSuccess(response(500));
            } else {
                fail("Unexpected method: " + request.getMethod());
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        dataManagement.validateResultIndexAndExecute(RESULT_ALIAS, function, true, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue().getMessage(), containsString("Failed to delete dummy result"));
        verifyNoInteractions(function);
    }

    public void testValidateResultIndexAndExecuteSkipsDummyProbeOnAoss() throws Exception {
        ADSdkDataManagement aossDataManagement = newDataManagement(
            dataManagementSettings().put(AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME.getKey(), "aoss").build()
        );
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        aossDataManagement.validateResultIndexAndExecute(RESULT_ALIAS, function, true, listener, TENANT_ID);

        verify(function).execute();
        verify(listener, never()).onFailure(any());
        verify(restClient, never()).performRequestAsync(any(Request.class), any(ResponseListener.class));
    }

    public void testInitCustomResultIndexDirectlyCreatesDirectIndexWithoutAliasOnAoss() throws Exception {
        AtomicReference<Request> capturedRequest = new AtomicReference<>();
        ADSdkDataManagement aossDataManagement = newDataManagement(aossDataManagementSettings());

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            capturedRequest.set(request);
            return response(200);
        });

        PlainActionFuture<CreateIndexResponse> future = PlainActionFuture.newFuture();
        aossDataManagement.initCustomResultIndexDirectly(RESULT_ALIAS, future, TENANT_ID, null);

        assertTrue(future.actionGet().isAcknowledged());
        Request request = capturedRequest.get();
        assertNotNull(request);
        assertEquals("PUT", request.getMethod());
        assertEquals("/" + RESULT_ALIAS, request.getEndpoint());
        assertEquals(AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD, headerValue(request, AwsSigV4RequestHeaders.CONTENT_SHA256));
        String requestBody = EntityUtils.toString(request.getEntity());
        assertThat(requestBody, containsString("\"mappings\""));
        assertFalse(requestBody.contains("\"aliases\""));
    }

    public void testInitCustomResultIndexDirectlyPassesDataSourceIdToDataPlaneFactory() throws Exception {
        CapturingDataPlaneClientFactory dataPlaneClientFactory = new CapturingDataPlaneClientFactory(restClient);
        ADSdkDataManagement dataManagementWithCapturingFactory = new ADSdkDataManagement(
            mock(Client.class),
            NamedXContentRegistry.EMPTY,
            dataManagementSettings().build(),
            clusterService,
            configDocumentStore,
            dataPlaneClientFactory
        );

        Response createResponse = response(200);
        when(restClient.performRequest(any(Request.class))).thenReturn(createResponse);

        PlainActionFuture<CreateIndexResponse> future = PlainActionFuture.newFuture();
        dataManagementWithCapturingFactory.initCustomResultIndexDirectly(RESULT_ALIAS, future, TENANT_ID, DATA_SOURCE_ID);

        assertTrue(future.actionGet().isAcknowledged());
        assertEquals(TENANT_ID, dataPlaneClientFactory.tenantId.get());
        assertEquals(DATA_SOURCE_ID, dataPlaneClientFactory.dataSourceId.get());
    }

    public void testAossCustomResultIndexExistenceChecksUseDirectIndexGet() throws Exception {
        AtomicReference<Request> capturedRequest = new AtomicReference<>();
        ADSdkDataManagement aossDataManagement = newDataManagement(aossDataManagementSettings());

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            capturedRequest.set(request);
            return response(200, "{}");
        });

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        aossDataManagement.doesResultIndexExists(RESULT_ALIAS, future, TENANT_ID);

        assertTrue(future.actionGet());
        Request request = capturedRequest.get();
        assertNotNull(request);
        assertEquals("GET", request.getMethod());
        assertEquals("/" + RESULT_ALIAS, request.getEndpoint());
    }

    public void testAossCustomResultIndexOrAliasExistenceChecksDirectIndexOnly() throws Exception {
        AtomicReference<Request> capturedRequest = new AtomicReference<>();
        ADSdkDataManagement aossDataManagement = newDataManagement(aossDataManagementSettings());

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            capturedRequest.set(request);
            return response(404, "{}");
        });

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        aossDataManagement.doesResultIndexOrAliasExists(RESULT_ALIAS, future, TENANT_ID, null);

        assertFalse(future.actionGet());
        Request request = capturedRequest.get();
        assertNotNull(request);
        assertEquals("GET", request.getMethod());
        assertEquals("/" + RESULT_ALIAS, request.getEndpoint());
    }

    public void testAossValidateResultIndexMappingUsesDirectIndex() throws Exception {
        AtomicReference<String> mappingEndpoint = new AtomicReference<>();
        ADSdkDataManagement aossDataManagement = newDataManagement(aossDataManagementSettings());

        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint())) {
                mappingEndpoint.set(request.getEndpoint());
                listener.onSuccess(response(200, validMappingResponseBody(RESULT_ALIAS)));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        aossDataManagement.validateResultIndexMapping(RESULT_ALIAS, future, TENANT_ID, null);

        assertTrue(future.actionGet());
        assertEquals("/" + RESULT_ALIAS + "/_mapping", mappingEndpoint.get());
        verify(restClient, never()).performRequest(any(Request.class));
    }

    public void testInitCustomResultIndexDirectlyUsesDateMathForNonAoss() throws Exception {
        AtomicReference<Request> capturedRequest = new AtomicReference<>();

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            capturedRequest.set(request);
            return response(200);
        });

        PlainActionFuture<CreateIndexResponse> future = PlainActionFuture.newFuture();
        dataManagement.initCustomResultIndexDirectly(RESULT_ALIAS, future, TENANT_ID, null);

        assertTrue(future.actionGet().isAcknowledged());
        Request request = capturedRequest.get();
        assertNotNull(request);
        assertEquals("PUT", request.getMethod());
        assertThat(request.getEndpoint(), containsString("%7Bnow%2Fd%7D"));
        assertThat(EntityUtils.toString(request.getEntity()), containsString("\"aliases\":{\"" + RESULT_ALIAS + "\":{}}"));
    }

    public void testInitCustomResultIndexDirectlyTreatsResourceAlreadyExistsBodyAsAlreadyExists() throws IOException {
        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        Response alreadyExists = response(400, "{\"error\":{\"type\":\"resource_already_exists_exception\"}}");
        when(restClient.performRequest(any(Request.class))).thenReturn(alreadyExists);

        dataManagement.initCustomResultIndexDirectly(RESULT_ALIAS, listener, TENANT_ID, null);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue() instanceof ResourceAlreadyExistsException);
    }

    public void testInitFlattenedResultIndexWithoutAliasOmitsAliases() throws Exception {
        AtomicReference<Request> capturedRequest = new AtomicReference<>();

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            capturedRequest.set(request);
            return response(200);
        });

        PlainActionFuture<CreateIndexResponse> future = PlainActionFuture.newFuture();
        dataManagement.initFlattenedResultIndex(null, future, TENANT_ID);

        assertTrue(future.actionGet().isAcknowledged());
        String requestBody = EntityUtils.toString(capturedRequest.get().getEntity());
        assertFalse(requestBody.contains("\"aliases\""));
    }

    public void testInitCustomResultIndexAndExecuteFallsBackToValidationOnAlreadyExistsFailure() {
        ADSdkDataManagement spy = spy(dataManagement);
        ExecutorFunction function = mock(ExecutorFunction.class);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(false);
            return null;
        }).when(spy).doesResultIndexOrAliasExists(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(new ResourceAlreadyExistsException(RESULT_ALIAS));
            return null;
        }).when(spy).initCustomResultIndexDirectly(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = invocation.getArgument(3);
            listener.onResponse(null);
            return null;
        }).when(spy).validateResultIndexAndExecute(eq(RESULT_ALIAS), same(function), eq(false), any(), eq(TENANT_ID), any());

        spy.initCustomResultIndexAndExecute(RESULT_ALIAS, function, future, TENANT_ID);

        assertNull(future.actionGet());
        verify(spy).validateResultIndexAndExecute(eq(RESULT_ALIAS), same(function), eq(false), any(), eq(TENANT_ID), any());
    }

    public void testValidateCustomIndexForBackendJobExecutesAfterCreatingMissingIndex() {
        ADSdkDataManagement spy = spy(dataManagement);
        ExecutorFunction function = mock(ExecutorFunction.class);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(false);
            return null;
        }).when(spy).doesResultIndexOrAliasExists(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(new CreateIndexResponse(true, true, RESULT_ALIAS));
            return null;
        }).when(spy).initCustomResultIndexDirectly(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        doAnswer(invocation -> {
            ExecutorFunction executorFunction = invocation.getArgument(1);
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = invocation.getArgument(3);
            executorFunction.execute();
            listener.onResponse(null);
            return null;
        }).when(spy).validateResultIndexAndExecute(eq(RESULT_ALIAS), any(ExecutorFunction.class), eq(true), any(), eq(TENANT_ID), any());

        spy.validateCustomIndexForBackendJob(RESULT_ALIAS, "config-id", "test-user", List.of("role-a"), function, future, TENANT_ID);

        assertNull(future.actionGet());
        verify(function).execute();
        verify(spy).validateResultIndexAndExecute(eq(RESULT_ALIAS), any(ExecutorFunction.class), eq(true), any(), eq(TENANT_ID), any());
    }

    public void testValidateCustomIndexForBackendJobRejectsInvalidExistingMapping() {
        ADSdkDataManagement spy = spy(dataManagement);
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> existsListener = invocation.getArgument(1);
            existsListener.onResponse(true);
            return null;
        }).when(spy).doesResultIndexOrAliasExists(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> mappingListener = invocation.getArgument(1);
            mappingListener.onResponse(false);
            return null;
        }).when(spy).validateResultIndexMapping(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());

        spy.validateCustomIndexForBackendJob(RESULT_ALIAS, "config-id", "test-user", List.of("role-a"), function, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue() instanceof EndRunException);
        assertEquals("Result index mapping is not correct", exceptionCaptor.getValue().getMessage());
        verifyNoInteractions(function);
    }

    public void testUpdateSkipsFurtherWorkAfterSuccessfulEmptySearch() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(null);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(configDocumentStore).search(any(), any(TenantContext.class), any());

        dataManagement.update(TENANT_ID);
        dataManagement.update(TENANT_ID);

        verify(configDocumentStore, times(1)).search(any(), any(TenantContext.class), any());
    }

    public void testUpdateSkipsFurtherWorkAfterEmptySearchHits() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(SearchHits.empty());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(configDocumentStore).search(any(), any(TenantContext.class), any());

        dataManagement.update(TENANT_ID);
        dataManagement.update(TENANT_ID);

        verify(configDocumentStore, times(1)).search(any(), any(TenantContext.class), any());
    }

    public void testUpdateInvalidMappingTriggersPutMapping() throws IOException {
        String customIndex = RESULT_ALIAS;
        SearchHit searchHit = new SearchHit(0).sourceRef(new BytesArray(configSource(customIndex, TENANT_ID, DATA_SOURCE_ID)));
        SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, null, 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(configDocumentStore).search(any(), any(TenantContext.class), any());

        AtomicReference<Request> capturedMappingRequest = new AtomicReference<>();
        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if ("HEAD".equals(request.getMethod())) {
                return response(404);
            }
            if ("PUT".equals(request.getMethod()) && ("/" + customIndex + "/_mapping").equals(request.getEndpoint())) {
                capturedMappingRequest.set(request);
                return response(200);
            }
            fail("Unexpected synchronous request: " + request);
            return null;
        });
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/_alias/" + customIndex).equals(request.getEndpoint())) {
                listener.onFailure(responseException(404, null));
            } else if (("/" + customIndex + "/_mapping").equals(request.getEndpoint())) {
                listener.onSuccess(response(200, "{\"" + customIndex + "\":{\"mappings\":{}}}"));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        dataManagement.update(TENANT_ID);

        verify(restClient, times(1)).performRequest(argThat(request -> {
            try {
                return "PUT".equals(request.getMethod()) && ("/" + customIndex + "/_mapping").equals(request.getEndpoint());
            } catch (Exception e) {
                return false;
            }
        }));
        assertEquals(
            AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD,
            headerValue(capturedMappingRequest.get(), AwsSigV4RequestHeaders.CONTENT_SHA256)
        );
    }

    public void testUpdateResolvesMappingEndpointFromConfigMetadata() throws IOException {
        DataPlaneClientFactory apiFactory = new DataPlaneClientFactory() {
            @Override
            public RestClient getClient(String tenantId, String dataSourceId) {
                throw new AssertionError("custom result mapping sweep should not resolve through the API data-plane factory");
            }
        };
        ADSdkDataManagement configStoreRoutedDataManagement = new ADSdkDataManagement(
            mock(Client.class),
            NamedXContentRegistry.EMPTY,
            dataManagementSettings().build(),
            clusterService,
            configDocumentStore,
            apiFactory
        );
        SearchResponse searchResponse = searchResponseWithSource(configSource(RESULT_ALIAS, OTHER_TENANT_ID, OTHER_DATA_SOURCE_ID));
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponse);
            return null;
        }).when(configDocumentStore).search(any(), any(TenantContext.class), any());
        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if ("PUT".equals(request.getMethod()) && ("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint())) {
                return response(200);
            }
            fail("Unexpected synchronous request: " + request);
            return null;
        });
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/_alias/" + RESULT_ALIAS).equals(request.getEndpoint())) {
                listener.onFailure(responseException(404, null));
            } else if (("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint())) {
                listener.onSuccess(response(200, "{\"" + RESULT_ALIAS + "\":{\"mappings\":{}}}"));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        configStoreRoutedDataManagement.update(null);

        assertEquals("application-b", RESOLVED_APPLICATION_ID.get());
        assertEquals(OTHER_DATA_SOURCE_ID, RESOLVED_DATA_SOURCE_ID.get());
        verify(restClient).performRequest(argThat(request -> {
            try {
                return "PUT".equals(request.getMethod()) && ("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint());
            } catch (Exception e) {
                return false;
            }
        }));
    }

    public void testUpdateSkipsWhenMaxRetryCountReached() throws Exception {
        getPrivateMap("updateRunningTimes", AtomicInteger.class)
            .put(TENANT_ID, new AtomicInteger(TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES));

        dataManagement.update(TENANT_ID);

        verifyNoInteractions(configDocumentStore);
    }

    public void testUpdateSkipsWhenTenantUpdateAlreadyRunning() throws Exception {
        getPrivateMap("updateRunning", AtomicBoolean.class).put(TENANT_ID, new AtomicBoolean(true));

        dataManagement.update(TENANT_ID);

        verifyNoInteractions(configDocumentStore);
    }

    public void testUpdateSkipsWhenSemaphorePermitUnavailable() throws Exception {
        setPrivateField("updateRunningSemaphore", new Semaphore(0));

        dataManagement.update(TENANT_ID);

        verifyNoInteractions(configDocumentStore);
        assertFalse(getPrivateMap("updateRunning", AtomicBoolean.class).get(TENANT_ID).get());
    }

    public void testUpdateTreatsMissingConfigIndexAsEmptyForSystemWideTenant() {
        AtomicReference<TenantContext> tenantContext = new AtomicReference<>();
        doAnswer(invocation -> {
            tenantContext.set(invocation.getArgument(1));
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onFailure(new OpenSearchStatusException("[index_not_found_exception] no such index", RestStatus.NOT_FOUND));
            return null;
        }).when(configDocumentStore).search(any(), any(TenantContext.class), any());

        dataManagement.update(null);
        dataManagement.update(null);

        verify(configDocumentStore, times(1)).search(any(), any(TenantContext.class), any());
        assertTrue(tenantContext.get().isSystemWide());
    }

    public void testUpdateRetriesAfterGenericConfigSearchFailure() {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalStateException("search failure"));
            return null;
        }).when(configDocumentStore).search(any(), any(TenantContext.class), any());

        dataManagement.update(TENANT_ID);
        dataManagement.update(TENANT_ID);

        verify(configDocumentStore, times(2)).search(any(), any(TenantContext.class), any());
    }

    public void testUpdateSkipsPutMappingWhenExistingMappingIsValid() throws Exception {
        ADSdkDataManagement spy = spy(dataManagement);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponseWithResultIndices(RESULT_ALIAS, "unrelated-index", RESULT_ALIAS));
            return null;
        }).when(configDocumentStore).search(any(), any(TenantContext.class), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(spy).validateResultIndexMapping(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());

        spy.update(TENANT_ID);
        spy.update(TENANT_ID);

        verify(configDocumentStore, times(1)).search(any(), any(TenantContext.class), any());
        verify(spy).validateResultIndexMapping(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        verify(restClient, never()).performRequest(any(Request.class));
    }

    public void testUpdateMappingFailureStillCompletesGroupedListener() throws IOException {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            listener.onResponse(searchResponseWithResultIndices(RESULT_ALIAS));
            return null;
        }).when(configDocumentStore).search(any(), any(TenantContext.class), any());

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if ("HEAD".equals(request.getMethod())) {
                return response(404);
            }
            if ("PUT".equals(request.getMethod()) && ("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint())) {
                return response(500);
            }
            fail("Unexpected synchronous request: " + request);
            return null;
        });
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/_alias/" + RESULT_ALIAS).equals(request.getEndpoint())) {
                listener.onFailure(responseException(404, null));
            } else if (("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint())) {
                listener.onSuccess(response(200, "{\"" + RESULT_ALIAS + "\":{\"mappings\":{}}}"));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        dataManagement.update(TENANT_ID);
        dataManagement.update(TENANT_ID);

        verify(configDocumentStore, times(1)).search(any(), any(TenantContext.class), any());
        verify(restClient, times(1)).performRequest(argThat(request -> {
            try {
                return "PUT".equals(request.getMethod()) && ("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint());
            } catch (Exception e) {
                return false;
            }
        }));
    }

    public void testValidateResultIndexAndExecuteValidatesMappingBeforeExecuting() {
        ADSdkDataManagement spy = spy(dataManagement);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        ExecutorFunction function = () -> future.onResponse(null);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(spy).validateResultIndexMapping(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if ("PUT".equals(request.getMethod())) {
                assertEquals(AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD, headerValue(request, AwsSigV4RequestHeaders.CONTENT_SHA256));
                listener.onSuccess(response(201));
            } else if ("DELETE".equals(request.getMethod())) {
                listener.onSuccess(response(200));
            } else {
                fail("Unexpected method: " + request.getMethod());
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        spy.validateResultIndexAndExecute(RESULT_ALIAS, function, false, future, TENANT_ID);

        assertNull(future.actionGet());
    }

    public void testValidateResultIndexAndExecuteRejectsInvalidMappingWhenValidationRequired() {
        ADSdkDataManagement spy = spy(dataManagement);
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> mappingListener = invocation.getArgument(1);
            mappingListener.onResponse(false);
            return null;
        }).when(spy).validateResultIndexMapping(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());

        spy.validateResultIndexAndExecute(RESULT_ALIAS, function, false, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue().getMessage(), containsString(RESULT_ALIAS));
        verifyNoInteractions(function);
    }

    public void testValidateResultIndexAndExecuteReportsWriteFailure() {
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener responseListener = invocation.getArgument(1);
            assertEquals("PUT", request.getMethod());
            responseListener.onSuccess(response(500, "{\"error\":\"bad_request\"}"));
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        dataManagement.validateResultIndexAndExecute(RESULT_ALIAS, function, true, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue().getMessage(), containsString("Failed to write dummy result"));
        verifyNoInteractions(function);
    }

    public void testValidateResultIndexAndExecuteReportsDeleteRequestFailure() {
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener responseListener = invocation.getArgument(1);
            if ("PUT".equals(request.getMethod())) {
                responseListener.onSuccess(response(201, "{\"result\":\"created\"}"));
            } else if ("DELETE".equals(request.getMethod())) {
                responseListener.onFailure(new IllegalStateException("delete failure"));
            } else {
                fail("Unexpected method: " + request.getMethod());
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        dataManagement.validateResultIndexAndExecute(RESULT_ALIAS, function, true, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue().getCause().getMessage(), containsString("delete failure"));
        verifyNoInteractions(function);
    }

    public void testValidateResultIndexAndExecuteRetriesConcreteIndexAfterAliasDeleteIndexNotFound() throws IOException {
        String concreteIndex = RESULT_ALIAS + "-history-2026.04.28-1";
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);
        List<String> deleteEndpoints = new ArrayList<>();

        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener responseListener = invocation.getArgument(1);
            if ("PUT".equals(request.getMethod())) {
                responseListener.onSuccess(response(201, "{\"_index\":\"" + concreteIndex + "\",\"result\":\"created\"}"));
            } else if ("DELETE".equals(request.getMethod())) {
                deleteEndpoints.add(request.getEndpoint());
                if (request.getEndpoint().startsWith("/" + RESULT_ALIAS + "/")) {
                    responseListener
                        .onFailure(
                            responseException(
                                404,
                                "{\"error\":{\"type\":\"index_not_found_exception\",\"reason\":\"no such index\"},\"status\":404}"
                            )
                        );
                } else if (request.getEndpoint().startsWith("/" + concreteIndex + "/")) {
                    responseListener.onSuccess(response(200));
                } else {
                    fail("Unexpected delete endpoint: " + request.getEndpoint());
                }
            } else {
                fail("Unexpected method: " + request.getMethod());
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        dataManagement.validateResultIndexAndExecute(RESULT_ALIAS, function, true, listener, TENANT_ID);

        assertEquals(2, deleteEndpoints.size());
        assertTrue(deleteEndpoints.get(0).startsWith("/" + RESULT_ALIAS + "/_doc/dummy_ad_result_id-"));
        assertTrue(deleteEndpoints.get(1).startsWith("/" + concreteIndex + "/_doc/dummy_ad_result_id-"));
        verify(function).execute();
        verify(listener, never()).onFailure(any());
    }

    public void testValidateResultIndexAndExecutePropagatesExecutorFailure() {
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);
        doAnswer(invocation -> { throw new IllegalStateException("execute failure"); }).when(function).execute();

        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener responseListener = invocation.getArgument(1);
            if ("PUT".equals(request.getMethod())) {
                responseListener.onSuccess(response(201));
            } else if ("DELETE".equals(request.getMethod())) {
                responseListener.onSuccess(response(200));
            } else {
                fail("Unexpected method: " + request.getMethod());
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        dataManagement.validateResultIndexAndExecute(RESULT_ALIAS, function, true, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertEquals("execute failure", exceptionCaptor.getValue().getMessage());
    }

    public void testValidateResultIndexMappingFallsBackWhenAliasResolutionReturnsNon200() throws Exception {
        AtomicReference<String> mappingEndpoint = new AtomicReference<>();
        Response aliasExists = response(200);
        when(restClient.performRequest(any(Request.class))).thenReturn(aliasExists);
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/_alias/" + RESULT_ALIAS).equals(request.getEndpoint())) {
                listener.onSuccess(response(500));
            } else if (("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint())) {
                mappingEndpoint.set(request.getEndpoint());
                listener.onSuccess(response(200, validMappingResponseBody(RESULT_ALIAS)));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        dataManagement.validateResultIndexMapping(RESULT_ALIAS, future, TENANT_ID, null);

        assertTrue(future.actionGet());
        assertEquals("/" + RESULT_ALIAS + "/_mapping", mappingEndpoint.get());
    }

    public void testValidateResultIndexMappingFallsBackWhenAliasResolutionFails() throws Exception {
        AtomicReference<String> mappingEndpoint = new AtomicReference<>();
        Response aliasExists = response(200);
        when(restClient.performRequest(any(Request.class))).thenReturn(aliasExists);
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            if (("/_alias/" + RESULT_ALIAS).equals(request.getEndpoint())) {
                listener.onFailure(new IllegalStateException("alias lookup failure"));
            } else if (("/" + RESULT_ALIAS + "/_mapping").equals(request.getEndpoint())) {
                mappingEndpoint.set(request.getEndpoint());
                listener.onSuccess(response(200, validMappingResponseBody(RESULT_ALIAS)));
            } else {
                fail("Unexpected async request: " + request);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        dataManagement.validateResultIndexMapping(RESULT_ALIAS, future, TENANT_ID, null);

        assertTrue(future.actionGet());
        assertEquals("/" + RESULT_ALIAS + "/_mapping", mappingEndpoint.get());
    }

    public void testValidateResultIndexMappingReturnsFalseWhenMappingStatusIsNot2xx() throws Exception {
        doReturn(response(404)).when(restClient).performRequest(any(Request.class));
        doAnswer(invocation -> {
            ResponseListener listener = invocation.getArgument(1);
            listener.onSuccess(response(500));
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        dataManagement.validateResultIndexMapping(RESULT_ALIAS, future, TENANT_ID, null);

        assertFalse(future.actionGet());
    }

    public void testValidateResultIndexMappingReturnsFalseWhenIndexRootIsMissing() throws Exception {
        doReturn(response(404)).when(restClient).performRequest(any(Request.class));
        doAnswer(invocation -> {
            ResponseListener listener = invocation.getArgument(1);
            listener.onSuccess(response(200, "{}"));
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        dataManagement.validateResultIndexMapping(RESULT_ALIAS, future, TENANT_ID, null);

        assertFalse(future.actionGet());
    }

    public void testInitCustomResultIndexDirectlyReportsNullResponseAsFailure() throws IOException {
        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        when(restClient.performRequest(any(Request.class))).thenThrow(new IOException("transport failure"));

        dataManagement.initCustomResultIndexDirectly(RESULT_ALIAS, listener, TENANT_ID, null);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue().getMessage(), containsString("failed with status null"));
    }

    public void testInitFlattenedResultIndexTreatsConflictAsAlreadyExists() throws Exception {
        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        doReturn(response(409)).when(restClient).performRequest(any(Request.class));

        dataManagement.initFlattenedResultIndex(RESULT_ALIAS, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue() instanceof ResourceAlreadyExistsException);
    }

    public void testValidateCustomIndexForBackendJobReportsValidationFailureBeforeExecution() {
        ADSdkDataManagement spy = spy(dataManagement);
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> existsListener = invocation.getArgument(1);
            existsListener.onResponse(true);
            return null;
        }).when(spy).doesResultIndexOrAliasExists(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> mappingListener = invocation.getArgument(1);
            mappingListener.onResponse(true);
            return null;
        }).when(spy).validateResultIndexMapping(eq(RESULT_ALIAS), any(), eq(TENANT_ID), any());
        doAnswer(invocation -> { throw new IllegalStateException("validation boom"); })
            .when(spy)
            .validateResultIndexAndExecute(eq(RESULT_ALIAS), any(ExecutorFunction.class), eq(true), any(), eq(TENANT_ID), any());

        spy.validateCustomIndexForBackendJob(RESULT_ALIAS, "config-id", "test-user", List.of("role-a"), function, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertEquals("validation boom", exceptionCaptor.getValue().getMessage());
        verifyNoInteractions(function);
    }

    public void testDoesJobIndexExistAndInitJobIndex() {
        PlainActionFuture<CreateIndexResponse> future = PlainActionFuture.newFuture();

        assertTrue(dataManagement.doesJobIndexExist());
        dataManagement.initJobIndex(future);

        assertEquals(".opendistro-anomaly-detector-jobs", future.actionGet().index());
    }

    public void testUnsupportedDefaultResultOperationsThrow() {
        PlainActionFuture<CreateIndexResponse> createFuture = PlainActionFuture.newFuture();
        PlainActionFuture<Void> validateFuture = PlainActionFuture.newFuture();

        expectThrows(UnsupportedOperationException.class, () -> dataManagement.initDefaultResultIndexDirectly(createFuture));
        expectThrows(
            UnsupportedOperationException.class,
            () -> dataManagement.validateDefaultResultIndexForBackendJob("config-id", "user", List.of("role-a"), () -> {}, validateFuture)
        );
    }

    private SearchResponse searchResponseWithResultIndices(String... resultIndices) {
        SearchHit[] hits = new SearchHit[resultIndices.length];
        for (int i = 0; i < resultIndices.length; i++) {
            String source = configSource(resultIndices[i], TENANT_ID, DATA_SOURCE_ID);
            hits[i] = new SearchHit(i).sourceRef(new BytesArray(source));
        }
        return searchResponseWithHits(hits);
    }

    private SearchResponse searchResponseWithSource(String source) {
        return searchResponseWithHits(new SearchHit[] { new SearchHit(0).sourceRef(new BytesArray(source)) });
    }

    private SearchResponse searchResponseWithHits(SearchHit[] hits) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(new SearchHits(hits, null, 1.0f));
        return searchResponse;
    }

    private String configSource(String resultIndex, String tenantId, String dataSourceId) {
        return "{"
            + "\""
            + Config.RESULT_INDEX_FIELD
            + "\":\""
            + resultIndex
            + "\",\""
            + CommonName.TENANT_ID_FIELD
            + "\":\""
            + tenantId
            + "\",\""
            + CommonName.DATA_SOURCE_ID_FIELD
            + "\":\""
            + dataSourceId
            + "\"}";
    }

    @SuppressWarnings("unchecked")
    private <T> Map<String, T> getPrivateMap(String fieldName, Class<T> valueType) throws Exception {
        Field field = SDKDataManagement.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Map<String, T>) field.get(dataManagement);
    }

    private void setPrivateField(String fieldName, Object value) throws Exception {
        Field field = SDKDataManagement.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(dataManagement, value);
    }

    private Response response(int statusCode) {
        return response(statusCode, null);
    }

    private Response response(int statusCode, String body) {
        Response response = mock(Response.class);
        BasicClassicHttpResponse httpResponse = new BasicClassicHttpResponse(statusCode);
        if (body != null) {
            httpResponse.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        }
        when(response.getStatusLine()).thenReturn(new StatusLine(httpResponse));
        when(response.getEntity()).thenReturn(httpResponse.getEntity());
        return response;
    }

    private String headerValue(Request request, String name) {
        for (Header header : request.getOptions().getHeaders()) {
            if (name.equalsIgnoreCase(header.getName())) {
                return header.getValue();
            }
        }
        return null;
    }

    private ResponseException responseException(int statusCode, String body) throws IOException {
        Response response = response(statusCode, body);
        when(response.getRequestLine()).thenReturn(new RequestLine("DELETE", "/result/_doc/dummy", HttpVersion.HTTP_1_1));
        when(response.getHost()).thenReturn(new HttpHost("https", "example.com"));
        when(response.hasWarnings()).thenReturn(false);
        return new ResponseException(response);
    }

    private ResponseException responseException(Response response, Request request) throws IOException {
        when(response.getRequestLine()).thenReturn(new RequestLine(request.getMethod(), request.getEndpoint(), HttpVersion.HTTP_1_1));
        when(response.getHost()).thenReturn(new HttpHost("https", "example.com"));
        when(response.hasWarnings()).thenReturn(false);
        return new ResponseException(response);
    }

    private void bridgeAsyncRestClientToSyncStub() throws IOException {
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            try {
                Response response = restClient.performRequest(request);
                if (response != null
                    && response.getStatusLine() != null
                    && response.getStatusLine().getStatusCode() >= 200
                    && response.getStatusLine().getStatusCode() < 300) {
                    listener.onSuccess(response);
                } else if (response != null) {
                    listener.onFailure(responseException(response, request));
                } else {
                    listener.onFailure(new IOException("null response"));
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));
    }

    private String aliasResponseBody(String alias, String concreteIndex) {
        return "{\"" + concreteIndex + "\":{\"aliases\":{\"" + alias + "\":{}}}}";
    }

    private String validMappingResponseBody(String indexName) {
        return "{\"" + indexName + "\":{\"mappings\":" + ADIndex.RESULT.getMapping() + "}}";
    }

    private Settings.Builder dataManagementSettings() {
        return Settings
            .builder()
            .put(
                AnomalyDetectorSettings.DATA_SOURCE_ENDPOINT_RESOLVER_FACTORY_CLASS.getKey(),
                TestDataSourceEndpointResolverFactory.class.getName()
            );
    }

    private Settings.Builder aossDataManagementSettings() {
        return dataManagementSettings()
            .put(AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME.getKey(), "aoss");
    }

    private ADSdkDataManagement newDataManagement(Settings.Builder settings) {
        return newDataManagement(settings.build());
    }

    private ADSdkDataManagement newDataManagement(Settings settings) {
        return new ADSdkDataManagement(mock(Client.class), NamedXContentRegistry.EMPTY, settings, clusterService, configDocumentStore);
    }

    @SuppressWarnings("unchecked")
    private void cacheRestClient(String endpoint, RestClient client) throws Exception {
        Field field = RestClientProvider.class.getDeclaredField("REST_CLIENTS");
        field.setAccessible(true);
        Cache<String, RestClient> cache = (Cache<String, RestClient>) field.get(null);
        cache.put(endpoint, client);
    }

    @SuppressWarnings("unchecked")
    private void invalidateRestClient(String endpoint) throws Exception {
        Field field = RestClientProvider.class.getDeclaredField("REST_CLIENTS");
        field.setAccessible(true);
        Cache<String, RestClient> cache = (Cache<String, RestClient>) field.get(null);
        cache.invalidate(endpoint);
    }

    public static class TestDataSourceEndpointResolverFactory implements DataSourceEndpointResolverFactory {
        @Override
        public DataSourceEndpointResolver create(Settings settings) {
            return new TestDataSourceEndpointResolver();
        }
    }

    public static class TestDataSourceEndpointResolver implements DataSourceEndpointResolver {
        @Override
        public String resolve(String applicationId, String dataSourceId) {
            RESOLVED_APPLICATION_ID.set(applicationId);
            RESOLVED_DATA_SOURCE_ID.set(dataSourceId);
            return ENDPOINT;
        }
    }

    private static class CapturingDataPlaneClientFactory implements DataPlaneClientFactory {
        private final RestClient restClient;
        private final AtomicReference<String> tenantId = new AtomicReference<>();
        private final AtomicReference<String> dataSourceId = new AtomicReference<>();

        private CapturingDataPlaneClientFactory(RestClient restClient) {
            this.restClient = restClient;
        }

        @Override
        public RestClient getClient(String tenantId, String dataSourceId) {
            this.tenantId.set(tenantId);
            this.dataSourceId.set(dataSourceId);
            return restClient;
        }
    }
}
