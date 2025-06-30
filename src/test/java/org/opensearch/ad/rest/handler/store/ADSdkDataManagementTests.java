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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.http.message.StatusLine;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.rest.handler.store.SDKDataManagement;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.Client;

import com.google.common.cache.Cache;

public class ADSdkDataManagementTests extends AbstractTimeSeriesTest {
    private static final String ENDPOINT = "http://127.0.0.1:9201";
    private static final String TENANT_ID = "tenant-a";
    private static final String RESULT_ALIAS = "opensearch-ad-plugin-result-sdk-branch";

    private RestClient restClient;
    private SdkClient sdkClient;
    private ClusterService clusterService;
    private ADSdkDataManagement dataManagement;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        System.setProperty("opensearch.tenant.endpoint", ENDPOINT);

        restClient = mock(RestClient.class);
        sdkClient = mock(SdkClient.class);
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TimeSeriesSettings.MAX_CONCURRENT_SDK_INDEX_MAPPING_UPDATES)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        cacheRestClient(ENDPOINT, restClient);

        dataManagement = new ADSdkDataManagement(mock(Client.class), NamedXContentRegistry.EMPTY, Settings.EMPTY, clusterService);
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
        dataManagement.validateResultIndexMapping(alias, future, TENANT_ID);

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
        dataManagement.validateResultIndexMapping(alias, future, TENANT_ID);

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

        dataManagement.validateResultIndexMapping(RESULT_ALIAS, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue().getCause().getMessage(), containsString("mapping failure"));
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

    public void testInitCustomResultIndexDirectlyEncodesDateMathIndexName() throws Exception {
        AtomicReference<Request> capturedRequest = new AtomicReference<>();

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            capturedRequest.set(request);
            return response(200);
        });

        PlainActionFuture<CreateIndexResponse> future = PlainActionFuture.newFuture();
        dataManagement.initCustomResultIndexDirectly(RESULT_ALIAS, future, TENANT_ID);

        assertTrue(future.actionGet().isAcknowledged());
        String expectedIndexName = TimeSeriesIndex.getCustomResultIndexPattern(RESULT_ALIAS);
        Request request = capturedRequest.get();
        assertNotNull(request);
        assertEquals("PUT", request.getMethod());
        assertEquals("/" + URLEncoder.encode(expectedIndexName, StandardCharsets.UTF_8), request.getEndpoint());
        assertThat(EntityUtils.toString(request.getEntity()), containsString("\"aliases\":{\"" + RESULT_ALIAS + "\":{}}"));
    }

    public void testInitCustomResultIndexDirectlyTreatsResourceAlreadyExistsBodyAsAlreadyExists() throws IOException {
        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        Response alreadyExists = response(400, "{\"error\":{\"type\":\"resource_already_exists_exception\"}}");
        when(restClient.performRequest(any(Request.class))).thenReturn(alreadyExists);

        dataManagement.initCustomResultIndexDirectly(RESULT_ALIAS, listener, TENANT_ID);

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

        doReturn(false).when(spy).doesResultIndexExists(RESULT_ALIAS, TENANT_ID);
        doReturn(false).when(spy).doesResultAliasExists(RESULT_ALIAS, TENANT_ID);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(new ResourceAlreadyExistsException(RESULT_ALIAS));
            return null;
        }).when(spy).initCustomResultIndexDirectly(eq(RESULT_ALIAS), any(), eq(TENANT_ID));
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = invocation.getArgument(3);
            listener.onResponse(null);
            return null;
        }).when(spy).validateResultIndexAndExecute(eq(RESULT_ALIAS), same(function), eq(false), any(), eq(TENANT_ID));

        spy.initCustomResultIndexAndExecute(RESULT_ALIAS, function, future, TENANT_ID);

        assertNull(future.actionGet());
        verify(spy).validateResultIndexAndExecute(eq(RESULT_ALIAS), same(function), eq(false), any(), eq(TENANT_ID));
    }

    public void testValidateCustomIndexForBackendJobExecutesAfterCreatingMissingIndex() {
        ADSdkDataManagement spy = spy(dataManagement);
        ExecutorFunction function = mock(ExecutorFunction.class);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();

        doReturn(false).when(spy).doesResultIndexExists(RESULT_ALIAS, TENANT_ID);
        doReturn(false).when(spy).doesResultAliasExists(RESULT_ALIAS, TENANT_ID);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(new CreateIndexResponse(true, true, RESULT_ALIAS));
            return null;
        }).when(spy).initCustomResultIndexDirectly(eq(RESULT_ALIAS), any(), eq(TENANT_ID));
        doAnswer(invocation -> {
            ExecutorFunction executorFunction = invocation.getArgument(1);
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = invocation.getArgument(3);
            executorFunction.execute();
            listener.onResponse(null);
            return null;
        }).when(spy).validateResultIndexAndExecute(eq(RESULT_ALIAS), any(ExecutorFunction.class), eq(true), any(), eq(TENANT_ID));

        spy.validateCustomIndexForBackendJob(RESULT_ALIAS, "config-id", "test-user", List.of("role-a"), function, future, TENANT_ID);

        assertNull(future.actionGet());
        verify(function).execute();
        verify(spy).validateResultIndexAndExecute(eq(RESULT_ALIAS), any(ExecutorFunction.class), eq(true), any(), eq(TENANT_ID));
    }

    public void testValidateCustomIndexForBackendJobRejectsInvalidExistingMapping() {
        ADSdkDataManagement spy = spy(dataManagement);
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        doReturn(true).when(spy).doesResultIndexExists(RESULT_ALIAS, TENANT_ID);
        doReturn(false).when(spy).doesResultAliasExists(RESULT_ALIAS, TENANT_ID);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> mappingListener = invocation.getArgument(1);
            mappingListener.onResponse(false);
            return null;
        }).when(spy).validateResultIndexMapping(eq(RESULT_ALIAS), any(), eq(TENANT_ID));

        spy.validateCustomIndexForBackendJob(RESULT_ALIAS, "config-id", "test-user", List.of("role-a"), function, listener, TENANT_ID);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue() instanceof EndRunException);
        assertEquals("Result index mapping is not correct", exceptionCaptor.getValue().getMessage());
        verifyNoInteractions(function);
    }

    public void testUpdateSkipsFurtherWorkAfterSuccessfulEmptySearch() {
        injectSdkClient(dataManagement, sdkClient);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(null);
        when(sdkClient.searchDataObjectAsync(any()))
            .thenReturn(CompletableFuture.completedFuture(new SearchDataObjectResponse(searchResponse)));

        dataManagement.update(TENANT_ID);
        dataManagement.update(TENANT_ID);

        verify(sdkClient, times(1)).searchDataObjectAsync(any());
    }

    public void testUpdateSkipsFurtherWorkAfterEmptySdkParseFailure() {
        injectSdkClient(dataManagement, sdkClient);
        SearchDataObjectResponse sdkResponse = mock(SearchDataObjectResponse.class);
        when(sdkResponse.searchResponse()).thenThrow(new AssertionError("malformed empty search response total: 0"));
        when(sdkClient.searchDataObjectAsync(any())).thenReturn(CompletableFuture.completedFuture(sdkResponse));

        dataManagement.update(TENANT_ID);
        dataManagement.update(TENANT_ID);

        verify(sdkClient, times(1)).searchDataObjectAsync(any());
    }

    public void testUpdateInvalidMappingTriggersPutMapping() throws IOException {
        injectSdkClient(dataManagement, sdkClient);
        String customIndex = RESULT_ALIAS;
        SearchHit searchHit = new SearchHit(0).sourceRef(new BytesArray("{\"" + Config.RESULT_INDEX_FIELD + "\":\"" + customIndex + "\"}"));
        SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, null, 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(sdkClient.searchDataObjectAsync(any()))
            .thenReturn(CompletableFuture.completedFuture(new SearchDataObjectResponse(searchResponse)));

        when(restClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            if ("HEAD".equals(request.getMethod())) {
                return response(404);
            }
            if ("PUT".equals(request.getMethod()) && ("/" + customIndex + "/_mapping").equals(request.getEndpoint())) {
                return response(200);
            }
            fail("Unexpected synchronous request: " + request);
            return null;
        });
        doAnswer(invocation -> {
            Request request = invocation.getArgument(0);
            ResponseListener listener = invocation.getArgument(1);
            assertEquals("/" + customIndex + "/_mapping", request.getEndpoint());
            listener.onSuccess(response(200, "{\"" + customIndex + "\":{\"mappings\":{}}}"));
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

    private String aliasResponseBody(String alias, String concreteIndex) {
        return "{\"" + concreteIndex + "\":{\"aliases\":{\"" + alias + "\":{}}}}";
    }

    private String validMappingResponseBody(String indexName) {
        return "{\"" + indexName + "\":{\"mappings\":" + ADIndex.RESULT.getMapping() + "}}";
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

    private void injectSdkClient(ADSdkDataManagement target, SdkClient sdkClient) {
        try {
            Field field = SDKDataManagement.class.getDeclaredField("sdkClient");
            field.setAccessible(true);
            field.set(target, sdkClient);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
