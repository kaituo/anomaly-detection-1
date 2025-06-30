/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.AbstractTimeSeriesTest;

public class SDKNodeFilterTests extends AbstractTimeSeriesTest {

    public void testHasGlobalBlockTreatsMissingClusterBlocksApiAsUnblocked() throws Exception {
        RestClient restClient = restClientFailingWith(responseException(404));
        SDKNodeFilter nodeFilter = new SDKNodeFilter(Settings.EMPTY, tenantId -> restClient);

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        nodeFilter.hasGlobalBlock("tenant", future);

        assertFalse(future.actionGet());
    }

    public void testAossDataPlaneSkipsClusterBlocksApi() throws Exception {
        AtomicInteger clientCalls = new AtomicInteger();
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME.getKey(), "aoss").build();
        SDKNodeFilter nodeFilter = new SDKNodeFilter(settings, tenantId -> {
            clientCalls.incrementAndGet();
            return mock(RestClient.class);
        });

        PlainActionFuture<Boolean> globalBlockFuture = PlainActionFuture.newFuture();
        nodeFilter.hasGlobalBlock("tenant", globalBlockFuture);
        assertFalse(globalBlockFuture.actionGet());

        PlainActionFuture<Boolean> indicesBlockFuture = PlainActionFuture.newFuture();
        nodeFilter.hasIndicesBlock("tenant", ClusterBlockLevel.WRITE, new String[] { "result-index" }, indicesBlockFuture);
        assertFalse(indicesBlockFuture.actionGet());
        assertEquals(0, clientCalls.get());
    }

    public void testHasIndicesBlockTreatsMissingClusterBlocksApiAsUnblocked() throws Exception {
        RestClient restClient = restClientFailingWith(responseException(404));
        SDKNodeFilter nodeFilter = new SDKNodeFilter(Settings.EMPTY, tenantId -> restClient);

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        nodeFilter.hasIndicesBlock("tenant", ClusterBlockLevel.WRITE, new String[] { "result-index" }, future);

        assertFalse(future.actionGet());
    }

    public void testHasGlobalBlockTreatsForbiddenAsUnblocked() throws Exception {
        // AOSS data-access policies frequently surface as 403 for routes that aren't in the supported set.
        RestClient restClient = restClientFailingWith(responseException(403));
        SDKNodeFilter nodeFilter = new SDKNodeFilter(Settings.EMPTY, tenantId -> restClient);

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        nodeFilter.hasGlobalBlock("tenant", future);

        assertFalse(future.actionGet());
    }

    public void testHasGlobalBlockTreatsBadRequestSecurityExceptionAsUnblocked() throws Exception {
        // Some AOSS / security-plugin shapes return 400 with a security_exception body
        // when the route is not permitted. Treat that as "no block" too.
        String body = "{\"error\":{\"type\":\"security_exception\",\"reason\":\"no permissions for [cluster:monitor/state]\"}}";
        RestClient restClient = restClientFailingWith(responseException(400, body));
        SDKNodeFilter nodeFilter = new SDKNodeFilter(Settings.EMPTY, tenantId -> restClient);

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        nodeFilter.hasGlobalBlock("tenant", future);

        assertFalse(future.actionGet());
    }

    public void testHasGlobalBlockPropagatesGenericBadRequest() throws Exception {
        // 400 without an unsupported-API marker should still surface to the caller.
        String body = "{\"error\":{\"type\":\"parse_exception\",\"reason\":\"unexpected token\"}}";
        RestClient restClient = restClientFailingWith(responseException(400, body));
        SDKNodeFilter nodeFilter = new SDKNodeFilter(Settings.EMPTY, tenantId -> restClient);

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        nodeFilter.hasGlobalBlock("tenant", future);

        Exception exception = expectThrows(Exception.class, future::actionGet);
        assertTrue(hasResponseExceptionStatus(exception, 400));
    }

    public void testHasGlobalBlockPropagatesOtherFailures() throws Exception {
        RestClient restClient = restClientFailingWith(responseException(500));
        SDKNodeFilter nodeFilter = new SDKNodeFilter(Settings.EMPTY, tenantId -> restClient);

        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        nodeFilter.hasGlobalBlock("tenant", future);

        Exception exception = expectThrows(Exception.class, future::actionGet);
        assertTrue(hasResponseExceptionStatus(exception, 500));
    }

    private RestClient restClientFailingWith(Exception exception) throws IOException {
        RestClient restClient = mock(RestClient.class);
        doAnswer(invocation -> {
            ResponseListener listener = invocation.getArgument(1);
            listener.onFailure(exception);
            return null;
        }).when(restClient).performRequestAsync(any(Request.class), any(ResponseListener.class));
        return restClient;
    }

    private ResponseException responseException(int statusCode) throws IOException {
        return responseException(statusCode, null);
    }

    private ResponseException responseException(int statusCode, String body) throws IOException {
        Response response = mock(Response.class);
        BasicClassicHttpResponse httpResponse = new BasicClassicHttpResponse(statusCode);
        when(response.getRequestLine()).thenReturn(new RequestLine("GET", "/_cluster/state/blocks", HttpVersion.HTTP_1_1));
        when(response.getHost()).thenReturn(new HttpHost("https", "example.com"));
        when(response.getStatusLine()).thenReturn(new StatusLine(httpResponse));
        when(response.hasWarnings()).thenReturn(false);
        if (body != null) {
            HttpEntity entity = new StringEntity(body, ContentType.APPLICATION_JSON);
            when(response.getEntity()).thenReturn(entity);
        } else {
            when(response.getEntity()).thenReturn(null);
        }
        return new ResponseException(response);
    }

    private boolean hasResponseExceptionStatus(Throwable throwable, int statusCode) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ResponseException) {
                return ((ResponseException) current).getResponse().getStatusLine().getStatusCode() == statusCode;
            }
            current = current.getCause();
        }
        return false;
    }
}
