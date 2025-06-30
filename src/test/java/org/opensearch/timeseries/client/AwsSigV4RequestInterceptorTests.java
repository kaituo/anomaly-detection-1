/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.hc.core5.http.message.BasicHttpRequest;
import org.apache.hc.core5.http.protocol.BasicHttpContext;
import org.opensearch.client.Request;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class AwsSigV4RequestInterceptorTests extends OpenSearchTestCase {
    private static final String DATA_SOURCE_URL_CONTEXT_KEY = "data-source-url";
    private static final String DATA_SOURCE_URL = "https://collection.us-east-1.aoss.amazonaws.com";
    private static final String TENANT_ID = "account-1:application-1:workspace-1";
    private static final String DATA_SOURCE_ID = "data-source-1";

    public void testUsesPreparedRequestSigningMaterialWhenComplete() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SESSION_TOKEN_CONTEXT_KEY, "user-token");
        threadContext.putTransient(DATA_SOURCE_URL_CONTEXT_KEY, DATA_SOURCE_URL);
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SERVICE_NAME_CONTEXT_KEY, "es");

        BasicClassicHttpRequest request = new BasicClassicHttpRequest("GET", URI.create(DATA_SOURCE_URL + "/_search"));
        Request restRequest = new Request("GET", "/_search");
        try (Releasable ignored = prepareRequest(threadContext, restRequest)) {
            copyRequestOptionsHeaders(restRequest, request);
            interceptor("us-east-1", "aoss").process(request, null, new BasicHttpContext());
        } finally {
            SigningRestClientProvider.closeAll();
        }

        String authorization = headerValue(request, "Authorization");
        assertTrue(authorization, authorization.contains("Credential=user-access/"));
        assertTrue(authorization, authorization.contains("/us-east-1/es/aws4_request"));
        assertEquals("user-token", headerValue(request, "X-Amz-Security-Token"));
        assertNull(headerValue(request, AwsSigV4RequestInterceptor.REQUEST_SIGNING_MATERIAL_ID_HEADER));
    }

    public void testFallsBackToServiceCredentialsWhenRequestSigningMaterialIncomplete() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");
        threadContext.putTransient(DATA_SOURCE_URL_CONTEXT_KEY, DATA_SOURCE_URL);

        BasicClassicHttpRequest request = new BasicClassicHttpRequest("GET", URI.create(DATA_SOURCE_URL + "/_search"));
        Request restRequest = new Request("GET", "/_search");
        try (Releasable ignored = prepareRequest(threadContext, restRequest)) {
            copyRequestOptionsHeaders(restRequest, request);
            interceptor("us-west-2", "aoss").process(request, null, new BasicHttpContext());
        } finally {
            SigningRestClientProvider.closeAll();
        }

        String authorization = headerValue(request, "Authorization");
        assertTrue(authorization, authorization.contains("Credential=service-access/"));
        assertTrue(authorization, authorization.contains("/us-west-2/aoss/aws4_request"));
        assertEquals("service-token", headerValue(request, "X-Amz-Security-Token"));
    }

    public void testUnsignedPayloadHeaderUsesUnsignedPayloadSigner() throws Exception {
        BasicClassicHttpRequest request = new BasicClassicHttpRequest("POST", URI.create("http://localhost:9200/_bulk"));
        request.setEntity(new StringEntity("{\"field\":\"value\"}", ContentType.APPLICATION_JSON));
        request.addHeader(AwsSigV4RequestHeaders.CONTENT_SHA256, AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD);

        interceptor("us-east-1", "aoss").process(request, null, new BasicHttpContext());

        assertFalse(AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD.equals(headerValue(request, AwsSigV4RequestHeaders.CONTENT_SHA256)));
        assertNotNull(headerValue(request, "Authorization"));
    }

    public void testAsyncRequestUsesPreparedBodyForPayloadHash() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SESSION_TOKEN_CONTEXT_KEY, "user-token");
        threadContext.putTransient(DATA_SOURCE_URL_CONTEXT_KEY, DATA_SOURCE_URL);
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SERVICE_NAME_CONTEXT_KEY, "aoss");
        String body = "{\"size\":0,\"aggregations\":{\"max_timefield\":{\"max\":{\"field\":\"@timestamp\"}}}}";

        BasicHttpRequest request = new BasicHttpRequest("POST", URI.create(DATA_SOURCE_URL + "/server-metrics/_search"));
        Request restRequest = new Request("POST", "/server-metrics/_search");
        restRequest.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        try (Releasable ignored = prepareRequest(threadContext, restRequest)) {
            copyRequestOptionsHeaders(restRequest, request);
            interceptor("us-east-1", "aoss").process(request, null, new BasicHttpContext());
        } finally {
            SigningRestClientProvider.closeAll();
        }

        assertEquals(sha256Hex(body), headerValue(request, AwsSigV4RequestHeaders.CONTENT_SHA256));
        assertNotNull(headerValue(request, "Authorization"));
        assertNull(headerValue(request, AwsSigV4RequestInterceptor.REQUEST_SIGNING_MATERIAL_ID_HEADER));
    }

    public void testAddUnsignedPayloadHeaderAddsHeaderOnce() {
        Request request = new Request("POST", "/_bulk");

        AwsSigV4RequestHeaders.addUnsignedPayloadHeader(request);
        AwsSigV4RequestHeaders.addUnsignedPayloadHeader(request);

        assertEquals(1, request.getOptions().getHeaders().size());
        assertEquals(AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD, headerValue(request, AwsSigV4RequestHeaders.CONTENT_SHA256));
    }

    public void testAddUnsignedPayloadHeaderDetectsExistingHeaderCaseInsensitively() {
        Request request = new Request("POST", "/_bulk");
        request.setOptions(request.getOptions().toBuilder().addHeader("X-Amz-Content-Sha256", AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD));

        AwsSigV4RequestHeaders.addUnsignedPayloadHeader(request);

        assertEquals(1, request.getOptions().getHeaders().size());
        assertEquals(AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD, headerValue(request, AwsSigV4RequestHeaders.CONTENT_SHA256));
    }

    public void testAddUnsignedPayloadHeaderRejectsNullRequest() {
        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> AwsSigV4RequestHeaders.addUnsignedPayloadHeader(null)
        );

        assertEquals("request must not be null", exception.getMessage());
    }

    public void testHasUnsignedPayloadHeaderForHttpRequest() {
        BasicClassicHttpRequest request = new BasicClassicHttpRequest("GET", URI.create("http://localhost:9200/_search"));

        assertFalse(AwsSigV4RequestHeaders.hasUnsignedPayloadHeader(request));

        request.addHeader(AwsSigV4RequestHeaders.CONTENT_SHA256, "different");
        assertFalse(AwsSigV4RequestHeaders.hasUnsignedPayloadHeader(request));

        request.setHeader(AwsSigV4RequestHeaders.CONTENT_SHA256, AwsSigV4RequestHeaders.UNSIGNED_PAYLOAD);
        assertTrue(AwsSigV4RequestHeaders.hasUnsignedPayloadHeader(request));
    }

    private Releasable prepareRequest(ThreadContext threadContext, Request request) {
        AossAwareDataPlaneClientFactory factory = new AossAwareDataPlaneClientFactory(
            "us-east-1",
            (applicationId, dataSourceId) -> DATA_SOURCE_URL,
            false,
            threadContext,
            new AwsSigV4ThreadContext(DATA_SOURCE_URL_CONTEXT_KEY),
            null,
            true
        );
        return factory.createRequestContext(TENANT_ID, DATA_SOURCE_ID).prepareRequest(request);
    }

    private void copyRequestOptionsHeaders(Request source, HttpRequest target) {
        for (Header header : source.getOptions().getHeaders()) {
            target.addHeader(header.getName(), header.getValue());
        }
    }

    private AwsSigV4RequestInterceptor interceptor(String defaultRegion, String defaultServiceName) {
        return new AwsSigV4RequestInterceptor(
            defaultRegion,
            defaultServiceName,
            StaticCredentialsProvider.create(AwsSessionCredentials.create("service-access", "service-secret", "service-token"))
        );
    }

    private String headerValue(HttpRequest request, String name) {
        for (Header header : request.getHeaders()) {
            if (name.equalsIgnoreCase(header.getName())) {
                return header.getValue();
            }
        }
        return null;
    }

    private String headerValue(Request request, String name) {
        for (Header header : request.getOptions().getHeaders()) {
            if (name.equalsIgnoreCase(header.getName())) {
                return header.getValue();
            }
        }
        return null;
    }

    private String sha256Hex(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        char[] chars = "0123456789abcdef".toCharArray();
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte next : digest) {
            hex.append(chars[(next >> 4) & 0x0F]);
            hex.append(chars[next & 0x0F]);
        }
        return hex.toString();
    }
}
