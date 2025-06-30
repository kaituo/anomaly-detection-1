/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.net.URI;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.hc.core5.http.protocol.BasicHttpContext;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class AwsSigV4RequestInterceptorTests extends OpenSearchTestCase {
    private static final String DATA_SOURCE_URL_CONTEXT_KEY = "data-source-url";

    public void testUsesThreadContextCredentialsWhenComplete() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SESSION_TOKEN_CONTEXT_KEY, "user-token");
        threadContext.putTransient(DATA_SOURCE_URL_CONTEXT_KEY, "https://collection.us-east-1.aoss.amazonaws.com");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_REGION_CONTEXT_KEY, "us-east-1");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SERVICE_NAME_CONTEXT_KEY, "es");

        BasicClassicHttpRequest request = new BasicClassicHttpRequest(
            "GET",
            URI.create("https://collection.us-east-1.aoss.amazonaws.com/_search")
        );
        interceptor(threadContext).process(request, null, new BasicHttpContext());

        String authorization = headerValue(request, "Authorization");
        assertTrue(authorization, authorization.contains("Credential=user-access/"));
        assertTrue(authorization, authorization.contains("/us-east-1/es/aws4_request"));
        assertEquals("user-token", headerValue(request, "X-Amz-Security-Token"));
    }

    public void testFallsBackToServiceCredentialsWhenThreadContextIncomplete() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, "user-access");
        threadContext.putTransient(AwsSigV4ThreadContext.AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, "user-secret");

        BasicClassicHttpRequest request = new BasicClassicHttpRequest(
            "GET",
            URI.create("https://collection.us-west-2.aoss.amazonaws.com/_search")
        );
        interceptor(threadContext).process(request, null, new BasicHttpContext());

        String authorization = headerValue(request, "Authorization");
        assertTrue(authorization, authorization.contains("Credential=service-access/"));
        assertTrue(authorization, authorization.contains("/us-west-2/aoss/aws4_request"));
        assertEquals("service-token", headerValue(request, "X-Amz-Security-Token"));
    }

    private AwsSigV4RequestInterceptor interceptor(ThreadContext threadContext) {
        return new AwsSigV4RequestInterceptor(
            "us-west-2",
            "aoss",
            threadContext,
            new AwsSigV4ThreadContext(DATA_SOURCE_URL_CONTEXT_KEY),
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
}
