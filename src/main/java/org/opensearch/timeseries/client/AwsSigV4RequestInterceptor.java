/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.hc.client5.http.RouteInfo;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.timeseries.client.AwsSigV4ThreadContext.RequestSigningMaterial;
import org.opensearch.timeseries.util.SecurityUtil;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/**
 * Signs outbound direct AOSS requests with SigV4.
 */
public class AwsSigV4RequestInterceptor implements HttpRequestInterceptor {
    private static final Logger LOG = LogManager.getLogger(AwsSigV4RequestInterceptor.class);
    private static final String AOSS_SERVICE_NAME = "aoss";

    private final String defaultRegion;
    private final String defaultServiceName;
    private final ThreadContext threadContext;
    private final AwsSigV4ThreadContext sigV4ThreadContext;
    private final Aws4Signer signer;
    private final AwsCredentialsProvider serviceCredentialsProvider;

    public AwsSigV4RequestInterceptor(String region) {
        this(requireNonBlank(region, "region must not be null"), AOSS_SERVICE_NAME, null, null, SecurityUtil.createCredentialsProvider());
    }

    public AwsSigV4RequestInterceptor(String defaultRegion, ThreadContext threadContext, AwsSigV4ThreadContext sigV4ThreadContext) {
        this(defaultRegion, AOSS_SERVICE_NAME, threadContext, sigV4ThreadContext, SecurityUtil.createCredentialsProvider());
    }

    AwsSigV4RequestInterceptor(
        String defaultRegion,
        String defaultServiceName,
        ThreadContext threadContext,
        AwsSigV4ThreadContext sigV4ThreadContext,
        AwsCredentialsProvider serviceCredentialsProvider
    ) {
        this.defaultRegion = normalize(defaultRegion);
        this.defaultServiceName = AwsSigV4ThreadContext.hasText(defaultServiceName) ? defaultServiceName.trim() : AOSS_SERVICE_NAME;
        this.threadContext = threadContext;
        if (threadContext != null) {
            this.sigV4ThreadContext = Objects
                .requireNonNull(sigV4ThreadContext, "sigV4ThreadContext must not be null when threadContext is provided");
        } else {
            this.sigV4ThreadContext = sigV4ThreadContext;
        }
        this.signer = Aws4Signer.create();
        this.serviceCredentialsProvider = Objects.requireNonNull(serviceCredentialsProvider, "serviceCredentialsProvider must not be null");
    }

    @Override
    public void process(HttpRequest request, EntityDetails entityDetails, HttpContext context) {
        try {
            byte[] body = extractBody(request);
            SigningParameters signingParameters = resolveSigningParameters();
            SdkHttpFullRequest.Builder sdkRequest = SdkHttpFullRequest
                .builder()
                .uri(buildSigningUri(request, context))
                .method(SdkHttpMethod.fromValue(request.getMethod()))
                .contentStreamProvider(() -> new ByteArrayInputStream(body));

            for (org.apache.hc.core5.http.Header header : request.getHeaders()) {
                String name = header.getName().toLowerCase(Locale.ROOT);
                if ("authorization".equals(name) || "content-length".equals(name) || "host".equals(name)) {
                    continue;
                }
                sdkRequest.appendHeader(header.getName(), header.getValue());
            }

            SdkHttpFullRequest signedRequest = signer
                .sign(
                    sdkRequest.build(),
                    Aws4SignerParams
                        .builder()
                        .awsCredentials(signingParameters.credentials)
                        .signingName(signingParameters.serviceName)
                        .signingRegion(Region.of(signingParameters.region))
                        .build()
                );

            for (Map.Entry<String, List<String>> header : signedRequest.headers().entrySet()) {
                request.removeHeaders(header.getKey());
                for (String value : header.getValue()) {
                    request.addHeader(header.getKey(), value);
                }
            }
        } catch (Exception e) {
            LOG.error("SigV4 signing failed for AOSS request {}", request.getRequestUri(), e);
            throw new RuntimeException("SigV4 signing failed", e);
        }
    }

    private SigningParameters resolveSigningParameters() {
        RequestSigningMaterial requestSigningMaterial = sigV4ThreadContext == null ? null : sigV4ThreadContext.resolve(threadContext);
        if (requestSigningMaterial != null) {
            return new SigningParameters(
                requestSigningMaterial.credentials(),
                requestSigningMaterial.region(),
                requestSigningMaterial.serviceName()
            );
        }
        String region = requireNonBlank(defaultRegion, "default signing region must not be null or blank");
        return new SigningParameters(serviceCredentialsProvider.resolveCredentials(), region, defaultServiceName);
    }

    private byte[] extractBody(HttpRequest request) throws Exception {
        if (request instanceof ClassicHttpRequest == false) {
            return new byte[0];
        }
        HttpEntity entity = ((ClassicHttpRequest) request).getEntity();
        if (entity == null) {
            return new byte[0];
        }
        if (entity.isRepeatable() == false) {
            throw new IllegalStateException("Cannot SigV4 sign a non-repeatable request entity");
        }
        return EntityUtils.toByteArray(entity);
    }

    private URI buildSigningUri(HttpRequest request, HttpContext context) throws Exception {
        URI requestUri = request.getUri();
        if (requestUri.isAbsolute()) {
            return requestUri;
        }

        HttpHost targetHost = resolveTargetHost(request, context);
        return new URI(
            targetHost.getSchemeName(),
            null,
            targetHost.getHostName(),
            targetHost.getPort(),
            requestUri.getRawPath(),
            requestUri.getRawQuery(),
            null
        );
    }

    private HttpHost resolveTargetHost(HttpRequest request, HttpContext context) {
        RouteInfo route = HttpClientContext.adapt(context).getHttpRoute();
        if (route != null && route.getTargetHost() != null) {
            return route.getTargetHost();
        }
        if (request.getScheme() != null && request.getAuthority() != null) {
            return new HttpHost(request.getScheme(), request.getAuthority());
        }
        throw new IllegalStateException("Cannot resolve target host for SigV4 signing");
    }

    private static String requireNonBlank(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim();
    }

    private static final class SigningParameters {
        private final AwsCredentials credentials;
        private final String region;
        private final String serviceName;

        private SigningParameters(AwsCredentials credentials, String region, String serviceName) {
            this.credentials = Objects.requireNonNull(credentials, "credentials must not be null");
            this.region = requireNonBlank(region, "signing region must not be null or blank");
            this.serviceName = requireNonBlank(serviceName, "signing service name must not be null or blank");
        }
    }
}
