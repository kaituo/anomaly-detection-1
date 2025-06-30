/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hc.client5.http.RouteInfo;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.client.Request;
import org.opensearch.common.lease.Releasable;
import org.opensearch.timeseries.client.AwsSigV4ThreadContext.RequestSigningMaterial;
import org.opensearch.timeseries.util.SecurityUtil;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.Aws4UnsignedPayloadSigner;
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
    /*
     * Internal correlation header used to bridge request-scoped SigV4 material from the high-level
     * RestClient Request into this low-level Apache HTTP interceptor. The header carries only an
     * opaque UUID; the actual credentials and service name stay in REQUEST_SIGNING_MATERIALS and
     * are removed when the request preparation Releasable closes. This header is skipped when
     * building the AWS canonical signed headers, so it is never part of the outbound SigV4 request.
     * If the header is absent or the UUID no longer maps to material, signing falls back to service
     * credentials. The interceptor removes this internal header from the Apache request after
     * resolving the material so it is not sent downstream.
     */
    static final String REQUEST_SIGNING_MATERIAL_ID_HEADER = "x-opensearch-timeseries-sigv4-material-id";
    static final String REQUEST_SIGNING_MATERIAL_CONTEXT_ATTRIBUTE = AwsSigV4RequestInterceptor.class.getName() + ".requestSigningMaterial";
    private static final ConcurrentMap<String, RequestSigningMaterial> REQUEST_SIGNING_MATERIALS = new ConcurrentHashMap<>();

    private final String defaultRegion;
    private final String defaultServiceName;
    private final Aws4Signer signer;
    private final Aws4UnsignedPayloadSigner unsignedPayloadSigner;
    private final AwsCredentialsProvider serviceCredentialsProvider;

    public AwsSigV4RequestInterceptor(String region, String serviceName) {
        this(requireNonBlank(region, "region must not be null"), serviceName, SecurityUtil.createCredentialsProvider());
    }

    AwsSigV4RequestInterceptor(String defaultRegion, String defaultServiceName, AwsCredentialsProvider serviceCredentialsProvider) {
        this.defaultRegion = normalize(defaultRegion);
        this.defaultServiceName = AwsSigV4ThreadContext.hasText(defaultServiceName) ? defaultServiceName.trim() : AOSS_SERVICE_NAME;
        this.signer = Aws4Signer.create();
        this.unsignedPayloadSigner = Aws4UnsignedPayloadSigner.create();
        this.serviceCredentialsProvider = Objects.requireNonNull(serviceCredentialsProvider, "serviceCredentialsProvider must not be null");
    }

    static Releasable attachRequestSigningMaterial(Request request, RequestSigningMaterial requestSigningMaterial) {
        Objects.requireNonNull(request, "request must not be null");
        Objects.requireNonNull(requestSigningMaterial, "requestSigningMaterial must not be null");
        String materialId = UUID.randomUUID().toString();
        REQUEST_SIGNING_MATERIALS.put(materialId, requestSigningMaterial);
        request.setOptions(request.getOptions().toBuilder().addHeader(REQUEST_SIGNING_MATERIAL_ID_HEADER, materialId));
        return () -> REQUEST_SIGNING_MATERIALS.remove(materialId);
    }

    /**
     * Sign an outbound Apache HttpRequest with SigV4 using input context.
     *
     * The overall flow is:
     * <ol>
     *   <li>Build {@code sdkRequest} from {@code request} (URI, method, body, filtered headers)
     *       &mdash; lines 92&ndash;104.</li>
     *   <li>Hand {@code sdkRequest} to {@code Aws4Signer.sign(...)} to get back a
     *       {@code signedRequest} with SigV4 headers added &mdash; lines 106&ndash;115.</li>
     *   <li>Copy those SigV4 headers from {@code signedRequest} back onto the real Apache
     *       {@code request} so it goes out signed &mdash; lines 117&ndash;122.</li>
     * </ol>
     *
     * @param request the outbound Apache HttpRequest that the connection manager will serialize and send over the socket
     * @param entityDetails the entity details of the request
     * @param context the HTTP context
     */
    @Override
    public void process(HttpRequest request, EntityDetails entityDetails, HttpContext context) {
        try {
            byte[] body = extractBody(request);
            SigningParameters signingParameters = resolveSigningParameters(request, context);
            request.removeHeaders(REQUEST_SIGNING_MATERIAL_ID_HEADER);
            URI signingUri = buildSigningUri(request, context);
            SdkHttpFullRequest.Builder sdkRequest = SdkHttpFullRequest
                .builder()
                .uri(signingUri)
                .method(SdkHttpMethod.fromValue(request.getMethod()))
                .contentStreamProvider(() -> new ByteArrayInputStream(body));

            // Copy headers from the real outbound Apache HttpClient 5 `request` (the actual
            // HttpRequest that the connection manager will serialize and send over the socket,
            // built by the OpenSearch low-level RestClient and handed to this interceptor) into
            // the parallel AWS SDK v2 `sdkRequest` shadow object. The shadow object exists solely
            // so Aws4Signer can compute a SigV4 signature: the signer only consumes
            // SdkHttpFullRequest and cannot operate on Apache's types.
            //
            // shouldSkipSigningHeader filters out headers that must NOT participate in the
            // canonical signed-headers list:
            // - "authorization": signing the prior signature would be self-referential.
            // - "content-length" and "host": the signer recomputes/derives these itself from
            // the URI and body.
            // - REQUEST_SIGNING_MATERIAL_ID_HEADER: an internal correlation header used only
            // to look up per-request credentials; it must never appear in the outbound SigV4
            // request.
            for (Header header : request.getHeaders()) {
                String name = header.getName().toLowerCase(Locale.ROOT);
                if (shouldSkipSigningHeader(name)) {
                    continue;
                }
                sdkRequest.appendHeader(header.getName(), header.getValue());
            }

            SdkHttpFullRequest unsignedRequest = sdkRequest.build();
            Aws4SignerParams signerParams = Aws4SignerParams
                .builder()
                .awsCredentials(signingParameters.credentials)
                .signingName(signingParameters.serviceName)
                .signingRegion(Region.of(signingParameters.region))
                .build();
            // UNSIGNED-PAYLOAD means the canonical request uses the literal marker instead of
            // hashing the body. The request still needs SigV4 Authorization headers, so use the
            // unsigned-payload signer variant rather than skipping signing.
            SdkHttpFullRequest signedRequest = AwsSigV4RequestHeaders.hasUnsignedPayloadHeader(request)
                ? unsignedPayloadSigner.sign(unsignedRequest, signerParams)
                : signer.sign(unsignedRequest, signerParams);

            // Transfer every header the AWS SDK signer generated (or modified) on `signedRequest`
            // back onto the original Apache HttpRequest that is about to go on the wire. Net
            // effect: SigV4-required headers like Authorization, x-amz-date,
            // x-amz-content-sha256, and Host (as the signer canonicalized them) end up on the
            // actual HTTP request that Apache HttpClient sends, replacing whatever was there
            // before.
            //
            // For each header in the signed result:
            // 1. removeHeaders(name) strips any pre-existing header of that name from the
            // outbound request, so the original (unsigned) value cannot shadow the signed
            // one. This matters because Apache HttpRequest allows multiple headers with the
            // same name; without removing first you would end up with both the old and the
            // signed value (e.g. two x-amz-date headers).
            // 2. The inner loop then adds back every value the signer produced. A single header
            // name can have multiple values (hence List<String>), so it iterates and re-adds
            // each one.
            for (Map.Entry<String, List<String>> header : signedRequest.headers().entrySet()) {
                request.removeHeaders(header.getKey());
                for (String value : header.getValue()) {
                    request.addHeader(header.getKey(), value);
                }
            }
            logSearchCurl(request, signingUri, body);
        } catch (Exception e) {
            LOG.error("SigV4 signing failed for AOSS request {}", request.getRequestUri(), e);
            throw new RuntimeException("SigV4 signing failed", e);
        }
    }

    private void logSearchCurl(HttpRequest request, URI signingUri, byte[] body) {
        String requestUri = request.getRequestUri();
        if (requestUri == null || requestUri.contains("/_search") == false) {
            return;
        }
        if (requestUri.contains(ADCommonName.CUSTOM_RESULT_INDEX_PREFIX) == false
            && requestUri.contains(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS) == false) {
            return;
        }

        StringBuilder curl = new StringBuilder();
        curl.append("curl -X ").append(shellQuote(request.getMethod())).append(" ").append(shellQuote(signingUri.toString()));
        for (Header header : request.getHeaders()) {
            if ("content-length".equals(header.getName().toLowerCase(Locale.ROOT))) {
                continue;
            }
            curl.append(" \\\n  -H ").append(shellQuote(header.getName() + ": " + header.getValue()));
        }
        if (body.length > 0) {
            curl.append(" \\\n  --data-binary ").append(shellQuote(new String(body, StandardCharsets.UTF_8)));
        }
        LOG
            .warn(
                "Signed AD custom result index search request curl replay command. "
                    + "This command includes short-lived SigV4 credentials and should only be used for debugging:\n{}",
                curl
            );
    }

    private String shellQuote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    private SigningParameters resolveSigningParameters(HttpRequest request, HttpContext context) {
        RequestSigningMaterial requestSigningMaterial = registeredRequestSigningMaterial(request, context);
        if (requestSigningMaterial != null) {
            String region = requireNonBlank(defaultRegion, "default signing region must not be null or blank");
            return new SigningParameters(requestSigningMaterial.credentials(), region, requestSigningMaterial.serviceName());
        }
        String region = requireNonBlank(defaultRegion, "default signing region must not be null or blank");
        return new SigningParameters(serviceCredentialsProvider.resolveCredentials(), region, defaultServiceName);
    }

    private RequestSigningMaterial registeredRequestSigningMaterial(HttpRequest request, HttpContext context) {
        Object contextValue = context == null ? null : context.getAttribute(REQUEST_SIGNING_MATERIAL_CONTEXT_ATTRIBUTE);
        if (contextValue instanceof RequestSigningMaterial) {
            return (RequestSigningMaterial) contextValue;
        }

        Header header = request.getFirstHeader(REQUEST_SIGNING_MATERIAL_ID_HEADER);
        if (header == null || AwsSigV4ThreadContext.hasText(header.getValue()) == false) {
            return null;
        }
        RequestSigningMaterial requestSigningMaterial = REQUEST_SIGNING_MATERIALS.get(header.getValue());
        if (requestSigningMaterial == null) {
            LOG.warn("No registered request-scoped SigV4 signing material found for async REST request; using fallback credentials.");
        } else if (context != null) {
            context.setAttribute(REQUEST_SIGNING_MATERIAL_CONTEXT_ATTRIBUTE, requestSigningMaterial);
        }
        return requestSigningMaterial;
    }

    private boolean shouldSkipSigningHeader(String lowerCaseName) {
        // The material-id header is removed from the real Apache request before this loop runs.
        // Keep it here as a guard so this internal lookup key cannot become part of the AWS
        // canonical signed headers if the signing flow is reordered later.
        return "authorization".equals(lowerCaseName)
            || "content-length".equals(lowerCaseName)
            || "host".equals(lowerCaseName)
            || REQUEST_SIGNING_MATERIAL_ID_HEADER.equals(lowerCaseName);
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
