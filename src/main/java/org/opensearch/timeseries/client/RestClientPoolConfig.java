/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.config.TlsConfig;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.util.Timeout;

final class RestClientPoolConfig {
    static final Timeout REQUEST_TIMEOUT = Timeout.ofMilliseconds(60_000);

    /*
     * A route is Apache HttpClient's connection bucket for a target network path. For our direct
     * data-plane requests, that is the endpoint's scheme/host/port, e.g. one AOSS collection
     * endpoint. Data-plane RestClient instances are built with one HttpHost in the current codebase
     * (RestClientProvider and SigningRestClientProvider), so one cached client has one route. That
     * makes MAX_CONN_PER_ROUTE the effective cap for one endpoint/traffic class/JVM.
     *
     * MAX_CONN_TOTAL is Apache HttpClient's aggregate cap across all routes in the same client. It
     * would matter if a future client were built with multiple hosts, but today no data-plane client
     * using this pool config has more than one route.
     *
     * Keep the per-route cap higher than the OpenSearch RestClient default (10) so detector bursts do
     * not serialize behind a tiny local pool, but still bounded: raising it opens more sockets, uses
     * more TLS/session memory, consumes more file descriptors and ephemeral ports, and can push more
     * simultaneous load to the same remote host. The remote host has finite connection/request
     * capacity regardless of how many clients we create, so increase this only with evidence of
     * pool-acquire timeouts and healthy downstream latency.
     */
    private static final int MAX_CONN_PER_ROUTE = 50;
    private static final int MAX_CONN_TOTAL = 100;
    private static final Timeout CONNECTION_REQUEST_TIMEOUT = Timeout.ofSeconds(5);

    private RestClientPoolConfig() {}

    static RequestConfig.Builder configureRequestTimeouts(RequestConfig.Builder requestConfig, Timeout timeout) {
        return requestConfig
            .setConnectTimeout(timeout)
            .setResponseTimeout(timeout)
            .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT);
    }

    static HttpAsyncClientBuilder configureConnectionPool(HttpAsyncClientBuilder builder) {
        // Force HTTP/1.1 for the data-plane (AOSS) client.
        //
        // The AOSS v2 checksum failure is transport-level, not signing-level. Once the interceptor
        // signs the actual JSON hash, later signing changes can only move the error around; they do
        // not change how the async HTTP client writes the body to the wire.
        //
        // The observed discriminator was the HTTP client/protocol:
        // - awscurl -> AOSS v2 succeeds: Python requests uses HTTP/1.1 and sends the body verbatim
        //   with a fixed Content-Length.
        // - AD -> AOSS v1 succeeds: v1 did not strictly verify the content checksum, so the wire
        //   framing difference was invisible there.
        // - AD -> AOSS v2 fails: the response identified Apache-HttpAsyncClient/5.4.4 over HTTP/2,
        //   and the DEBUG-SIGN headers captured at interceptor time had no Content-Length. After
        //   signing, the async HTTP/2 producer re-framed the POST body into DATA frames. AOSS v2's
        //   strict checksum layer hashes what actually arrives, which no longer matches the bytes
        //   hashed and signed by the interceptor.
        //
        // HTTP/1.1 avoids that mismatch because it sends the request body with a stable
        // Content-Length, matching the awscurl path. Forcing HTTP/1.1 here is therefore the actual
        // fix: it keeps the signed payload hash and the bytes AOSS v2 receives aligned.
        //
        // IMPORTANT: the version policy MUST be set on the connection manager. When a custom
        // PoolingAsyncClientConnectionManager is supplied, HttpAsyncClientBuilder#setVersionPolicy
        // is ignored and the protocol is negotiated by the connection manager (via ALPN), so a
        // FORCE_HTTP_1 on the builder alone has no effect and HTTP/2 is still used.
        return builder
            .setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1)
            .setConnectionManager(
                PoolingAsyncClientConnectionManagerBuilder
                    .create()
                    .setMaxConnPerRoute(MAX_CONN_PER_ROUTE)
                    .setMaxConnTotal(MAX_CONN_TOTAL)
                    .setTlsStrategy(createTlsStrategy())
                    // The connection manager owns protocol negotiation; force HTTP/1.1 here via
                    // TlsConfig so ALPN does not upgrade to HTTP/2.
                    .setDefaultTlsConfig(
                        TlsConfig.custom().setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1).build()
                    )
                    .build()
            );
    }

    /**
     * Builds the TLS strategy for the custom async connection pool installed by
     * {@link #configureConnectionPool(HttpAsyncClientBuilder)}.
     *
     * <p>Once we supply our own {@link PoolingAsyncClientConnectionManagerBuilder} (to raise the
     * pool limits past the RestClient defaults), the RestClient no longer configures TLS for us, so
     * the connection manager needs an explicit {@link TlsStrategy} or HTTPS calls to the data-plane
     * (AOSS) endpoint would fail. {@link SSLContext#getDefault()} pulls in the JVM's default
     * trust/key material.
     *
     * <p>The explicit {@code setTlsDetailsFactory} is a required Apache HttpClient 5 /
     * httpcore5-reactor workaround: with the async client the default TLS-upgrade path fails to
     * populate {@link TlsDetails}, throwing at connection time (typically an NPE / "TLS upgrade"
     * failure while reading the SSL session or ALPN protocol). Supplying a factory that builds
     * {@code TlsDetails} from {@code sslEngine.getSession()} + {@code getApplicationProtocol()}
     * fixes it. The same snippet is used in the test harness ({@code ODFERestTestCase}).
     *
     * <p>The {@link NoSuchAlgorithmException} is rethrown as an unchecked {@link IllegalStateException}
     * to fail fast, since a JVM without a default SSL context is unrecoverable here.
     */
    private static TlsStrategy createTlsStrategy() {
        try {
            return ClientTlsStrategyBuilder
                .create()
                .setSslContext(SSLContext.getDefault())
                .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                    @Override
                    public TlsDetails create(SSLEngine sslEngine) {
                        return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                    }
                })
                .build();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("could not create the default ssl context", e);
        }
    }
}
