/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Reads per-request AWS signing material from OpenSearch ThreadContext headers
 * or transients.
 * <p>
 * The credential and service-name keys are fixed canonical names (the static
 * constants below). Request-scoped material does not provide a signing region.
 * Data-plane client factories can resolve it from the target endpoint, falling
 * back to {@code plugins.timeseries.region}. The data-source URL key is configurable
 * via
 * {@link TimeSeriesSettings#DATA_PLANE_ENDPOINT_CONTEXT_KEY} and is supplied at
 * construction time so that the same key is used by both the
 * {@link org.opensearch.timeseries.rest.handler.store.endpoint.ThreadContextEndpointResolver}
 * (for endpoint resolution) and the SigV4 signing path (for sanity-checking
 * that
 * request-scoped signing material is complete).
 */
public final class AwsSigV4ThreadContext {
    private static final Logger LOG = LogManager.getLogger(AwsSigV4ThreadContext.class);

    public static final String AWS_ACCESS_KEY_CONTEXT_KEY = "aws-access-key-id";
    public static final String AWS_SECRET_ACCESS_KEY_CONTEXT_KEY = "aws-secret-access-key";
    public static final String AWS_SESSION_TOKEN_CONTEXT_KEY = "aws-session-token";
    /**
     * @deprecated Signing region is sourced from the data-plane client factory,
     *             not request context.
     */
    @Deprecated
    public static final String AWS_REGION_CONTEXT_KEY = "aws-region";
    public static final String AWS_SERVICE_NAME_CONTEXT_KEY = "aws-service-name";

    private final String dataSourceUrlContextKey;

    public AwsSigV4ThreadContext(String dataSourceUrlContextKey) {
        Objects.requireNonNull(dataSourceUrlContextKey, "dataSourceUrlContextKey must not be null");
        if (dataSourceUrlContextKey.isBlank()) {
            throw new IllegalArgumentException("dataSourceUrlContextKey must not be blank");
        }
        this.dataSourceUrlContextKey = dataSourceUrlContextKey;
    }

    /**
     * Build from cluster {@link Settings} using
     * {@link TimeSeriesSettings#DATA_PLANE_ENDPOINT_CONTEXT_KEY}.
     */
    public static AwsSigV4ThreadContext fromSettings(Settings settings) {
        return new AwsSigV4ThreadContext(TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.get(settings));
    }

    public String dataSourceUrlContextKey() {
        return dataSourceUrlContextKey;
    }

    RequestSigningMaterial resolve(ThreadContext threadContext) {
        return resolve(threadContext, true);
    }

    RequestSigningMaterial resolve(ThreadContext threadContext, boolean logMissingMaterial) {
        if (threadContext == null) {
            if (logMissingMaterial) {
                LOG.debug("No ThreadContext available for request-scoped SigV4 signing material; using service credentials.");
            }
            return null;
        }

        String accessKey = contextValue(threadContext, AWS_ACCESS_KEY_CONTEXT_KEY);
        String secretAccessKey = contextValue(threadContext, AWS_SECRET_ACCESS_KEY_CONTEXT_KEY);
        String sessionToken = contextValue(threadContext, AWS_SESSION_TOKEN_CONTEXT_KEY);
        String dataSourceUrl = contextValue(threadContext, dataSourceUrlContextKey);
        String serviceName = contextValue(threadContext, AWS_SERVICE_NAME_CONTEXT_KEY);

        List<String> presentKeys = new ArrayList<>(5);
        List<String> missingKeys = new ArrayList<>(5);
        recordKeyStatus(presentKeys, missingKeys, AWS_ACCESS_KEY_CONTEXT_KEY, accessKey);
        recordKeyStatus(presentKeys, missingKeys, AWS_SECRET_ACCESS_KEY_CONTEXT_KEY, secretAccessKey);
        recordKeyStatus(presentKeys, missingKeys, AWS_SESSION_TOKEN_CONTEXT_KEY, sessionToken);
        recordKeyStatus(presentKeys, missingKeys, dataSourceUrlContextKey, dataSourceUrl);
        recordKeyStatus(presentKeys, missingKeys, AWS_SERVICE_NAME_CONTEXT_KEY, serviceName);

        if (missingKeys.isEmpty()) {
            if (logMissingMaterial) {
                LOG.info("Found complete request-scoped SigV4 ThreadContext signing material. presentKeys={}", presentKeys);
            }
            return new RequestSigningMaterial(AwsSessionCredentials.create(accessKey, secretAccessKey, sessionToken), serviceName);
        }

        if (logMissingMaterial == false) {
            return null;
        }

        if (presentKeys.isEmpty()) {
            LOG
                .warn(
                    "No request-scoped SigV4 ThreadContext signing material found. missingKeys={}; using service credentials.",
                    missingKeys
                );
        } else {
            LOG
                .warn(
                    "Incomplete request-scoped SigV4 ThreadContext signing material. presentKeys={} missingKeys={}; using service credentials.",
                    presentKeys,
                    missingKeys
                );
        }
        return null;
    }

    boolean hasRequestSigningMaterial(ThreadContext threadContext) {
        return resolve(threadContext, false) != null;
    }

    String serviceName(ThreadContext threadContext) {
        return threadContext == null ? null : contextValue(threadContext, AWS_SERVICE_NAME_CONTEXT_KEY);
    }

    private static String contextValue(ThreadContext threadContext, String key) {
        String headerValue = normalize(threadContext.getHeader(key));
        return hasText(headerValue) ? headerValue : transientValue(threadContext, key);
    }

    private static String transientValue(ThreadContext threadContext, String key) {
        Object value = threadContext.getTransient(key);
        return value == null ? null : normalize(value.toString());
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String stringValue = value.trim();
        return stringValue.isEmpty() ? null : stringValue;
    }

    private static void recordKeyStatus(List<String> presentKeys, List<String> missingKeys, String key, String value) {
        if (hasText(value)) {
            presentKeys.add(key);
        } else {
            missingKeys.add(key);
        }
    }

    static boolean hasText(String value) {
        return value != null && value.isBlank() == false;
    }

    static final class RequestSigningMaterial {
        private final AwsCredentials credentials;
        private final String serviceName;

        private RequestSigningMaterial(AwsCredentials credentials, String serviceName) {
            this.credentials = credentials;
            this.serviceName = serviceName;
        }

        AwsCredentials credentials() {
            return credentials;
        }

        String serviceName() {
            return serviceName;
        }
    }
}
