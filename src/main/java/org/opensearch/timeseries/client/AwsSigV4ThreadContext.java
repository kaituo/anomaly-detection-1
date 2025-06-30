/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Reads per-request AWS signing material from OpenSearch ThreadContext transients.
 * <p>
 * The credential, region, and service-name keys are fixed canonical names (the static
 * constants below). The data-source URL key is configurable via
 * {@link TimeSeriesSettings#DATA_PLANE_ENDPOINT_CONTEXT_KEY} and is supplied at
 * construction time so that the same key is used by both the
 * {@link org.opensearch.timeseries.rest.handler.store.endpoint.ThreadContextEndpointResolver}
 * (for endpoint resolution) and the SigV4 signing path (for sanity-checking that
 * request-scoped signing material is complete).
 */
public final class AwsSigV4ThreadContext {
    public static final String AWS_ACCESS_KEY_CONTEXT_KEY = "aws-access-key-id";
    public static final String AWS_SECRET_ACCESS_KEY_CONTEXT_KEY = "aws-secret-access-key";
    public static final String AWS_SESSION_TOKEN_CONTEXT_KEY = "aws-session-token";
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
     * Build from cluster {@link Settings} using {@link TimeSeriesSettings#DATA_PLANE_ENDPOINT_CONTEXT_KEY}.
     */
    public static AwsSigV4ThreadContext fromSettings(Settings settings) {
        return new AwsSigV4ThreadContext(TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.get(settings));
    }

    public String dataSourceUrlContextKey() {
        return dataSourceUrlContextKey;
    }

    RequestSigningMaterial resolve(ThreadContext threadContext) {
        if (threadContext == null) {
            return null;
        }

        String accessKey = transientValue(threadContext, AWS_ACCESS_KEY_CONTEXT_KEY);
        String secretAccessKey = transientValue(threadContext, AWS_SECRET_ACCESS_KEY_CONTEXT_KEY);
        String sessionToken = transientValue(threadContext, AWS_SESSION_TOKEN_CONTEXT_KEY);
        String dataSourceUrl = transientValue(threadContext, dataSourceUrlContextKey);
        String region = transientValue(threadContext, AWS_REGION_CONTEXT_KEY);
        String serviceName = transientValue(threadContext, AWS_SERVICE_NAME_CONTEXT_KEY);

        if (hasText(accessKey)
            && hasText(secretAccessKey)
            && hasText(sessionToken)
            && hasText(dataSourceUrl)
            && hasText(region)
            && hasText(serviceName)) {
            return new RequestSigningMaterial(AwsSessionCredentials.create(accessKey, secretAccessKey, sessionToken), region, serviceName);
        }
        return null;
    }

    boolean hasRequestSigningMaterial(ThreadContext threadContext) {
        return resolve(threadContext) != null;
    }

    String region(ThreadContext threadContext) {
        return threadContext == null ? null : transientValue(threadContext, AWS_REGION_CONTEXT_KEY);
    }

    String serviceName(ThreadContext threadContext) {
        return threadContext == null ? null : transientValue(threadContext, AWS_SERVICE_NAME_CONTEXT_KEY);
    }

    private static String transientValue(ThreadContext threadContext, String key) {
        Object value = threadContext.getTransient(key);
        if (value == null) {
            return null;
        }
        String stringValue = value.toString().trim();
        return stringValue.isEmpty() ? null : stringValue;
    }

    static boolean hasText(String value) {
        return value != null && value.isBlank() == false;
    }

    static final class RequestSigningMaterial {
        private final AwsCredentials credentials;
        private final String region;
        private final String serviceName;

        private RequestSigningMaterial(AwsCredentials credentials, String region, String serviceName) {
            this.credentials = credentials;
            this.region = region;
            this.serviceName = serviceName;
        }

        AwsCredentials credentials() {
            return credentials;
        }

        String region() {
            return region;
        }

        String serviceName() {
            return serviceName;
        }
    }
}
