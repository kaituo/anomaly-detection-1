/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.timeseries.util;

import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.rest.handler.store.spi.DefaultTenantEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.spi.TenantEndpointResolver;

/**
 * SDK-based implementation of IndexOperations for multi-tenant environments.
 * For result indices, it issues HTTP calls to get the actual health status.
 * For other indices, it returns "green" since they're managed internally.
 * For index name resolution, it builds S3 key prefixes for multi-tenant storage.
 */
public class SDKIndexOperations implements IndexOperations {
    private static final Logger LOG = LogManager.getLogger(SDKIndexOperations.class);
    private static final String GREEN_STATUS = "green";

    /**
     * Constants for entity model key generation
     */
    public static final String ENTITY_MODEL_ID_INFIX = "_entity_";
    public static final int ENTITY_KEY_SEGMENT_LENGTH = 2;
    public static final int ENTITY_KEY_SEGMENT_DEPTH = 3;

    private final TenantEndpointResolver endpointResolver;

    // Result indices that need actual health checks via HTTP
    private final Set<String> resultIndices;

    public SDKIndexOperations() {
        this(Settings.EMPTY);
    }

    @SuppressWarnings("unused")
    public SDKIndexOperations(Settings settings) {
        this.endpointResolver = ServiceLoader.load(TenantEndpointResolver.class).findFirst().orElseGet(DefaultTenantEndpointResolver::new);
        // Result indices that require actual HTTP health checks
        this.resultIndices = Set.of(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS);
    }

    @Override
    public String getIndexHealthStatus(String indexOrAliasName) throws IllegalArgumentException {
        // Only check result indices via HTTP; return green for all other indices
        if (!isResultIndex(indexOrAliasName)) {
            return GREEN_STATUS;
        }

        try {
            return getIndexHealthViaHttp(indexOrAliasName);
        } catch (Exception e) {
            LOG.error("Failed to get index health for {} via HTTP", indexOrAliasName, e);
            // Return non-existent status on failure as a safe default
            return NONEXISTENT_INDEX_STATUS;
        }
    }

    @Override
    public String resolveIndexName(String tenantId, String configId, String modelId, String defaultIndexName) {
        // multi-tenant: repurpose checkpoint index name as s3 key prefix
        if (Strings.isEmpty(tenantId) || Strings.isEmpty(configId)) {
            return defaultIndexName;
        }

        String basePrefix = StringUtil.sanitizeId(tenantId) + "/" + configId;
        String entityId = extractEntityIdentifier(modelId);

        if (Strings.isEmpty(entityId)) {
            // Single-stream detector: no entity suffix, so use tenant/config prefix as the full key.
            return basePrefix;
        }

        return buildEntityModelKey(basePrefix, entityId);
    }

    /**
     * Append short chunks of the entity identifier to the base prefix to finish building the S3 object key.
     * Example (detector id {@code ZoNYVJsq5ry6e-SWXmAt1Q}, entity id {@code _cLQbZUBxkwQb14jsXV9}):
     * {@code .../ZoNYVJsq5ry6e-SWXmAt1Q/_c/LQ/bZ/UBxkwQb14jsXV9}.
     * Splitting the entity id into fixed-length segments creates multiple sub-prefixes so that S3 can route
     * each prefix independently, avoiding hot-spotting within a single detector prefix.
     *
     * @param basePrefix hierarchy that already includes tenant, config, and detector prefixes
     * @param entityId full entity identifier, usually a base64 hash
     * @return S3 object key prefix with entity sub-prefixes appended
     */
    public static String buildEntityModelKey(String basePrefix, String entityId) {
        StringBuilder keyBuilder = new StringBuilder(basePrefix);

        int index = 0;
        int segmentCount = 0;
        while (segmentCount < ENTITY_KEY_SEGMENT_DEPTH && (index + ENTITY_KEY_SEGMENT_LENGTH) < entityId.length()) {
            // Break the entity id into short prefixes to distribute objects across partitions.
            keyBuilder.append("/").append(entityId, index, index + ENTITY_KEY_SEGMENT_LENGTH);
            index += ENTITY_KEY_SEGMENT_LENGTH;
            segmentCount++;
        }

        if (index < entityId.length()) {
            keyBuilder.append("/").append(entityId.substring(index));
        }

        return keyBuilder.toString();
    }

    /**
     * Extract entity identifier from model ID.
     * @param modelId the model ID containing entity information
     * @return entity identifier or empty string if not found
     */
    public static String extractEntityIdentifier(String modelId) {
        if (Strings.isEmpty(modelId)) {
            return "";
        }
        int entityInfixIndex = modelId.indexOf(ENTITY_MODEL_ID_INFIX);
        if (entityInfixIndex < 0) {
            return "";
        }
        int entityStartIndex = entityInfixIndex + ENTITY_MODEL_ID_INFIX.length();
        return entityStartIndex < modelId.length() ? modelId.substring(entityStartIndex) : "";
    }

    private boolean isResultIndex(String indexOrAliasName) {
        return resultIndices.contains(indexOrAliasName);
    }

    private String getIndexHealthViaHttp(String indexOrAliasName) throws Exception {
        String endpoint = endpointResolver.resolve(null);
        RestClient restClient = RestClientProvider.getRestClient(endpoint);

        // Use cluster health API to get index health
        String healthEndpoint = "/_cluster/health/" + indexOrAliasName;
        Request request = new Request("GET", healthEndpoint);

        Response response = restClient.performRequest(request);
        String responseBody = org.apache.hc.core5.http.io.entity.EntityUtils.toString(response.getEntity());
        Map<String, Object> healthResponse = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), responseBody, false);

        // Extract status from response
        Object statusObj = healthResponse.get("status");
        if (statusObj != null) {
            return statusObj.toString().toLowerCase(Locale.ROOT);
        }

        // If no status found, return non-existent
        return NONEXISTENT_INDEX_STATUS;
    }
}
