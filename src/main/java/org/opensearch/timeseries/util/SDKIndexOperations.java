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
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.rest.handler.store.endpoint.EndpointResolverFactoryLoader;

/**
 * SDK-based implementation of IndexOperations for multi-tenant environments.
 * For result indices, it issues HTTP calls to get the actual health status.
 * For other indices, it returns "green" since they're managed internally.
 */
public class SDKIndexOperations implements IndexOperations {
    private static final Logger LOG = LogManager.getLogger(SDKIndexOperations.class);
    private static final String GREEN_STATUS = "green";

    private final DataSourceEndpointResolver endpointResolver;

    // Result indices that need actual health checks via HTTP
    private final Set<String> resultIndices;

    public SDKIndexOperations() {
        this(Settings.EMPTY);
    }

    public SDKIndexOperations(Settings settings) {
        this.endpointResolver = EndpointResolverFactoryLoader.loadDataSourceEndpointResolver(settings, getClass().getClassLoader());
        // Result indices that require actual HTTP health checks
        this.resultIndices = Set.of(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS);
    }

    @Override
    public String getIndexHealthStatus(String tenantId, String indexOrAliasName) throws IllegalArgumentException {
        // Only check result indices via HTTP; return green for all other indices
        if (!isResultIndex(indexOrAliasName)) {
            return GREEN_STATUS;
        }

        try {
            return getIndexHealthViaHttp(tenantId, indexOrAliasName);
        } catch (Exception e) {
            LOG.error("Failed to get index health for {} via HTTP", indexOrAliasName, e);
            // Return non-existent status on failure as a safe default
            return NONEXISTENT_INDEX_STATUS;
        }
    }

    private boolean isResultIndex(String indexOrAliasName) {
        return resultIndices.contains(indexOrAliasName);
    }

    private String getIndexHealthViaHttp(String tenantId, String indexOrAliasName) throws Exception {
        String endpoint = endpointResolver.resolve(tenantId);
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
