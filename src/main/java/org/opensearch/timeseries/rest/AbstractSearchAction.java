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

package org.opensearch.timeseries.rest;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.timeseries.util.RestHandlerUtils.getSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.client.RestDataPlaneResponse;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;
import org.owasp.encoder.Encode;

/**
 * Abstract class to handle search request.
 *
 * @param <T> the response payload type
 */
public abstract class AbstractSearchAction<T extends ToXContentObject> extends BaseRestHandler {

    protected final String index;
    protected final Class<T> clazz;
    protected final List<String> urlPaths;
    protected final List<Pair<String, String>> deprecatedPaths;
    protected final ActionType<SearchResponse> actionType;
    protected final Supplier<Boolean> enabledSupplier;
    protected final String disabledMsg;
    protected final Supplier<Boolean> isMultiTenancyEnabledSupplier;

    private final Logger logger = LogManager.getLogger(AbstractSearchAction.class);

    public AbstractSearchAction(
        List<String> urlPaths,
        List<Pair<String, String>> deprecatedPaths,
        String index,
        Class<T> clazz,
        ActionType<SearchResponse> actionType,
        Supplier<Boolean> adEnabledSupplier,
        String disabledMsg,
        Supplier<Boolean> isMultiTenancyEnabledSupplier
    ) {
        this.index = index;
        this.clazz = clazz;
        this.urlPaths = urlPaths;
        this.deprecatedPaths = deprecatedPaths;
        this.actionType = actionType;
        this.enabledSupplier = adEnabledSupplier;
        this.disabledMsg = disabledMsg;
        this.isMultiTenancyEnabledSupplier = isMultiTenancyEnabledSupplier;
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!enabledSupplier.get()) {
            throw new IllegalStateException(disabledMsg);
        }
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
            // order of response will be re-arranged everytime we use `_source`, we sometimes do this
            // even if user doesn't give this field as we exclude ui_metadata if request isn't from OSD
            // ref-link: https://github.com/elastic/elasticsearch/issues/17639
            searchSourceBuilder.fetchSource(getSourceContext(request, searchSourceBuilder));
            searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
            String tenantId = TenantAwareHelper.getTenantID(isMultiTenancyEnabledSupplier.get(), request);
            SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(this.index).preference(tenantId);
            return channel -> client.execute(actionType, searchRequest, search(channel));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }
    }

    protected void onFailure(RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (Exception exception) {
            logger.error("Failed to send back failure response for search AD result", exception);
        }
    }

    protected RestResponseListener<SearchResponse> search(RestChannel channel) {
        return new RestResponseListener<SearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse response) throws Exception {
                if (response.isTimedOut()) {
                    return new BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString());
                }
                return new BytesRestResponse(RestStatus.OK, toClientSearchResponse(response, channel.newBuilder()));
            }
        };
    }

    static XContentBuilder toClientSearchResponse(SearchResponse response, XContentBuilder builder) throws IOException {
        if (response instanceof RestDataPlaneResponse == false) {
            return response.toXContent(builder, EMPTY_PARAMS);
        }

        /*
         * Single-tenant search keeps native InternalAggregation objects:
         * Dashboards -> AD REST action -> transport SearchRequest -> OpenSearch search engine
         *            <- native SearchResponse with InternalAggregation
         * AD REST action serializes it directly to JSON.
         *
         * Multi-tenant/AOSS search goes through SdkDataAccess:
         * Dashboards -> AD REST action -> AD transport action/SearchHandler -> SdkDataAccess
         * -> low-level REST request to AOSS with typed_keys=true
         * <- AOSS REST JSON response with names like "sterms#detectors"
         * -> SearchResponse.fromXContent(parser), which creates ParsedAggregation objects
         * -> AD REST action serializes SearchResponse back to JSON for Dashboards.
         *
         * ParsedAggregation always serializes names as type#name. Normalize only marked
         * REST data-plane responses so single-tenant output remains exactly on the native path.
         */
        XContentBuilder typedBuilder = response.toXContent(JsonXContent.contentBuilder(), EMPTY_PARAMS);
        Map<String, Object> responseMap = XContentHelper
            .convertToMap(BytesReference.bytes(typedBuilder), false, MediaTypeRegistry.JSON)
            .v2();
        normalizeAggregationKeys(responseMap);
        builder.map(responseMap);
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static void normalizeAggregationKeys(Map<String, Object> map) {
        Object aggregations = map.get("aggregations");
        if (aggregations instanceof Map) {
            map.put("aggregations", normalizeTypedAggregationMap((Map<String, Object>) aggregations));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeTypedAggregationMap(Map<String, Object> aggregations) {
        Map<String, Object> normalized = new LinkedHashMap<>(aggregations.size());
        for (Map.Entry<String, Object> entry : aggregations.entrySet()) {
            String name = stripTypedKeyPrefix(entry.getKey());
            normalized.put(name, normalizeAggregationValue(entry.getValue()));
        }
        return normalized;
    }

    @SuppressWarnings("unchecked")
    private static Object normalizeAggregationValue(Object value) {
        if (value instanceof Map) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                String key = stripTypedKeyPrefix(entry.getKey());
                normalized.put(key, "_source".equals(key) ? entry.getValue() : normalizeAggregationValue(entry.getValue()));
            }
            return normalized;
        } else if (value instanceof List) {
            List<Object> normalized = new ArrayList<>();
            for (Object item : (List<Object>) value) {
                normalized.add(normalizeAggregationValue(item));
            }
            return normalized;
        }
        return value;
    }

    private static String stripTypedKeyPrefix(String key) {
        int delimiterIndex = key.indexOf('#');
        return delimiterIndex < 0 ? key : key.substring(delimiterIndex + 1);
    }

    @Override
    public List<Route> routes() {
        List<Route> routes = new ArrayList<>();
        for (String path : urlPaths) {
            routes.add(new Route(RestRequest.Method.POST, path));
            routes.add(new Route(RestRequest.Method.GET, path));
        }
        return routes;
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        List<ReplacedRoute> replacedRoutes = new ArrayList<>();
        for (Pair<String, String> deprecatedPath : deprecatedPaths) {
            replacedRoutes
                .add(
                    new ReplacedRoute(RestRequest.Method.POST, deprecatedPath.getKey(), RestRequest.Method.POST, deprecatedPath.getValue())
                );
            replacedRoutes
                .add(new ReplacedRoute(RestRequest.Method.GET, deprecatedPath.getKey(), RestRequest.Method.GET, deprecatedPath.getValue()));

        }
        return replacedRoutes;
    }
}
