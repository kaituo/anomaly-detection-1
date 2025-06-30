/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.transport.ADHCImputeAction;
import org.opensearch.ad.transport.ADHCImputeNodesResponse;
import org.opensearch.ad.transport.ADHCImputeRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SDKNodeFilter;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * REST handler for HC impute action that broadcasts impute requests to data nodes.
 * This endpoint is called by the HTTP node communicator for multi-tenant mode.
 */
public class RestADHCImputeAction extends BaseRestHandler {

    public static final String HC_IMPUTE_ACTION = "ad_hc_impute_action";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String DATA_START_MILLIS_FIELD = "data_start_millis";
    public static final String DATA_END_MILLIS_FIELD = "data_end_millis";

    private final SDKNodeFilter nodeFilter;

    public RestADHCImputeAction() {
        this.nodeFilter = new SDKNodeFilter();
    }

    @Override
    public String getName() {
        return HC_IMPUTE_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            new Route(
                RestRequest.Method.POST,
                String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, RestHandlerUtils.HC_IMPUTE)
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String configId = request.param(DETECTOR_ID);
        if (Strings.isEmpty(configId)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Missing required parameter: %s", DETECTOR_ID));
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        String tenantId = request.header(CommonName.TENANT_ID_HEADER);
        String taskId = null;
        long dataStartMillis = 0;
        long dataEndMillis = 0;

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case CommonName.TENANT_ID_FIELD:
                    tenantId = parser.text();
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.textOrNull();
                    break;
                case DATA_START_MILLIS_FIELD:
                    dataStartMillis = parser.longValue();
                    break;
                case DATA_END_MILLIS_FIELD:
                    dataEndMillis = parser.longValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        if (dataStartMillis <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid %s: %d", DATA_START_MILLIS_FIELD, dataStartMillis));
        }
        if (dataEndMillis <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid %s: %d", DATA_END_MILLIS_FIELD, dataEndMillis));
        }

        // Get eligible data nodes for the request
        DiscoveryNode[] nodes = nodeFilter.getEligibleDataNodes();

        ADHCImputeRequest imputeRequest = new ADHCImputeRequest(configId, tenantId, taskId, dataStartMillis, dataEndMillis, nodes);

        return channel -> client.execute(ADHCImputeAction.INSTANCE, imputeRequest, new RestToXContentListener<ADHCImputeNodesResponse>(channel));
    }
}
