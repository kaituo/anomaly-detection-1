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

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ADSingleStreamResultAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.transport.SingleStreamResultRequest;
import org.opensearch.timeseries.util.InternalApiAccessValidator;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * REST handler for single stream result transport action.
 * This endpoint is called by the HTTP node communicator for multi-tenant mode.
 */
public class RestADSingleStreamResultAction extends BaseRestHandler {

    public static final String SINGLE_STREAM_RESULT_ACTION = "ad_single_stream_result_action";
    private final Settings settings;

    public RestADSingleStreamResultAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String getName() {
        return SINGLE_STREAM_RESULT_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            TimeSeriesAnalyticsPlugin.AD_BASE_INTERNAL_DETECTORS_URI,
                            DETECTOR_ID,
                            RestHandlerUtils.SINGLE_STREAM_RESULT
                        )
                )
            );
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        InternalApiAccessValidator.validateInternalMultiTenantRequest(request, settings, AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED);

        String configId = request.param(DETECTOR_ID);
        if (Strings.isEmpty(configId)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Missing required parameter: %s", DETECTOR_ID));
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        long startMillis = 0;
        long endMillis = 0;
        String modelId = null;
        double[] datapoint = null;
        String taskId = null;
        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings), request);
        String configJson = null;

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case CommonName.MODEL_ID_KEY:
                    modelId = parser.text();
                    break;
                case CommonName.START_JSON_KEY:
                    startMillis = parser.longValue();
                    break;
                case CommonName.END_JSON_KEY:
                    endMillis = parser.longValue();
                    break;
                case CommonName.VALUE_LIST_FIELD:
                    datapoint = parseDoubleArray(parser);
                    break;
                case CommonName.RUN_ONCE_FIELD:
                    taskId = parser.textOrNull();
                    break;
                case CommonName.TENANT_ID_FIELD:
                    tenantId = TenantAwareHelper.reconcileTenantId(tenantId, parser.text());
                    break;
                case CommonName.CONFIG_JSON_FIELD:
                    configJson = parseConfigJson(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        SingleStreamResultRequest singleStreamRequest = new SingleStreamResultRequest(
            configId,
            modelId,
            startMillis,
            endMillis,
            datapoint,
            taskId,
            tenantId,
            configJson
        );

        return channel -> client
            .execute(ADSingleStreamResultAction.INSTANCE, singleStreamRequest, new RestToXContentListener<AcknowledgedResponse>(channel));
    }

    private double[] parseDoubleArray(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<Object> values = parser.list();
        double[] result = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value instanceof Number) {
                result[i] = ((Number) value).doubleValue();
            } else if (value != null) {
                result[i] = Double.parseDouble(value.toString());
            }
        }
        return result;
    }

    private String parseConfigJson(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        }
        try (org.opensearch.core.xcontent.XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            return builder.toString();
        }
    }
}
