/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.EntityADResultAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.transport.EntityResultRequest;
import org.opensearch.timeseries.util.InternalApiAccessValidator;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * REST handler for entity result transport action.
 * This endpoint is called by the HTTP node communicator for multi-tenant mode.
 */
public class RestEntityADResultAction extends BaseRestHandler {

    public static final String ENTITY_RESULT_ACTION = "ad_entity_result_action";
    private final Settings settings;

    public RestEntityADResultAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String getName() {
        return ENTITY_RESULT_ACTION;
    }

    @Override
    public java.util.List<Route> routes() {
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
                            RestHandlerUtils.ENTITY_RESULT
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

        long start = 0;
        long end = 0;
        String taskId = null;
        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings), request);
        Map<Entity, double[]> entities = new HashMap<>();

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case CommonName.START_JSON_KEY:
                    start = parser.longValue();
                    break;
                case CommonName.END_JSON_KEY:
                    end = parser.longValue();
                    break;
                case CommonName.TASK_ID_FIELD:
                    taskId = parser.textOrNull();
                    break;
                case CommonName.TENANT_ID_FIELD:
                    tenantId = TenantAwareHelper.reconcileTenantId(tenantId, parser.text());
                    break;
                case CommonName.ENTITIES_JSON_KEY:
                    parseEntities(parser, entities);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        EntityResultRequest entityResultRequest = new EntityResultRequest(
            configId,
            entities,
            start,
            end,
            AnalysisType.AD,
            taskId,
            tenantId
        );
        return channel -> client
            .execute(EntityADResultAction.INSTANCE, entityResultRequest, new RestToXContentListener<AcknowledgedResponse>(channel));
    }

    private void parseEntities(XContentParser parser, Map<Entity, double[]> entities) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            Entity entity = null;
            double[] values = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                switch (fieldName) {
                    case CommonName.ENTITY_KEY:
                        entity = Entity.parse(parser);
                        break;
                    case CommonName.VALUE_JSON_KEY:
                        values = parseDoubleArray(parser);
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
            if (entity != null && values != null) {
                entities.put(entity, values);
            }
        }
    }

    private double[] parseDoubleArray(XContentParser parser) throws IOException {
        java.util.List<Object> values = parser.list();
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
}
