/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.rest;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ADBatchAnomalyResultRequest;
import org.opensearch.ad.transport.ADBatchAnomalyResultResponse;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.InternalApiAccessValidator;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * Internal REST handler for historical batch task execution in multi-tenant mode.
 */
public class RestADBatchTaskRemoteExecutionAction extends BaseRestHandler {

    public static final String AD_BATCH_TASK_REMOTE_EXECUTION_ACTION = "ad_batch_task_remote_execution_action";

    private final Settings settings;

    public RestADBatchTaskRemoteExecutionAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String getName() {
        return AD_BATCH_TASK_REMOTE_EXECUTION_ACTION;
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
                            RestHandlerUtils.AD_TASK_REMOTE
                        )
                )
            );
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        InternalApiAccessValidator.validateInternalMultiTenantRequest(request, settings, AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED);
        RestHandlerUtils
            .promoteHeaderToTransient(
                request,
                client.threadPool().getThreadContext(),
                TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.get(settings)
            );

        String configId = request.param(DETECTOR_ID);
        if (Strings.isEmpty(configId)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Missing required parameter: %s", DETECTOR_ID));
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        ADTask adTask = ADTask.parse(parser);
        if (Strings.isEmpty(adTask.getConfigId())) {
            throw new IllegalArgumentException("AD batch task detector ID is missing");
        }
        if (!configId.equals(adTask.getConfigId())) {
            throw new IllegalArgumentException("Detector ID in path and body must match");
        }

        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings), request);
        TenantAwareHelper.reconcileTenantId(tenantId, adTask.getTenantId());

        return channel -> client
            .execute(
                ADBatchTaskRemoteExecutionAction.INSTANCE,
                new ADBatchAnomalyResultRequest(adTask),
                new RestToXContentListener<ADBatchAnomalyResultResponse>(channel)
            );
    }
}
