/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.rest;

import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ForwardADTaskAction;
import org.opensearch.ad.transport.ForwardADTaskRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.InternalApiAccessValidator;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * Internal REST handler for historical task forwarding in multi-tenant mode.
 */
public class RestADForwardTaskAction extends BaseRestHandler {

    public static final String FORWARD_AD_TASK_ACTION = "ad_forward_task_action";

    private final Settings settings;

    public RestADForwardTaskAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String getName() {
        return FORWARD_AD_TASK_ACTION;
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
                            RestHandlerUtils.FORWARD_TASK
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

        ForwardADTaskRequest forwardRequest = ForwardADTaskRequest.parseWithDetectorId(request.contentParser(), configId);
        if (forwardRequest.getDetector() == null || Strings.isEmpty(forwardRequest.getDetector().getId())) {
            throw new IllegalArgumentException("Forward AD task request detector is missing");
        }
        if (!configId.equals(forwardRequest.getDetector().getId())) {
            throw new IllegalArgumentException("Detector ID in path and body must match");
        }

        String tenantId = TenantAwareHelper.getTenantID(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings), request);
        TenantAwareHelper.reconcileTenantId(tenantId, getBodyTenantId(forwardRequest));

        return channel -> client.execute(ForwardADTaskAction.INSTANCE, forwardRequest, new RestToXContentListener<JobResponse>(channel));
    }

    private String getBodyTenantId(ForwardADTaskRequest request) {
        if (request.getDetector() != null && request.getDetector().getTenantId() != null) {
            return request.getDetector().getTenantId();
        }
        return request.getAdTask() == null ? null : request.getAdTask().getTenantId();
    }
}
