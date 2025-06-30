/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest;

import java.util.List;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.CronAction;
import org.opensearch.timeseries.transport.CronRequest;
import org.opensearch.timeseries.transport.CronResponse;
import org.opensearch.timeseries.util.InternalApiAccessValidator;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * REST handler for the cron action that performs hourly maintenance tasks.
 * This endpoint is called by the SQS consumer to trigger maintenance on each data node.
 */
public class RestCronAction extends BaseRestHandler {

    public static final String CRON_ACTION = "timeseries_cron_action";
    public static final String CRON_URI = TimeSeriesAnalyticsPlugin.TIMESERIES_BASE_URI + "/_internal/_cron";

    private final Settings settings;

    public RestCronAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String getName() {
        return CRON_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.POST, CRON_URI));
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        InternalApiAccessValidator.validateInternalMultiTenantRequest(
            request,
            settings,
            AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED
        );
        CronRequest cronRequest = new CronRequest();
        return channel -> client.execute(CronAction.INSTANCE, cronRequest, new RestToXContentListener<CronResponse>(channel));
    }
}
