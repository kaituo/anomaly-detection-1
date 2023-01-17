/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import static org.opensearch.timeseries.util.RestHandlerUtils.FORECASTER_ID;
import static org.opensearch.timeseries.util.RestHandlerUtils.START_JOB;
import static org.opensearch.timeseries.util.RestHandlerUtils.STOP_JOB;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.client.node.NodeClient;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.ForecasterJobAction;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.rest.RestJobAction;
import org.opensearch.timeseries.transport.JobRequest;

import com.google.common.collect.ImmutableList;

public class RestForecasterJobAction extends RestJobAction {
    public static final String FORECAST_JOB_ACTION = "forecaster_job_action";

    @Override
    public String getName() {
        return FORECAST_JOB_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        String forecasterId = request.param(FORECASTER_ID);
        boolean historical = request.paramAsBoolean("historical", false);
        String rawPath = request.rawPath();
        DateRange dateRange = parseInputDateRange(request);

        JobRequest forecasterJobRequest = new JobRequest(forecasterId, dateRange, historical, rawPath);

        return channel -> client.execute(ForecasterJobAction.INSTANCE, forecasterJobRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                /// start forecaster Job
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, FORECASTER_ID, START_JOB)
                ),
                /// stop forecaster Job
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, FORECASTER_ID, STOP_JOB)
                )
            );
    }
}
