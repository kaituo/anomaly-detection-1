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

package org.opensearch.forecast.rest;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.FORECASTER_ID;
import static org.opensearch.timeseries.util.RestHandlerUtils.RUN;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.ForecastResultAction;
import org.opensearch.forecast.transport.ForecastResultRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to forecast.
 */
public class RestExecuteForecasterAction extends BaseRestHandler {

    public static final String FORECASTER_ACTION = "execute_forecaster";

    public RestExecuteForecasterAction() {}

    @Override
    public String getName() {
        return FORECASTER_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }
        ForecasterExecutionInput input = getForecasterExecutionInput(request);
        return channel -> {
            String error = validateAdExecutionInput(input);
            if (StringUtils.isNotBlank(error)) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, error));
                return;
            }

            ForecastResultRequest getRequest = new ForecastResultRequest(
                input.getForecasterId(),
                input.getPeriodStart().toEpochMilli(),
                input.getPeriodEnd().toEpochMilli(),
                false
            );
            client.execute(ForecastResultAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
        };
    }

    private ForecasterExecutionInput getForecasterExecutionInput(RestRequest request) throws IOException {
        String forecasterId = null;
        if (request.hasParam(FORECASTER_ID)) {
            forecasterId = request.param(FORECASTER_ID);
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        ForecasterExecutionInput input = ForecasterExecutionInput.parse(parser, forecasterId);
        if (forecasterId != null) {
            input.setForecasterId(forecasterId);
        }
        return input;
    }

    private String validateAdExecutionInput(ForecasterExecutionInput input) {
        if (StringUtils.isBlank(input.getForecasterId())) {
            return "Must set forecaster id or forecaster";
        }
        if (input.getPeriodStart() == null || input.getPeriodEnd() == null) {
            return "Must set both period start and end date with epoch of milliseconds";
        }
        if (!input.getPeriodStart().isBefore(input.getPeriodEnd())) {
            return "Period start date should be before end date";
        }
        return null;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // execute forester once
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, FORECASTER_ID, RUN)
                )
            );
    }
}
