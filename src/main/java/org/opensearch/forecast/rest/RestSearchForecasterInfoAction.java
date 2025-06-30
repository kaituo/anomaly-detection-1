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

import static org.opensearch.timeseries.util.RestHandlerUtils.COUNT;
import static org.opensearch.timeseries.util.RestHandlerUtils.MATCH;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.transport.SearchForecasterInfoAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.SearchConfigInfoRequest;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

public class RestSearchForecasterInfoAction extends BaseRestHandler {

    public static final String SEARCH_FORECASTER_INFO_ACTION = "search_forecaster_info";

    private final Settings settings;

    public RestSearchForecasterInfoAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String getName() {
        return SEARCH_FORECASTER_INFO_ACTION;
    }

    @Override
    @org.opensearch.timeseries.annotation.SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: NodeClient parameter is required by the OpenSearch REST handler contract.")
    protected RestChannelConsumer prepareRequest(RestRequest request, org.opensearch.transport.client.node.NodeClient client)
        throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            String forecasterName = request.param("name", null);
            String rawPath = request.rawPath();

            String tenantId = TenantAwareHelper.getTenantID(ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.get(this.settings), request);
            SearchConfigInfoRequest searchForecasterInfoRequest = new SearchConfigInfoRequest(forecasterName, rawPath, tenantId);
            return channel -> client
                .execute(SearchForecasterInfoAction.INSTANCE, searchForecasterInfoRequest, new RestToXContentListener<>(channel));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }
    }

    @Override
    public List<RestHandler.Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, COUNT)
                ),
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, MATCH)
                )
            );
    }
}
