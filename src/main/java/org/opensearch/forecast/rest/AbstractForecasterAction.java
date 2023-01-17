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

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_INTERVAL;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_WINDOW_DELAY;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_FORECAST_FEATURES;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_HC_FORECASTERS;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_SINGLE_STREAM_FORECASTERS;
import static org.opensearch.forecast.settings.ForecastSettings.REQUEST_TIMEOUT;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.rest.BaseRestHandler;

public abstract class AbstractForecasterAction extends BaseRestHandler {
    protected volatile TimeValue requestTimeout;
    protected volatile TimeValue forecastInterval;
    protected volatile TimeValue forecastWindowDelay;
    protected volatile Integer maxSingleStreamForecasters;
    protected volatile Integer maxHCForecasters;
    protected volatile Integer maxForecastFeatures;
    protected volatile Integer maxCategoricalFields;

    public AbstractForecasterAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        this.forecastInterval = FORECAST_INTERVAL.get(settings);
        this.forecastWindowDelay = FORECAST_WINDOW_DELAY.get(settings);
        this.maxSingleStreamForecasters = MAX_SINGLE_STREAM_FORECASTERS.get(settings);
        this.maxHCForecasters = MAX_HC_FORECASTERS.get(settings);
        this.maxForecastFeatures = MAX_FORECAST_FEATURES;
        this.maxCategoricalFields = ForecastNumericSetting.maxCategoricalFields();
        // TODO: will add more cluster setting consumer later
        // TODO: inject ClusterSettings only if clusterService is only used to get ClusterSettings
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_INTERVAL, it -> forecastInterval = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_WINDOW_DELAY, it -> forecastWindowDelay = it);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_SINGLE_STREAM_FORECASTERS, it -> maxSingleStreamForecasters = it);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_HC_FORECASTERS, it -> maxHCForecasters = it);
    }
}
