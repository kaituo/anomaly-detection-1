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

package org.opensearch.forecast.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.transport.BaseSearchConfigInfoTransportAction;
import org.opensearch.transport.TransportService;

public class SearchForecasterInfoTransportAction extends BaseSearchConfigInfoTransportAction {

    @Inject
    public SearchForecasterInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        DataAccess dataAccess,
        Settings settings,
        RunContext runContext
    ) {
        super(
            transportService,
            actionFilters,
            dataAccess,
            SearchForecasterInfoAction.NAME,
            ForecastCommonName.CONFIG_INDEX,
            settings,
            runContext
        );
    }

    @Override
    protected Setting<Boolean> getMultiTenancyEnabledSetting() {
        return ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED;
    }
}
