/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest.handler.store;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.timeseries.rest.handler.store.DataManagement;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;

/**
 * Delegating config store specialized for forecaster resources.
 */
public class ForecastDelegatingDataManagement extends DelegatingDataManagement<ForecastIndex> {

    public ForecastDelegatingDataManagement(
        DataManagement<ForecastIndex> indexStore,
        DataManagement<ForecastIndex> sdkStore,
        ClusterService clusterService
    ) {
        super(indexStore, sdkStore, clusterService, ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED);
    }
}
