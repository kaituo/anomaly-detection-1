/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.transport.BaseSuggestConfigParamTransportAction;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class SuggestForecasterParamTransportAction extends BaseSuggestConfigParamTransportAction {
    public static final Logger logger = LogManager.getLogger(SuggestForecasterParamTransportAction.class);

    @Inject
    public SuggestForecasterParamTransportAction(
        Client client,
        SecurityClientUtil clientUtil,
        ClusterService clusterService,
        Settings settings,
        ForecastIndexManagement anomalyDetectionIndices,
        ActionFilters actionFilters,
        TransportService transportService,
        SearchFeatureDao searchFeatureDao
    ) {
        super(
            SuggestForecasterParamAction.NAME,
            client,
            clientUtil,
            clusterService,
            settings,
            actionFilters,
            transportService,
            FORECAST_FILTER_BY_BACKEND_ROLES,
            AnalysisType.FORECAST,
            searchFeatureDao
        );
    }
}
