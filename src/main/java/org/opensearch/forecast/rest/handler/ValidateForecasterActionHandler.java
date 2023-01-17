/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest.handler;

import java.time.Clock;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;

public class ValidateForecasterActionHandler extends AbstractForecasterActionHandler<ValidateConfigResponse> {

    public ValidateForecasterActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        ForecastIndexManagement forecastIndices,
        Config forecaster,
        TimeValue requestTimeout,
        Integer maxSingleStreamForecasters,
        Integer maxHCForecasters,
        Integer maxFeatures,
        Integer maxCategoricalFields,
        Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        Clock clock,
        Settings settings
    ) {
        super(
            clusterService,
            client,
            clientUtil,
            null,
            forecastIndices,
            Config.NO_ID,
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry,
            user,
            null,
            searchFeatureDao,
            validationType,
            true,
            clock,
            settings
        );
    }

}
