/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.EntityProfileRunner;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;

public class ForecastEntityProfileRunner extends EntityProfileRunner {

    public ForecastEntityProfileRunner(
        NodeCommunicator nodeCommunicator,
        DataAccess dataAccess,
        StateManager stateManager,
        long requiredSamples
    ) {
        super(
            nodeCommunicator,
            dataAccess,
            stateManager,
            requiredSamples,
            ForecastNumericSetting.maxCategoricalFields(),
            AnalysisType.FORECAST,
            ForecastIndex.RESULT.getIndexName(),
            ForecastCommonName.FORECASTER_ID_KEY
        );
    }
}
