/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskProfile;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.ForecasterProfile;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ProfileRunner;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.TransportService;

public class ForecastProfileRunner extends
    ProfileRunner<TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastDelegatingDataManagement, ForecastTaskProfile, ForecastTaskManager, ForecasterProfile, ForecastProfileAction, ForecastTaskProfileRunner> {

    public ForecastProfileRunner(
        NodeCommunicator nodeCommunicator,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeSelector nodeFilter,
        long requiredSamples,
        TransportService transportService,
        ForecastTaskManager forecastTaskManager,
        ForecastTaskProfileRunner taskProfileRunner,
        DataAccess dataAccess
    ) {
        super(
            nodeCommunicator,
            dataAccess,
            xContentRegistry,
            nodeFilter,
            requiredSamples,
            transportService,
            forecastTaskManager,
            AnalysisType.FORECAST,
            ForecastTaskType.REALTIME_TASK_TYPES,
            ForecastTaskType.RUN_ONCE_TASK_TYPES,
            ForecastNumericSetting.maxCategoricalFields(),
            ProfileName.FORECAST_TASK,
            ForecastProfileAction.INSTANCE,
            taskProfileRunner
        );
    }

    @Override
    protected ForecasterProfile.Builder createProfileBuilder() {
        return new ForecasterProfile.Builder();
    }

}
