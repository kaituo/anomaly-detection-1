/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest.handler;

import static org.opensearch.forecast.model.ForecastTaskType.HISTORICAL_FORECASTER_TASK_TYPES;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_REQUEST_TIMEOUT;

import java.util.List;

import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.ExecuteForecastResultResponseRecorder;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.forecast.transport.ForecastResultAction;
import org.opensearch.forecast.transport.ForecastResultRequest;
import org.opensearch.forecast.transport.StopForecasterAction;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultRequest;

public class ForecastIndexJobActionHandler extends
    IndexJobActionHandler<ForecastIndex, ForecastIndexManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastResult, ForecastProfileAction, ExecuteForecastResultResponseRecorder> {

    public ForecastIndexJobActionHandler(
        Client client,
        ForecastIndexManagement indexManagement,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager adTaskManager,
        ExecuteForecastResultResponseRecorder recorder,
        NodeStateManager nodeStateManager,
        Settings settings
    ) {
        super(
            client,
            indexManagement,
            xContentRegistry,
            adTaskManager,
            recorder,
            ForecastResultAction.INSTANCE,
            AnalysisType.FORECAST,
            ForecastIndex.STATE.getIndexName(),
            StopForecasterAction.INSTANCE,
            nodeStateManager,
            settings,
            FORECAST_REQUEST_TIMEOUT
        );
    }

    @Override
    protected ResultRequest createResultRequest(String configID, long start, long end) {
        return new ForecastResultRequest(configID, start, end, false);
    }

    @Override
    protected List<ForecastTaskType> getHistorialConfigTaskTypes() {
        return HISTORICAL_FORECASTER_TASK_TYPES;
    }
}
