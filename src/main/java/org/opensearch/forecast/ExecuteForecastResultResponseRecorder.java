/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;

import org.opensearch.commons.authuser.User;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

public class ExecuteForecastResultResponseRecorder extends
    ExecuteResultResponseRecorder<ForecastIndex, ForecastDelegatingDataManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastResult> {

    public ExecuteForecastResultResponseRecorder(
        ResultBulkIndexingHandler<ForecastResult, ForecastIndex, ForecastDelegatingDataManagement> resultHandler,
        ForecastTaskManager taskManager,
        DiscoveryNodeSelector nodeFilter,
        ThreadPool threadPool,
        NodeCommunicator nodeCommunicator,
        DataAccess dataAccess,
        StateManager nodeStateManager,
        Clock clock,
        int forecastResultMappingVersion
    ) {
        super(
            resultHandler,
            taskManager,
            nodeFilter,
            threadPool,
            ForecastCommonName.FORECAST_THREAD_POOL_NAME,
            nodeCommunicator,
            dataAccess,
            nodeStateManager,
            clock,
            ForecastIndex.RESULT,
            AnalysisType.FORECAST,
            forecastResultMappingVersion
        );
    }

    @Override
    protected ForecastResult createErrorResult(
        String configId,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executeStartTime,
        Instant executeEndTime,
        String errorMessage,
        User user,
        String tenantId
    ) {
        return new ForecastResult(
            configId,
            null, // no task id
            new ArrayList<FeatureData>(),
            dataStartTime,
            dataEndTime,
            executeStartTime,
            executeEndTime,
            errorMessage,
            Optional.empty(), // single-stream forecasters have no entity
            user,
            resultMappingVersion,
            tenantId
        );
    }

    @Override
    protected void updateRealtimeTask(ResultResponse<ForecastResult> response, String configId, String tenantId, Clock clock) {
        if (taskManager.skipUpdateRealtimeTask(configId, response.getError())) {
            return;
        }

        delayedUpdate(response, configId, tenantId, clock);
    }
}
