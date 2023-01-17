/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import java.time.Instant;
import java.util.ArrayList;

import org.opensearch.client.Client;
import org.opensearch.commons.authuser.User;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.handler.ResultIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class ExecuteForecastResultResponseRecorder extends
    ExecuteResultResponseRecorder<ForecastIndex, ForecastIndexManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager> {

    public ExecuteForecastResultResponseRecorder(
        ForecastIndexManagement indexManagement,
        ResultIndexingHandler<IndexableResult, ForecastIndex, ForecastIndexManagement> resultHandler,
        ForecastTaskManager taskManager,
        DiscoveryNodeFilterer nodeFilter,
        ThreadPool threadPool,
        Client client,
        NodeStateManager nodeStateManager,
        TaskCacheManager taskCacheManager,
        int rcfMinSamples
    ) {
        super(
            indexManagement,
            resultHandler,
            taskManager,
            nodeFilter,
            threadPool,
            client,
            nodeStateManager,
            taskCacheManager,
            rcfMinSamples,
            ForecastIndex.RESULT,
            AnalysisType.FORECAST
        );
    }

    @Override
    protected ForecastResult createErrorResult(
        String configId,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executeEndTime,
        String errorMessage,
        User user
    ) {
        return new ForecastResult(
            configId,
            null, // no task id
            new ArrayList<FeatureData>(),
            dataStartTime,
            dataEndTime,
            executeEndTime,
            Instant.now(),
            errorMessage,
            null, // single-stream forecasters have no entity
            user,
            indexManagement.getSchemaVersion(resultIndex),
            null // no model id
        );
    }
}
