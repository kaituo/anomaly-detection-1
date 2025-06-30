/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest.handler;

import static org.opensearch.forecast.model.ForecastTaskType.RUN_ONCE_TASK_TYPES;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_REQUEST_TIMEOUT;

import java.util.List;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.ExecuteForecastResultResponseRecorder;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastResultAction;
import org.opensearch.forecast.transport.ForecastResultRequest;
import org.opensearch.forecast.transport.StopForecasterAction;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.ResultRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: resultAction is local host call only (safe in multitenant); index/stopConfigAction are single-tenant only.")
public class ForecastIndexJobActionHandler extends
    IndexJobActionHandler<ForecastIndex, ForecastDelegatingDataManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastResult, ExecuteForecastResultResponseRecorder> {

    public ForecastIndexJobActionHandler(
        Client client,
        ForecastDelegatingDataManagement indexManagement,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager adTaskManager,
        ExecuteForecastResultResponseRecorder recorder,
        StateManager nodeStateManager,
        Settings settings,
        RunContext runContext
    ) {
        super(
            client,
            indexManagement,
            adTaskManager,
            recorder,
            ForecastResultAction.INSTANCE,
            AnalysisType.FORECAST,
            ForecastIndex.STATE.getIndexName(),
            StopForecasterAction.INSTANCE,
            nodeStateManager,
            runContext,
            settings,
            FORECAST_REQUEST_TIMEOUT
        );
    }

    @Override
    protected ResultRequest createResultRequest(String configID, long start, long end, String tenantId) {
        return new ForecastResultRequest(configID, start, end, tenantId);
    }

    @Override
    protected List<ForecastTaskType> getBatchConfigTaskTypes() {
        return RUN_ONCE_TASK_TYPES;
    }

    /**
     * Stop config.
     * For realtime, will set job as disabled.
     * For run once, will set its task as inactive.
     *
     * @param configId config id
     * @param historical stop historical analysis or not
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    @Override
    public void stopConfig(
        String configId,
        String tenantId,
        boolean historical,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        // make sure forecaster exists
        nodeStateManager.getConfig(configId, tenantId, AnalysisType.FORECAST, (config) -> {
            if (!config.isPresent()) {
                listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, RestStatus.NOT_FOUND));
                return;
            }
            taskManager.getAndExecuteOnLatestConfigLevelTask(configId, tenantId, ForecastTaskType.RUN_ONCE_TASK_TYPES, (task) -> {
                // stop realtime forecaster job
                stopJob(configId, tenantId, transportService, listener);
            }, transportService, true, listener); // true means reset task state as inactive/stopped state
        }, listener);
    }
}
