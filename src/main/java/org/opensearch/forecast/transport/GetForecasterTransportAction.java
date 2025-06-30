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

import java.util.Optional;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.ForecastEntityProfileRunner;
import org.opensearch.forecast.ForecastProfileRunner;
import org.opensearch.forecast.ForecastTaskProfileRunner;
import org.opensearch.forecast.client.ForecastNodeCommunicator;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskProfile;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.model.ForecasterProfile;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.model.EntityProfile;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.BaseGetConfigTransportAction;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.TransportService;

public class GetForecasterTransportAction extends
    BaseGetConfigTransportAction<GetForecasterResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastDelegatingDataManagement, ForecastTaskManager, Forecaster, ForecastEntityProfileRunner, ForecastTaskProfile, ForecasterProfile, ForecastProfileAction, ForecastTaskProfileRunner, ForecastProfileRunner> {

    @Inject
    public GetForecasterTransportAction(
        TransportService transportService,
        DiscoveryNodeSelector nodeFilter,
        ActionFilters actionFilters,
        ClusterService clusterService,
        DataAccess dataAccess,
        StateManager stateManager,
        ForecastNodeCommunicator nodeCommunicator,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager forecastTaskManager,
        ForecastTaskProfileRunner taskProfileRunner,
        ForecastDelegatingDataManagement dataManagement,
        RunContext runContext
    ) {
        super(
            transportService,
            nodeFilter,
            actionFilters,
            clusterService,
            dataAccess,
            stateManager,
            nodeCommunicator,
            settings,
            xContentRegistry,
            forecastTaskManager,
            GetForecasterAction.NAME,
            Forecaster.class,
            ForecastTaskType.ALL_FORECAST_TASK_TYPES,
            ForecastTaskType.REALTIME_FORECAST_HC_FORECASTER.name(),
            ForecastTaskType.REALTIME_FORECAST_SINGLE_STREAM.name(),
            ForecastTaskType.RUN_ONCE_FORECAST_HC_FORECASTER.name(),
            ForecastTaskType.RUN_ONCE_FORECAST_SINGLE_STREAM.name(),
            ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES,
            taskProfileRunner,
            ForecastIndex.CONFIG.getIndexName(),
            dataManagement,
            runContext
        );
    }

    @Override
    protected GetForecasterResponse createResponse(
        long version,
        String id,
        long primaryTerm,
        long seqNo,
        Forecaster config,
        Job job,
        boolean returnJob,
        Optional<ForecastTask> realtimeTask,
        Optional<ForecastTask> historicalTask,
        boolean returnTask,
        RestStatus restStatus,
        ForecasterProfile forecasterProfile,
        EntityProfile entityProfile,
        boolean profileResponse
    ) {
        return new GetForecasterResponse(
            id,
            version,
            primaryTerm,
            seqNo,
            config,
            job,
            returnJob,
            realtimeTask.orElse(null),
            historicalTask.orElse(null),
            returnTask,
            restStatus,
            forecasterProfile,
            entityProfile,
            profileResponse
        );
    }

    @Override
    protected ForecastEntityProfileRunner createEntityProfileRunner(
        NodeCommunicator nodeCommunicator,
        DataAccess dataAccess,
        StateManager stateManager,
        NamedXContentRegistry xContentRegistry
    ) {
        return new ForecastEntityProfileRunner(nodeCommunicator, dataAccess, stateManager, TimeSeriesSettings.NUM_MIN_SAMPLES);
    }

    @Override
    protected ForecastProfileRunner createProfileRunner(
        NodeCommunicator nodeCommunicator,
        DataAccess dataAccess,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeSelector nodeFilter,
        TransportService transportService,
        ForecastTaskManager taskManager,
        ForecastTaskProfileRunner taskProfileRunner
    ) {
        return new ForecastProfileRunner(
            nodeCommunicator,
            xContentRegistry,
            nodeFilter,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            transportService,
            taskManager,
            taskProfileRunner,
            dataAccess
        );
    }

    @Override
    protected void adjustState(Optional<ForecastTask> taskOptional, Job job) {
        if (taskOptional.isPresent()) {
            ForecastTask task = taskOptional.get();
            String state = task.getState();
            if (TaskState.INACTIVE.name().equals(state) || TaskState.STOPPED.name().equals(state)) {
                if (job == null) {
                    task.setState(TaskState.INACTIVE_NOT_STARTED.name());
                } else {
                    task.setState(TaskState.INACTIVE_STOPPED.name());
                }
            }
        }
    }

    @Override
    protected Setting<Boolean> getMultiTenancyEnabledSetting() {
        return ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED;
    }
}
