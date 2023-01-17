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

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.BaseGetConfigTransportAction;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class GetForecasterTransportAction extends
    BaseGetConfigTransportAction<GetForecasterResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement, ForecastTaskManager, Forecaster> {

    // private final Set<String> allProfileTypeStrs;
    // private final Set<DetectorProfileName> allProfileTypes;
    // private final Set<DetectorProfileName> defaultDetectorProfileTypes;
    // private final Set<String> allEntityProfileTypeStrs;
    // private final Set<EntityProfileName> allEntityProfileTypes;
    // private final Set<EntityProfileName> defaultEntityProfileTypes;

    @Inject
    public GetForecasterTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager forecastTaskManager
    ) {
        super(
            transportService,
            nodeFilter,
            actionFilters,
            clusterService,
            client,
            clientUtil,
            settings,
            xContentRegistry,
            forecastTaskManager,
            GetForecasterAction.NAME,
            Forecaster.class,
            Forecaster.FORECAST_PARSE_FIELD_NAME,
            ForecastTaskType.ALL_FORECAST_TASK_TYPES,
            ForecastTaskType.FORECAST_REALTIME_HC_FORECASTER.name(),
            ForecastTaskType.FORECAST_REALTIME_SINGLE_STREAM.name(),
            ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER.name(),
            ForecastTaskType.FORECAST_HISTORICAL_SINGLE_STREAM.name(),
            ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES
        );
    }

    @Override
    protected void getExecuteProfile(
        GetConfigRequest request,
        Entity entity,
        String typesStr,
        boolean all,
        String configId,
        ActionListener<GetForecasterResponse> listener
    ) {
        // TODO Auto-generated method stub

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
        RestStatus restStatus
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
            false
        );
    }
}
