/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.client.ForecastNodeCommunicator;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.rest.handler.store.ForecastDelegatingDataManagement;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.transport.TransportService;

public class ForecastResultTransportAction extends HandledTransportAction<ForecastResultRequest, ForecastResultResponse> {

    private static final Logger LOG = LogManager.getLogger(ForecastResultTransportAction.class);
    private ResultProcessor<ForecastResultRequest, ForecastResult, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastDelegatingDataManagement, ForecastTaskManager> resultProcessor;
    private CircuitBreakerService circuitBreakerService;
    // Cache HC forecaster id. This is used to count HC failure stats. We can tell a forecaster
    // is HC or not by checking if forecaster id exists in this field or not. Will add
    // forecaster id to this field when start to run realtime detection and remove forecaster
    // id once realtime detection done.
    private final Set<String> hcForecasters;
    private final ForecastStats forecastStats;
    private final StateManager nodeStateManager;
    private final Settings settings;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final HashRing hashRing;
    private final TransportService transportService;
    private final ForecastTaskManager realTimeTaskManager;
    private final NamedXContentRegistry xContentRegistry;
    private final DataAccess dataAccess;
    private final FeatureManager featureManager;
    private final DiscoveryNodeSelector discoveryNodeSelector;
    private final RunContext runContext;
    private final ForecastNodeCommunicator nodeCommunicator;

    @Inject
    public ForecastResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        DataAccess dataAccess,
        StateManager nodeStateManager,
        FeatureManager featureManager,
        ForecastModelManager modelManager,
        HashRing hashRing,
        ClusterService clusterService,
        CircuitBreakerService circuitBreakerService,
        ForecastStats forecastStats,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager realTimeTaskManager,
        DiscoveryNodeSelector discoveryNodeSelector,
        RunContext runContext,
        ForecastNodeCommunicator nodeCommunicator
    ) {
        super(ForecastResultAction.NAME, transportService, actionFilters, ForecastResultRequest::new);

        this.settings = settings;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.hashRing = hashRing;
        this.transportService = transportService;
        this.realTimeTaskManager = realTimeTaskManager;
        this.xContentRegistry = xContentRegistry;
        this.dataAccess = dataAccess;
        this.featureManager = featureManager;
        this.discoveryNodeSelector = discoveryNodeSelector;
        this.runContext = runContext;
        this.nodeCommunicator = nodeCommunicator;

        this.circuitBreakerService = circuitBreakerService;
        this.hcForecasters = new HashSet<>();
        this.forecastStats = forecastStats;
        this.nodeStateManager = nodeStateManager;

        this.resultProcessor = null;
    }

    @Override
    protected void doExecute(Task task, ForecastResultRequest request, ActionListener<ForecastResultResponse> listener) {
        try {
            TenantAwareHelper.validateTenantId(request.getTenantId(), settings, ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        runContext.runWithSystemAuth(() -> {
            String forecastID = request.getConfigId();
            ActionListener<ForecastResultResponse> original = listener;
            ActionListener<ForecastResultResponse> wrappedListener = ActionListener.wrap(r -> {
                hcForecasters.remove(forecastID);
                original.onResponse(r);
            }, e -> {
                // If exception is TimeSeriesException and it should not be counted in stats,
                // we will not count it in failure stats.
                if (!(e instanceof TimeSeriesException) || ((TimeSeriesException) e).isCountedInStats()) {
                    forecastStats.getStat(StatNames.FORECAST_EXECUTE_FAIL_COUNT.getName()).increment();
                    if (hcForecasters.contains(forecastID)) {
                        forecastStats.getStat(StatNames.FORECAST_HC_EXECUTE_FAIL_COUNT.getName()).increment();
                    }
                }
                hcForecasters.remove(forecastID);
                original.onFailure(e);
            });

            if (!ForecastEnabledSetting.isForecastEnabled()) {
                throw new EndRunException(forecastID, ForecastCommonMessages.DISABLED_ERR_MSG, true).countedInStats(false);
            }

            forecastStats.getStat(StatNames.FORECAST_EXECUTE_REQUEST_COUNT.getName()).increment();

            if (circuitBreakerService.isOpen()) {
                wrappedListener.onFailure(new LimitExceededException(forecastID, CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
                return;
            }

            this.resultProcessor = new ForecastResultProcessor(
                ForecastSettings.FORECAST_REQUEST_TIMEOUT,
                StatNames.FORECAST_HC_EXECUTE_REQUEST_COUNT,
                settings,
                clusterService,
                threadPool,
                hashRing,
                nodeStateManager,
                transportService,
                forecastStats,
                realTimeTaskManager,
                xContentRegistry,
                dataAccess,
                ForecastResultResponse.class,
                featureManager,
                AnalysisType.FORECAST,
                false,
                discoveryNodeSelector,
                nodeCommunicator
            );

            try {
                nodeStateManager
                    .getConfig(
                        forecastID,
                        request.getTenantId(),
                        AnalysisType.FORECAST,
                        // only used for real time
                        true,
                        resultProcessor.onGetConfig(wrappedListener, forecastID, request, Optional.of(hcForecasters))
                    );
            } catch (Exception ex) {
                ResultProcessor.handleExecuteException(ex, wrappedListener, forecastID);
            }
        }, exception -> {
            LOG.error(exception);
            listener.onFailure(exception);
        });
    }
}
