/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class ForecastRunOnceTransportAction extends HandledTransportAction<ForecastResultRequest, ForecastResultResponse> {

    private static final Logger LOG = LogManager.getLogger(ForecastRunOnceTransportAction.class);
    private ResultProcessor<ForecastResultRequest, ForecastResult, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement, ForecastTaskManager> resultProcessor;
    private final Client client;
    private CircuitBreakerService circuitBreakerService;
    private final NodeStateManager nodeStateManager;

    private final Settings settings;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final HashRing hashRing;
    private final TransportService transportService;
    private final ForecastTaskManager realTimeTaskManager;
    private final NamedXContentRegistry xContentRegistry;
    private final SecurityClientUtil clientUtil;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final FeatureManager featureManager;
    private final ForecastStats forecastStats;

    @Inject
    public ForecastRunOnceTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        Client client,
        SecurityClientUtil clientUtil,
        NodeStateManager nodeStateManager,
        FeatureManager featureManager,
        ForecastModelManager modelManager,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        CircuitBreakerService circuitBreakerService,
        ForecastStats forecastStats,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager realTimeTaskManager
    ) {
        super(ForecastRunOnceAction.NAME, transportService, actionFilters, ForecastResultRequest::new);

        this.resultProcessor = null;
        this.settings = settings;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.hashRing = hashRing;
        this.transportService = transportService;
        this.realTimeTaskManager = realTimeTaskManager;
        this.xContentRegistry = xContentRegistry;
        this.clientUtil = clientUtil;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.featureManager = featureManager;
        this.forecastStats = forecastStats;

        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
        this.nodeStateManager = nodeStateManager;
    }

    @Override
    protected void doExecute(Task task, ForecastResultRequest request, ActionListener<ForecastResultResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            String forecastID = request.getConfigId();

            if (!ForecastEnabledSetting.isForecastEnabled()) {
                listener.onFailure(new EndRunException(forecastID, ForecastCommonMessages.DISABLED_ERR_MSG, true).countedInStats(false));
            }

            if (circuitBreakerService.isOpen()) {
                listener.onFailure(new LimitExceededException(forecastID, CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
                return;
            }
            try {
                resultProcessor = new ForecastReultProcessor(
                    ForecastSettings.FORECAST_REQUEST_TIMEOUT,
                    TimeSeriesSettings.INTERVAL_RATIO_FOR_REQUESTS,
                    EntityForecastResultAction.NAME,
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
                    client,
                    clientUtil,
                    indexNameExpressionResolver,
                    ForecastResultResponse.class,
                    featureManager,
                    AnalysisType.FORECAST,
                    true
                );

                nodeStateManager
                    .getConfig(
                        forecastID,
                        AnalysisType.FORECAST,
                        resultProcessor.onGetConfig(listener, forecastID, request, Optional.empty())
                    );

                AtomicInteger waitTimes = new AtomicInteger(0);

                threadPool
                    .schedule(
                        () -> checkIfRunOnceFinished(forecastID, waitTimes),
                        new TimeValue(10, TimeUnit.SECONDS),
                        TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME
                    );

                // check for status
            } catch (Exception ex) {
                ResultProcessor.handleExecuteException(ex, listener, forecastID);
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private void checkIfRunOnceFinished(String forecastID, AtomicInteger waitTimes) {
        client.execute(ForecastRunOnceProfileAction.INSTANCE, new ForecastRunOnceProfileRequest(forecastID), ActionListener.wrap(r -> {
            if (r.isAnswerTrue()) {
                if (waitTimes.get() < 10) {
                    waitTimes.addAndGet(1);
                    threadPool
                        .schedule(
                            () -> checkIfRunOnceFinished(forecastID, waitTimes),
                            new TimeValue(10, TimeUnit.SECONDS),
                            TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME
                        );
                } else {
                    LOG.warn("Timed out run once of forecaster {}", forecastID);
                }
            }

            LOG.info("Run once of forecaster {} finished", forecastID);
        }, e -> { LOG.error("Failed to finish run once of forecaster " + forecastID, e); }));
    }
}
