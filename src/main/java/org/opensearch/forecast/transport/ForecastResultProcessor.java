/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MAX_ENTITIES_PER_INTERVAL;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_PAGE_SIZE;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SinglePointFeatures;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.transport.SingleStreamResultRequest;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class ForecastResultProcessor extends
    ResultProcessor<ForecastResultRequest, ForecastResult, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement, ForecastTaskManager> {

    private static final Logger LOG = LogManager.getLogger(ForecastResultProcessor.class);

    public ForecastResultProcessor(
        Setting<TimeValue> requestTimeoutSetting,
        float intervalRatioForRequests,
        String entityResultAction,
        StatNames hcRequestCountStat,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        HashRing hashRing,
        NodeStateManager nodeStateManager,
        TransportService transportService,
        ForecastStats timeSeriesStats,
        ForecastTaskManager realTimeTaskManager,
        NamedXContentRegistry xContentRegistry,
        Client client,
        SecurityClientUtil clientUtil,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Class<ForecastResultResponse> transportResultResponseClazz,
        FeatureManager featureManager,
        AnalysisType analysisType,
        boolean runOnce
    ) {
        super(
            requestTimeoutSetting,
            intervalRatioForRequests,
            entityResultAction,
            hcRequestCountStat,
            settings,
            clusterService,
            threadPool,
            TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME,
            hashRing,
            nodeStateManager,
            transportService,
            timeSeriesStats,
            realTimeTaskManager,
            xContentRegistry,
            client,
            clientUtil,
            indexNameExpressionResolver,
            transportResultResponseClazz,
            featureManager,
            FORECAST_MAX_ENTITIES_PER_INTERVAL,
            FORECAST_PAGE_SIZE,
            analysisType,
            runOnce
        );
    }

    @Override
    protected ActionListener<SinglePointFeatures> onFeatureResponseForSingleStreamConfig(
        String forecasterId,
        Config config,
        ActionListener<ForecastResultResponse> listener,
        String rcfModelId,
        DiscoveryNode rcfNode,
        long dataStartTime,
        long dataEndTime,
        String taskId
    ) {
        return ActionListener.wrap(featureOptional -> {
            Optional<Exception> previousException = nodeStateManager.fetchExceptionAndClear(forecasterId);
            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error(new ParameterizedMessage("Previous forecast exception of [{}]", forecasterId), exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }
            }

            Forecaster forecaster = (Forecaster) config;

            if (featureOptional.getUnprocessedFeatures().isEmpty()) {
                // Feature not available is common when we have data holes. Respond empty response
                // and don't log to avoid bloating our logs.
                LOG.debug("No data in current window between {} and {} for {}", dataStartTime, dataEndTime, forecasterId);
                listener
                    .onResponse(createResultResponse(new ArrayList<FeatureData>(), "No data in current window", null, null, false, taskId));
                return;
            }

            final AtomicReference<Exception> failure = new AtomicReference<Exception>();

            LOG.info("Sending forecast single stream request to {} for model {}", rcfNode.getId(), rcfModelId);

            transportService
                .sendRequest(
                    rcfNode,
                    ForecastSingleStreamResultAction.NAME,
                    new SingleStreamResultRequest(
                        forecasterId,
                        rcfModelId,
                        dataStartTime,
                        dataEndTime,
                        featureOptional.getUnprocessedFeatures().get(),
                        taskId
                    ),
                    option,
                    new ActionListenerResponseHandler<>(
                        new ErrorResponseListener(rcfNode.getId(), forecasterId, failure),
                        AcknowledgedResponse::new,
                        ThreadPool.Names.SAME
                    )
                );

            if (previousException.isPresent()) {
                listener.onFailure(previousException.get());
            } else if (!featureOptional.getUnprocessedFeatures().isPresent()) {
                // Feature not available is common when we have data holes. Respond empty response
                // and don't log to avoid bloating our logs.
                LOG.debug("No data in current window between {} and {} for {}", dataStartTime, dataEndTime, forecasterId);
                listener
                    .onResponse(createResultResponse(new ArrayList<FeatureData>(), "No data in current window", null, null, false, taskId));
            } else {
                listener
                    .onResponse(
                        createResultResponse(new ArrayList<FeatureData>(), null, null, forecaster.getIntervalInMinutes(), true, taskId)
                    );
            }
        }, exception -> { handleQueryFailure(exception, listener, forecasterId); });
    }

    @Override
    protected ForecastResultResponse createResultResponse(
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long configInterval,
        Boolean isHC,
        String taskId
    ) {
        return new ForecastResultResponse(features, error, rcfTotalUpdates, configInterval, isHC, taskId);
    }

}
