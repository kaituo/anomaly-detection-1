/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.ForecastNodeStateManager;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeState;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SinglePointFeatures;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.task.RealTimeTaskManager;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.transport.SingleStreamResultRequest;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class ForecastReultProcessor extends
    ResultProcessor<NodeState, ForecastResultRequest, ForecastResultResponse, ForecastNodeStateManager> {

    private static final Logger LOG = LogManager.getLogger(ForecastReultProcessor.class);

    public ForecastReultProcessor(
        Setting<TimeValue> requestTimeoutSetting,
        float intervalRatioForRequests,
        String entityResultAction,
        StatNames hcRequestCountStat,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        HashRing hashRing,
        ForecastNodeStateManager nodeStateManager,
        TransportService transportService,
        Stats timeSeriesStats,
        RealTimeTaskManager realTimeTaskManager,
        NamedXContentRegistry xContentRegistry,
        Client client,
        SecurityClientUtil clientUtil,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Class<ForecastResultResponse> transportResultResponseClazz,
        FeatureManager featureManager
    ) {
        super(
            requestTimeoutSetting,
            intervalRatioForRequests,
            entityResultAction,
            hcRequestCountStat,
            settings,
            clusterService,
            threadPool,
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
            featureManager
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
        long dataEndTime
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

            if (featureOptional.getUnprocessedFeatures().isPresent()) {
                // Feature not available is common when we have data holes. Respond empty response
                // and don't log to avoid bloating our logs.
                LOG.debug("No data in current detection window between {} and {} for {}", dataStartTime, dataEndTime, forecasterId);
                listener
                    .onResponse(
                        ResultResponse
                            .create(
                                new ArrayList<FeatureData>(),
                                "No data in current detection window",
                                null,
                                null,
                                false,
                                transportResultResponseClazz
                            )
                    );
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
                        featureOptional.getUnprocessedFeatures().get()
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
                LOG.debug("No data in current detection window between {} and {} for {}", dataStartTime, dataEndTime, forecasterId);
                listener
                    .onResponse(
                        ResultResponse
                            .create(
                                new ArrayList<FeatureData>(),
                                "No data in current detection window",
                                null,
                                null,
                                false,
                                transportResultResponseClazz
                            )
                    );
            } else {
                listener
                    .onResponse(
                        ResultResponse
                            .create(
                                new ArrayList<FeatureData>(),
                                null,
                                null,
                                forecaster.getIntervalInMinutes(),
                                true,
                                transportResultResponseClazz
                            )
                    );
            }
        }, exception -> { handleQueryFailure(exception, listener, forecasterId); });
    }

}
