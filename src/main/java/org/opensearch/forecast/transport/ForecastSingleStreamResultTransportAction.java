/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastCheckpointReadWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.forecast.ratelimit.ForecastResultWriteWorker;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.transport.SingleStreamResultRequest;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastSingleStreamResultTransportAction extends HandledTransportAction<SingleStreamResultRequest, AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityForecastResultTransportAction.class);
    private CircuitBreakerService circuitBreakerService;
    private ForecastCacheProvider cache;
    private final NodeStateManager stateManager;
    private ForecastCheckpointReadWorker checkpointReadQueue;
    private ForecastModelManager modelManager;
    private ForecastIndexManagement indexUtil;
    private ForecastResultWriteWorker resultWriteQueue;
    private Stats stats;
    private ForecastColdStartWorker forecastColdStartQueue;

    @Inject
    public ForecastSingleStreamResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        ForecastCacheProvider cache,
        NodeStateManager stateManager,
        ForecastCheckpointReadWorker checkpointReadQueue,
        ForecastModelManager modelManager,
        ForecastIndexManagement indexUtil,
        ForecastResultWriteWorker resultWriteQueue,
        Stats stats,
        ForecastColdStartWorker forecastColdStartQueue
    ) {
        super(ForecastSingleStreamResultAction.NAME, transportService, actionFilters, SingleStreamResultRequest::new);
        this.circuitBreakerService = circuitBreakerService;
        this.cache = cache;
        this.stateManager = stateManager;
        this.checkpointReadQueue = checkpointReadQueue;
        this.modelManager = modelManager;
        this.indexUtil = indexUtil;
        this.resultWriteQueue = resultWriteQueue;
        this.stats = stats;
        this.forecastColdStartQueue = forecastColdStartQueue;
    }

    @Override
    protected void doExecute(Task task, SingleStreamResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (circuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getConfigId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String forecasterId = request.getConfigId();

            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(forecasterId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error("Previous exception of {}: {}", forecasterId, exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }

                listener = ExceptionUtil.wrapListener(listener, exception, forecasterId);
            }

            stateManager.getConfig(forecasterId, AnalysisType.FORECAST, onGetConfig(listener, forecasterId, request, previousException));
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }
    }

    public ActionListener<Optional<? extends Config>> onGetConfig(
        ActionListener<AcknowledgedResponse> listener,
        String forecasterId,
        SingleStreamResultRequest request,
        Optional<Exception> prevException
    ) {
        return ActionListener.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new EndRunException(forecasterId, "Config " + forecasterId + " is not available.", false));
                return;
            }

            Config config = configOptional.get();

            Instant executionStartTime = Instant.now();

            String modelId = request.getModelId();
            double[] datapoint = request.getDataPoint();
            ModelState<RCFCaster> modelState = cache.get().get(modelId, config);
            if (modelState == null) {
                // cache miss
                checkpointReadQueue
                    .put(
                        new FeatureRequest(
                            System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                            forecasterId,
                            RequestPriority.MEDIUM,
                            request.getModelId(),
                            datapoint,
                            request.getStart()
                        )
                    );
            } else {
                try {
                    RCFCasterResult result = modelManager
                        .getResult(
                            new Sample(datapoint, Instant.ofEpochMilli(request.getStart()), Instant.ofEpochMilli(request.getEnd())),
                            modelState,
                            modelId,
                            Optional.empty(),
                            config
                        );
                    // result.getRcfScore() = 0 means the model is not initialized
                    if (result.getRcfScore() > 0) {
                        List<ForecastResult> indexableResults = result
                            .toIndexableResults(
                                config,
                                Instant.ofEpochMilli(request.getStart()),
                                Instant.ofEpochMilli(request.getEnd()),
                                executionStartTime,
                                Instant.now(),
                                ParseUtils.getFeatureData(datapoint, config),
                                Optional.empty(),
                                indexUtil.getSchemaVersion(ForecastIndex.RESULT),
                                modelId,
                                null,
                                null
                            );

                        for (ForecastResult r : indexableResults) {
                            resultWriteQueue
                                .put(
                                    ResultWriteRequest
                                        .create(
                                            System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                                            forecasterId,
                                            RequestPriority.MEDIUM,
                                            r,
                                            config.getCustomResultIndex(),
                                            ForecastResultWriteRequest.class
                                        )
                                );
                        }
                    }
                } catch (IllegalArgumentException e) {
                    // fail to score likely due to model corruption. Re-cold start to recover.
                    LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", modelId), e);
                    stats.getStat(StatNames.FORECAST_MODEL_CORRUTPION_COUNT.getName()).increment();
                    cache.get().removeModel(forecasterId, modelId);
                    forecastColdStartQueue
                        .put(
                            new FeatureRequest(
                                System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                                forecasterId,
                                RequestPriority.MEDIUM,
                                modelId,
                                datapoint,
                                request.getStart()
                            )
                        );
                }
            }

            // respond back
            if (prevException.isPresent()) {
                listener.onFailure(prevException.get());
            } else {
                listener.onResponse(new AcknowledgedResponse(true));
            }
        }, exception -> {
            LOG
                .error(
                    new ParameterizedMessage(
                        "fail to get entity's anomaly grade for detector [{}]: start: [{}], end: [{}]",
                        forecasterId,
                        request.getStart(),
                        request.getEnd()
                    ),
                    exception
                );
            listener.onFailure(exception);
        });
    }
}
