/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.util.Optional;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.ad.caching.ADCacheBuffer;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.ml.ADCheckpointStore;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ADRealTimeInferencer;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ADColdEntityWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.transport.AbstractSingleStreamResultTransportAction;
import org.opensearch.timeseries.transport.SingleStreamResultRequest;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADSingleStreamResultTransportAction extends
    AbstractSingleStreamResultTransportAction<ThresholdedRandomCutForest, ADIndex, ADDelegatingDataManagement, ADCheckpointStore, ADCheckpointWriteWorker, ADCheckpointMaintainWorker, ADCacheBuffer, ADPriorityCache, ADCacheProvider, AnomalyResult, ThresholdingResult, ADColdStart, ADModelManager, ADPriorityCache, ADSaveResultStrategy, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, ADColdStartWorker, ADRealTimeInferencer, ADCheckpointReadWorker, ADResultWriteRequest, ADColdEntityWorker> {

    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public ADSingleStreamResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        ADCacheProvider cache,
        StateManager stateManager,
        ADCheckpointReadWorker checkpointReadQueue,
        ADRealTimeInferencer inferencer,
        NamedXContentRegistry xContentRegistry,
        ThreadPool threadPool,
        ADColdEntityWorker coldEntityQueue
    ) {
        super(
            transportService,
            actionFilters,
            circuitBreakerService,
            cache,
            stateManager,
            checkpointReadQueue,
            ADSingleStreamResultAction.NAME,
            AnalysisType.AD,
            inferencer,
            threadPool,
            ADCommonName.AD_THREAD_POOL_NAME,
            coldEntityQueue
        );
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    public ADResultWriteRequest createResultWriteRequest(Config config, AnomalyResult result) {
        return new ADResultWriteRequest(
            System.currentTimeMillis() + config.getInferredFrequencyInMilliseconds(),
            config.getId(),
            RequestPriority.MEDIUM,
            result,
            config.getCustomResultIndexOrAlias(),
            config.getFlattenResultIndexAlias(),
            config.getDataSourceId()
        );
    }

    @Override
    protected void doExecute(Task task, SingleStreamResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (Strings.isNullOrEmpty(request.getConfigJson())) {
            super.doExecute(task, request, listener);
            return;
        }

        if (circuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getConfigId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String configId = request.getConfigId();

            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(configId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }
                listener = ExceptionUtil.wrapListener(listener, exception, configId);
            }

            onGetConfig(listener, configId, request, previousException).onResponse(Optional.of(parseInlineConfig(request)));
        } catch (Exception exception) {
            listener.onFailure(exception);
        }
    }

    private Config parseInlineConfig(SingleStreamResultRequest request) throws Exception {
        try (
            XContentParser parser = RestHandlerUtils
                .createXContentParserFromRegistry(xContentRegistry, new BytesArray(request.getConfigJson()))
        ) {
            parser.nextToken();
            return org.opensearch.ad.model.AnomalyDetector.parse(parser, request.getConfigId(), null);
        }
    }

}
