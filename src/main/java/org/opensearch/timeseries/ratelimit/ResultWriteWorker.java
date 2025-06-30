/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ratelimit;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.transport.ResultBulkRequest;
import org.opensearch.timeseries.transport.ResultBulkResponse;
import org.opensearch.timeseries.transport.handler.IndexMemoryPressureAwareResultHandler;
import org.opensearch.timeseries.util.ExceptionUtil;

public abstract class ResultWriteWorker<ResultType extends IndexableResult, ResultWriteRequestType extends ResultWriteRequest<ResultType>, BatchRequestType extends ResultBulkRequest<ResultType, ResultWriteRequestType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>, ResultHandlerType extends IndexMemoryPressureAwareResultHandler<ResultType, ResultWriteRequestType, BatchRequestType, ResultBulkResponse, IndexType, DataManagementType>>
    extends BatchWorker<ResultWriteRequestType, BatchRequestType, ResultBulkResponse> {
    private static final Logger LOG = LogManager.getLogger(ResultWriteWorker.class);
    protected final ResultHandlerType resultHandler;
    protected NamedXContentRegistry xContentRegistry;
    private CheckedFunction<XContentParser, ? extends ResultType, IOException> resultParser;

    public ResultWriteWorker(
        String queueName,
        long heapSize,
        int singleRequestSize,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Setting<Integer> concurrencySetting,
        Duration executionTtl,
        Setting<Integer> batchSizeSetting,
        Duration stateTtl,
        StateManager timeSeriesNodeStateManager,
        ResultHandlerType resultHandler,
        NamedXContentRegistry xContentRegistry,
        CheckedFunction<XContentParser, ? extends ResultType, IOException> resultParser,
        AnalysisType context
    ) {
        super(
            queueName,
            heapSize,
            singleRequestSize,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            threadPoolName,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            concurrencySetting,
            executionTtl,
            batchSizeSetting,
            stateTtl,
            timeSeriesNodeStateManager,
            context
        );
        this.resultHandler = resultHandler;
        this.xContentRegistry = xContentRegistry;
        this.resultParser = resultParser;
    }

    @Override
    protected void executeBatchRequest(BatchRequestType request, ActionListener<ResultBulkResponse> listener) {
        if (request.numberOfActions() < 1) {
            listener.onResponse(null);
            return;
        }
        LOG.info("Result write worker flushing tenant [{}] with [{}] result requests", request.getTenantId(), request.numberOfActions());
        resultHandler.flush(request, request.getTenantId(), listener);
    }

    @Override
    protected ActionListener<ResultBulkResponse> getResponseListener(List<ResultWriteRequestType> toProcess, BatchRequestType bulkRequest) {
        return ActionListener.wrap(adResultBulkResponse -> {
            int retryRequests = requestCount(adResultBulkResponse == null ? Optional.empty() : adResultBulkResponse.getRetryRequests());
            int missingResultIndexRequests = requestCount(
                adResultBulkResponse == null ? Optional.empty() : adResultBulkResponse.getMissingResultIndexRequests()
            );
            LOG
                .info(
                    "Result write worker completed tenant [{}], requests [{}], retryRequests [{}], missingResultIndexRequests [{}]",
                    bulkRequest.getTenantId(),
                    toProcess.size(),
                    retryRequests,
                    missingResultIndexRequests
                );
            if (retryRequests == 0 && missingResultIndexRequests == 0) {
                // all successful
                return;
            }

            adResultBulkResponse.getRetryRequests().ifPresent(requests -> enqueueRetryRequestIteration(requests, 0));
            adResultBulkResponse
                .getMissingResultIndexRequests()
                .ifPresent(requests -> recreateMissingCustomResultIndexAndRetryIteration(requests, 0));
        }, exception -> {
            if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                super.putAll(toProcess);
            } else if (ExceptionUtil.isIndexNotFound(exception)) {
                recreateCustomResultIndicesAndRetryRequests(toProcess, 0);
            } else if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get model checkpoint requests or shard not avialble");
                setCoolDownStart();
            }

            for (ResultWriteRequestType request : toProcess) {
                nodeStateManager.setException(request.getConfigId(), exception);
            }
            LOG.error("Fail to save results", exception);
        });
    }

    private int requestCount(Optional<List<IndexRequest>> requests) {
        return requests.map(List::size).orElse(0);
    }

    private void enqueueRetryRequestIteration(List<IndexRequest> requestToRetry, int index) {
        if (index >= requestToRetry.size()) {
            return;
        }
        DocWriteRequest<?> currentRequest = requestToRetry.get(index);
        Optional<ResultType> resultToRetry = getResult(currentRequest);
        if (false == resultToRetry.isPresent()) {
            enqueueRetryRequestIteration(requestToRetry, index + 1);
            return;
        }

        ResultType result = resultToRetry.get();
        String id = result.getConfigId();
        // not sure if we should cache or not. Don't cache to be safe.
        nodeStateManager.getConfig(id, result.getTenantId(), context, false, onGetConfig(requestToRetry, index, id, result));
    }

    protected Optional<ResultType> getResult(DocWriteRequest<?> request) {
        try {
            if (false == (request instanceof IndexRequest)) {
                LOG.error(new ParameterizedMessage("We should only send IndexRquest, but get [{}].", request));
                return Optional.empty();
            }
            // we send IndexRequest previously
            IndexRequest indexRequest = (IndexRequest) request;
            BytesReference indexSource = indexRequest.source();
            MediaType indexContentType = indexRequest.getContentType();
            try (
                XContentParser xContentParser = XContentHelper
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, indexSource, indexContentType)
            ) {
                // the first character is null. Without skipping it, we get
                // org.opensearch.core.common.ParsingException: Failed to parse object: expecting token of type [START_OBJECT] but found
                // [null]
                xContentParser.nextToken();
                return Optional.of(resultParser.apply(xContentParser));
            }
        } catch (Exception e) {
            LOG.error(new ParameterizedMessage("Fail to parse index request [{}]", request), e);
        }
        return Optional.empty();
    }

    private ActionListener<Optional<? extends Config>> onGetConfig(
        List<IndexRequest> requestToRetry,
        int index,
        String id,
        ResultType resultToRetry
    ) {
        return ActionListener.wrap(configOptional -> {
            if (false == configOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("Config [{}] is not available.", id));
                enqueueRetryRequestIteration(requestToRetry, index + 1);
                return;
            }

            Config config = configOptional.get();
            requeueResult(resultToRetry, config);

            enqueueRetryRequestIteration(requestToRetry, index + 1);

        }, exception -> {
            LOG.error(new ParameterizedMessage("fail to get config [{}]", id), exception);
            enqueueRetryRequestIteration(requestToRetry, index + 1);
        });
    }

    private void recreateMissingCustomResultIndexAndRetryIteration(List<IndexRequest> requestToRetry, int index) {
        if (index >= requestToRetry.size()) {
            return;
        }
        IndexRequest currentRequest = requestToRetry.get(index);
        Optional<ResultType> resultToRetry = getResult(currentRequest);
        if (false == resultToRetry.isPresent()) {
            recreateMissingCustomResultIndexAndRetryIteration(requestToRetry, index + 1);
            return;
        }

        ResultType result = resultToRetry.get();
        String id = result.getConfigId();
        nodeStateManager.getConfig(id, result.getTenantId(), context, false, ActionListener.wrap(configOptional -> {
            if (false == configOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("Config [{}] is not available.", id));
                recreateMissingCustomResultIndexAndRetryIteration(requestToRetry, index + 1);
                return;
            }

            Config config = configOptional.get();
            String customResultIndex = config.getCustomResultIndexOrAlias();
            if (customResultIndex == null || false == customResultIndex.equals(currentRequest.index())) {
                LOG
                    .warn(
                        "Skip recreating custom result index for config [{}]. Failed index [{}], configured custom result index [{}]",
                        id,
                        currentRequest.index(),
                        customResultIndex
                    );
                recreateMissingCustomResultIndexAndRetryIteration(requestToRetry, index + 1);
                return;
            }

            String tenantId = result.getTenantId() == null ? config.getTenantId() : result.getTenantId();
            resultHandler.initCustomResultIndexForRetry(customResultIndex, tenantId, config.getDataSourceId(), ActionListener.wrap(r -> {
                requeueResult(result, config);
                recreateMissingCustomResultIndexAndRetryIteration(requestToRetry, index + 1);
            }, exception -> {
                LOG.error(new ParameterizedMessage("Fail to recreate custom result index [{}]", customResultIndex), exception);
                nodeStateManager.setException(id, exception);
                recreateMissingCustomResultIndexAndRetryIteration(requestToRetry, index + 1);
            }));
        }, exception -> {
            LOG.error(new ParameterizedMessage("fail to get config [{}]", id), exception);
            recreateMissingCustomResultIndexAndRetryIteration(requestToRetry, index + 1);
        }));
    }

    private void recreateCustomResultIndicesAndRetryRequests(List<ResultWriteRequestType> requestsToRetry, int index) {
        if (index >= requestsToRetry.size()) {
            return;
        }
        ResultWriteRequestType request = requestsToRetry.get(index);
        String customResultIndex = request.getResultIndex();
        if (customResultIndex == null) {
            recreateCustomResultIndicesAndRetryRequests(requestsToRetry, index + 1);
            return;
        }
        resultHandler
            .initCustomResultIndexForRetry(customResultIndex, request.getTenantId(), request.getDataSourceId(), ActionListener.wrap(r -> {
                super.put(request);
                recreateCustomResultIndicesAndRetryRequests(requestsToRetry, index + 1);
            }, exception -> {
                LOG.error(new ParameterizedMessage("Fail to recreate custom result index [{}]", customResultIndex), exception);
                nodeStateManager.setException(request.getConfigId(), exception);
                recreateCustomResultIndicesAndRetryRequests(requestsToRetry, index + 1);
            }));
    }

    private void requeueResult(ResultType result, Config config) {
        ResultWriteRequestType request = createResultWriteRequest(
            // expire based on execute start time
            result.getExecutionStartTime().toEpochMilli() + config.getInferredFrequencyInMilliseconds(),
            result.getConfigId(),
            result.isHighPriority() ? RequestPriority.HIGH : RequestPriority.MEDIUM,
            result,
            config.getCustomResultIndexOrAlias(),
            config.getFlattenResultIndexAlias()
        );
        request.setDataSourceId(config.getDataSourceId());
        super.put(request);
    }

    protected abstract ResultWriteRequestType createResultWriteRequest(
        long expirationEpochMs,
        String configId,
        RequestPriority priority,
        ResultType result,
        String resultIndex,
        String flattenResultIndex
    );
}
