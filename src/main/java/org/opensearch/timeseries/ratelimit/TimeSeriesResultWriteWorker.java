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
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.TimeSeriesNodeStateManager;
import org.opensearch.timeseries.breaker.TimeSeriesCircuitBreakerService;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkRequest;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkResponse;
import org.opensearch.timeseries.transport.handler.IndexPressureAwareResultHandler;
import org.opensearch.timeseries.util.ExceptionUtil;

public abstract class TimeSeriesResultWriteWorker<ResultType extends IndexableResult, ResultWriteRequestType extends ResultWriteRequest<ResultType>, BatchRequestType extends TimeSeriesResultBulkRequest<ResultType, ResultWriteRequestType>, NodeStateType extends ExpiringState>
        extends BatchWorker<ResultWriteRequestType, BatchRequestType, TimeSeriesResultBulkResponse, NodeStateType> {
    private static final Logger LOG = LogManager.getLogger(TimeSeriesResultWriteWorker.class);
    protected final IndexPressureAwareResultHandler<ResultType, BatchRequestType, TimeSeriesResultBulkResponse> resultHandler;
    protected NamedXContentRegistry xContentRegistry;
    private CheckedFunction<XContentParser, ? extends ResultType, IOException> resultParser;

    public TimeSeriesResultWriteWorker(String queueName, long heapSize, int singleRequestSize,
            Setting<Float> maxHeapPercentForQueueSetting, ClusterService clusterService, Random random,
            TimeSeriesCircuitBreakerService adCircuitBreakerService, ThreadPool threadPool, Settings settings,
            float maxQueuedTaskRatio, Clock clock, float mediumSegmentPruneRatio, float lowSegmentPruneRatio,
            int maintenanceFreqConstant, Setting<Integer> concurrencySetting, Duration executionTtl,
            Setting<Integer> batchSizeSetting, Duration stateTtl,
            TimeSeriesNodeStateManager<NodeStateType> timeSeriesNodeStateManager,
            IndexPressureAwareResultHandler<ResultType, BatchRequestType, TimeSeriesResultBulkResponse> resultHandler,
            NamedXContentRegistry xContentRegistry,
            CheckedFunction<XContentParser, ? extends ResultType, IOException> resultParser) {
        super(queueName, heapSize, singleRequestSize, maxHeapPercentForQueueSetting, clusterService, random,
                adCircuitBreakerService, threadPool, settings, maxQueuedTaskRatio, clock, mediumSegmentPruneRatio,
                lowSegmentPruneRatio, maintenanceFreqConstant, concurrencySetting, executionTtl, batchSizeSetting, stateTtl,
                timeSeriesNodeStateManager);
        this.resultHandler = resultHandler;
        this.xContentRegistry = xContentRegistry;
        this.resultParser = resultParser;
    }

    @Override
    protected void executeBatchRequest(BatchRequestType request, ActionListener<TimeSeriesResultBulkResponse> listener) {
        if (request.numberOfActions() < 1) {
            listener.onResponse(null);
            return;
        }
        resultHandler.flush(request, listener);
    }

    @Override
    protected ActionListener<TimeSeriesResultBulkResponse> getResponseListener(
        List<ResultWriteRequestType> toProcess,
        BatchRequestType bulkRequest
    ) {
        return ActionListener.wrap(adResultBulkResponse -> {
            if (adResultBulkResponse == null || false == adResultBulkResponse.getRetryRequests().isPresent()) {
                // all successful
                return;
            }

            enqueueRetryRequestIteration(adResultBulkResponse.getRetryRequests().get(), 0);
        }, exception -> {
            if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                super.putAll(toProcess);
            } else if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get AD model checkpoint requests or shard not avialble");
                setCoolDownStart();
            }

            for (ResultWriteRequestType request : toProcess) {
                nodeStateManager.setException(request.getId(), exception);
            }
            LOG.error("Fail to save results", exception);
        });
    }

    private void enqueueRetryRequestIteration(List<IndexRequest> requestToRetry, int index) {
        if (index >= requestToRetry.size()) {
            return;
        }
        DocWriteRequest<?> currentRequest = requestToRetry.get(index);
        Optional<ResultType> resultToRetry = getAnomalyResult(currentRequest);
        if (false == resultToRetry.isPresent()) {
            enqueueRetryRequestIteration(requestToRetry, index + 1);
            return;
        }
        ResultType result = resultToRetry.get();
        String id = result.getId();
        nodeStateManager.getConfig(id, onGetDetector(requestToRetry, index, id, result));
    }

    protected Optional<ResultType> getAnomalyResult(DocWriteRequest<?> request) {
        try {
            if (false == (request instanceof IndexRequest)) {
                LOG.error(new ParameterizedMessage("We should only send IndexRquest, but get [{}].", request));
                return Optional.empty();
            }
            // we send IndexRequest previously
            IndexRequest indexRequest = (IndexRequest) request;
            BytesReference indexSource = indexRequest.source();
            XContentType indexContentType = indexRequest.getContentType();
            try (
                XContentParser xContentParser = XContentHelper
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, indexSource, indexContentType)
            ) {
                // the first character is null. Without skipping it, we get
                // org.opensearch.common.ParsingException: Failed to parse object: expecting token of type [START_OBJECT] but found
                // [null]
                xContentParser.nextToken();
                return Optional.of(resultParser.apply(xContentParser));
            }
        } catch (Exception e) {
            LOG.error(new ParameterizedMessage("Fail to parse index request [{}]", request), e);
        }
        return Optional.empty();
    }

    private ActionListener<Optional<? extends Config>> onGetDetector(
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
                super.put(
                        createResultWriteRequest(
                             // expire based on execute start time
                                resultToRetry.getExecutionStartTime().toEpochMilli() + config.getIntervalInMilliseconds(),
                                id,
                                resultToRetry.isHighPriority() ? RequestPriority.HIGH : RequestPriority.MEDIUM,
                                resultToRetry,
                                config.getCustomResultIndex()
                                )
                );

                enqueueRetryRequestIteration(requestToRetry, index + 1);

            }, exception -> {
                LOG.error(new ParameterizedMessage("fail to get config [{}]", id), exception);
                enqueueRetryRequestIteration(requestToRetry, index + 1);
            });
        }

    protected abstract ResultWriteRequestType createResultWriteRequest(long expirationEpochMs,
            String configId,
            RequestPriority priority,
            ResultType result,
            String resultIndex);
}
