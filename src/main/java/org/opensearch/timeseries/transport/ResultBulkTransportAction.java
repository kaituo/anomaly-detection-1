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

package org.opensearch.timeseries.transport;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.IndexingPressure.MAX_INDEXING_BYTES;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexingPressure;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.util.BulkUtil;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

@SuppressWarnings("rawtypes")
public abstract class ResultBulkTransportAction<ResultType extends IndexableResult, ADResultWriteRequestType extends ResultWriteRequest<ResultType>, ResultBulkRequestType extends ResultBulkRequest<ResultType, ADResultWriteRequestType>>
    extends HandledTransportAction<ResultBulkRequestType, ResultBulkResponse> {
    private static final Logger LOG = LogManager.getLogger(ResultBulkTransportAction.class);
    protected IndexingPressure indexingPressure;
    private final long primaryAndCoordinatingLimits;
    protected float softLimit;
    protected float hardLimit;
    protected String indexName;
    private final DataAccess dataAccess;
    protected Random random;
    protected StateManager nodeStateManager;

    public ResultBulkTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        Settings settings,
        DataAccess dataAccess,
        float softLimit,
        float hardLimit,
        String indexName,
        Writeable.Reader<ResultBulkRequestType> requestReader
    ) {
        super(actionName, transportService, actionFilters, requestReader, ThreadPool.Names.SAME);
        this.indexingPressure = indexingPressure;
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.dataAccess = dataAccess;

        this.softLimit = softLimit;
        this.hardLimit = hardLimit;
        this.indexName = indexName;

        // random seed is 42. Can be any number
        this.random = new Random(42);
    }

    @Override
    protected void doExecute(Task task, ResultBulkRequestType request, ActionListener<ResultBulkResponse> listener) {
        long startNanos = System.nanoTime();
        LOG.info("Result bulk transport received tenant [{}], resultRequests [{}]", request.getTenantId(), request.numberOfActions());
        // Concurrent indexing memory limit = 10% of heap
        // indexing pressure = indexing bytes / indexing limit
        // Write all until index pressure (global indexing memory pressure) is less than 80% of 10% of heap. Otherwise, index
        // all non-zero anomaly grade index requests and index zero anomaly grade index requests with probability (1 - index pressure).
        long totalBytes = indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes() + indexingPressure.getCurrentReplicaBytes();
        float indexingPressurePercent = (float) totalBytes / primaryAndCoordinatingLimits;
        List<? extends ResultWriteRequest> results = request.getResults();

        if (results == null || results.size() < 1) {
            listener.onResponse(new ResultBulkResponse());
        }

        BulkRequest bulkRequest = prepareBulkRequest(indexingPressurePercent, request);
        LOG
            .info(
                "Result bulk transport prepared tenant [{}], resultRequests [{}], bulkActions [{}], indices [{}], indexingPressurePercent [{}]",
                request.getTenantId(),
                results == null ? 0 : results.size(),
                bulkRequest.numberOfActions(),
                Arrays.toString(bulkRequest.getIndices().toArray(new String[0])),
                indexingPressurePercent
            );

        if (bulkRequest.numberOfActions() > 0) {
            try (Releasable ignored = dataAccess.bindRouting(request.getTenantId(), request.getDataSourceId())) {
                dataAccess
                    .bulk(
                        bulkRequest,
                        TenantContext.user(request.getTenantId(), request.getDataSourceId()),
                        ActionListener.wrap(bulkResponse -> {
                            List<IndexRequest> failedRequests = BulkUtil.getFailedIndexRequest(bulkRequest, bulkResponse);
                            List<IndexRequest> missingResultIndexRequests = BulkUtil
                                .getMissingResultIndexRequests(bulkRequest, bulkResponse);
                            LOG
                                .info(
                                    "Result bulk transport indexed tenant [{}], bulkActions [{}], failedRequests [{}], missingResultIndexRequests [{}], elapsedMs [{}]",
                                    request.getTenantId(),
                                    bulkRequest.numberOfActions(),
                                    failedRequests.size(),
                                    missingResultIndexRequests.size(),
                                    elapsedMillis(startNanos)
                                );
                            listener.onResponse(new ResultBulkResponse(failedRequests, missingResultIndexRequests));
                        }, e -> {
                            LOG
                                .error(
                                    "Failed to bulk index AD result for tenant ["
                                        + request.getTenantId()
                                        + "] after ["
                                        + elapsedMillis(startNanos)
                                        + "] ms",
                                    e
                                );
                            listener.onFailure(e);
                        })
                    );
            }
        } else {
            listener.onResponse(new ResultBulkResponse());
        }
    }

    protected abstract BulkRequest prepareBulkRequest(float indexingPressurePercent, ResultBulkRequestType request);

    protected void addResult(BulkRequest bulkRequest, ToXContentObject result, String resultIndex) {
        String index = resultIndex == null ? indexName : resultIndex;
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(index).source(result.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            bulkRequest.add(indexRequest);
        } catch (IOException e) {
            LOG.error("Failed to prepare bulk index request for index " + index, e);
        }
    }

    private static long elapsedMillis(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }
}
