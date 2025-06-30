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

package org.opensearch.ad.transport.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.constant.ADCommonName.ANOMALY_RESULT_INDEX_ALIAS;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

import com.google.common.collect.ImmutableList;

public class AnomalyResultBulkIndexHandlerTests extends ADUnitTestCase {

    private ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADDelegatingDataManagement> bulkIndexHandler;
    private ActionListener<BulkResponse> listener;
    private ADDelegatingDataManagement anomalyDetectionIndices;
    private String configId;
    private DataAccess dataAccess;
    private DiscoveryNodeSelector discoveryNodeSelector;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        anomalyDetectionIndices = mock(ADDelegatingDataManagement.class);
        Settings settings = Settings.EMPTY;
        dataAccess = mock(DataAccess.class);
        discoveryNodeSelector = mock(DiscoveryNodeSelector.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        setResultIndexOrAliasExists(false);
        doAnswer(invocation -> {
            ActionListener<BulkResponse> l = invocation.getArgument(2);
            l.onResponse(mock(BulkResponse.class));
            return null;
        }).when(dataAccess).bulk(any(), any(), any());
        bulkIndexHandler = new ResultBulkIndexingHandler(
            dataAccess,
            settings,
            threadPool,
            ANOMALY_RESULT_INDEX_ALIAS,
            anomalyDetectionIndices,
            discoveryNodeSelector,
            AnomalyDetectorSettings.AD_BACKOFF_INITIAL_DELAY,
            AnomalyDetectorSettings.AD_MAX_RETRY_FOR_BACKOFF
        );
        listener = spy(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });
        configId = "testId";
    }

    private void setResultIndexOrAliasExists(boolean... existsValues) {
        AtomicInteger invocationCount = new AtomicInteger();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> existsListener = invocation.getArgument(1);
            int responseIndex = Math.min(invocationCount.getAndIncrement(), existsValues.length - 1);
            existsListener.onResponse(existsValues[responseIndex]);
            return null;
        }).when(anomalyDetectionIndices).doesResultIndexOrAliasExists(any(), any(), nullable(String.class));
    }

    private void failResultIndexOrAliasExists(Exception exception) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> existsListener = invocation.getArgument(1);
            existsListener.onFailure(exception);
            return null;
        }).when(anomalyDetectionIndices).doesResultIndexOrAliasExists(any(), any(), nullable(String.class));
    }

    public void testNullAnomalyResults() {
        bulkIndexHandler.bulk(null, null, null, null, listener);
        verify(listener, times(1)).onResponse(null);
        verify(anomalyDetectionIndices, never()).doesConfigIndexExist();
    }

    public void testAnomalyResultBulkIndexHandler_IndexNotExist() {
        setResultIndexOrAliasExists(false);
        AnomalyResult anomalyResult = mock(AnomalyResult.class);
        when(anomalyResult.getConfigId()).thenReturn(configId);

        bulkIndexHandler.bulk("testIndex", ImmutableList.of(anomalyResult), configId, null, listener);
        verify(anomalyDetectionIndices, times(1)).initCustomResultIndexDirectly(eq("testIndex"), any(), any());
    }

    public void testAnomalyResultBulkIndexHandler_InValidResultIndexMapping() {
        setResultIndexOrAliasExists(true);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(false);
            return null;
        }).when(anomalyDetectionIndices).validateResultIndexMapping(eq("testIndex"), any(), any());

        AnomalyResult anomalyResult = mock(AnomalyResult.class);

        when(anomalyResult.getConfigId()).thenReturn(configId);

        bulkIndexHandler.bulk("testIndex", ImmutableList.of(anomalyResult), configId, null, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("wrong index mapping of custom result index", exceptionCaptor.getValue().getMessage());
    }

    public void testAnomalyResultBulkIndexHandler_FailBulkIndexAnomaly() throws IOException {
        setResultIndexOrAliasExists(true);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(anomalyDetectionIndices).validateResultIndexMapping(eq("testIndex"), any(), any());
        AnomalyResult anomalyResult = mock(AnomalyResult.class);
        when(anomalyResult.getConfigId()).thenReturn(configId);
        when(anomalyResult.toXContent(any(), any())).thenThrow(new RuntimeException());

        bulkIndexHandler.bulk("testIndex", ImmutableList.of(anomalyResult), configId, null, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to prepare request to bulk index results", exceptionCaptor.getValue().getMessage());
    }

    public void testCreateADResultIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(anomalyDetectionIndices).initDefaultResultIndexDirectly(any());
        bulkIndexHandler.bulk(null, ImmutableList.of(mock(AnomalyResult.class)), configId, null, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Creating result index with mappings call not acknowledged", exceptionCaptor.getValue().getMessage());
    }

    public void testWrongAnomalyResult() {
        doReturn(true).when(anomalyDetectionIndices).doesDefaultResultIndexExist();
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(2);
            BulkItemResponse[] bulkItemResponses = new BulkItemResponse[2];
            String indexName = ANOMALY_RESULT_INDEX_ALIAS;
            String type = "_doc";
            String idPrefix = "id";
            String uuid = "uuid";
            int shardIntId = 0;
            ShardId shardId = new ShardId(new Index(indexName, uuid), shardIntId);
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(
                ANOMALY_RESULT_INDEX_ALIAS,
                randomAlphaOfLength(5),
                new VersionConflictEngineException(new ShardId(ANOMALY_RESULT_INDEX_ALIAS, "", 1), "id", "test")
            );
            bulkItemResponses[0] = new BulkItemResponse(0, randomFrom(DocWriteRequest.OpType.values()), failure);
            bulkItemResponses[1] = new BulkItemResponse(
                1,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, idPrefix + 1, 1, 1, randomInt(), true)
            );
            BulkResponse bulkResponse = new BulkResponse(bulkItemResponses, 10);
            listener.onResponse(bulkResponse);
            return null;
        }).when(dataAccess).bulk(any(), any(), any());
        bulkIndexHandler
            .bulk(null, ImmutableList.of(wrongAnomalyResult(), TestHelpers.randomAnomalyDetectResult()), configId, null, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("VersionConflictEngineException"));
    }

    public void testBulkSaveException() {
        doReturn(true).when(anomalyDetectionIndices).doesDefaultResultIndexExist();

        String testError = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException(testError));
            return null;
        }).when(dataAccess).bulk(any(), any(), any());

        bulkIndexHandler.bulk(null, ImmutableList.of(TestHelpers.randomAnomalyDetectResult()), configId, null, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(testError, exceptionCaptor.getValue().getMessage());
    }

    private AnomalyResult wrongAnomalyResult() {
        return new AnomalyResult(
            randomAlphaOfLength(5),
            null,
            randomDouble(),
            randomDouble(),
            randomDouble(),
            null,
            null,
            null,
            null,
            null,
            randomAlphaOfLength(5),
            Optional.empty(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            randomDoubleBetween(1.1, 10.0, true),
            null,
            null
        );
    }

    public void testResponseIsAcknowledgedTrue() throws InterruptedException {
        String testIndex = "testIndex";

        setResultIndexOrAliasExists(false);

        // Mock initCustomResultIndexDirectly to simulate index creation and call the listener
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            // Simulate immediate onResponse call
            listener.onResponse(new CreateIndexResponse(true, true, testIndex));
            return null;
        }).when(anomalyDetectionIndices).initCustomResultIndexDirectly(eq(testIndex), any(), any());

        AnomalyResult result = TestHelpers.randomAnomalyDetectResult();

        // Call bulk method
        bulkIndexHandler.bulk(testIndex, ImmutableList.of(result), configId, null, listener);

        verify(dataAccess, times(1)).bulk(any(), any(), any());
    }

    public void testResponseIsAcknowledgedFalse() {
        String testIndex = "testIndex";
        setResultIndexOrAliasExists(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(new CreateIndexResponse(false, false, testIndex));
            return null;
        }).when(anomalyDetectionIndices).initCustomResultIndexDirectly(eq(testIndex), any(), any());

        AnomalyResult result = TestHelpers.randomAnomalyDetectResult();
        bulkIndexHandler.bulk(testIndex, ImmutableList.of(result), configId, null, listener);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Creating custom result index with mappings call not acknowledged", exceptionCaptor.getValue().getMessage());
    }

    public void testResourceAlreadyExistsException() {
        String testIndex = "testIndex";
        setResultIndexOrAliasExists(false, true);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(new ResourceAlreadyExistsException("index already exists"));
            return null;
        }).when(anomalyDetectionIndices).initCustomResultIndexDirectly(eq(testIndex), any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(anomalyDetectionIndices).validateResultIndexMapping(eq(testIndex), any(), any());

        AnomalyResult result = TestHelpers.randomAnomalyDetectResult();
        bulkIndexHandler.bulk(testIndex, ImmutableList.of(result), configId, null, listener);

        verify(dataAccess, times(1)).bulk(any(), any(), any());
    }

    public void testOtherException() {
        String testIndex = "testIndex";
        setResultIndexOrAliasExists(false);

        Exception testException = new OpenSearchRejectedExecutionException("Test exception");

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(testException);
            return null;
        }).when(anomalyDetectionIndices).initCustomResultIndexDirectly(eq(testIndex), any(), any());

        AnomalyResult result = mock(AnomalyResult.class);
        bulkIndexHandler.bulk(testIndex, ImmutableList.of(result), configId, null, listener);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(testException, exceptionCaptor.getValue());
    }

    public void testTimeSeriesExceptionCaughtInBulk() {
        String testIndex = "testIndex";
        TimeSeriesException testException = new TimeSeriesException("Test TimeSeriesException");

        failResultIndexOrAliasExists(testException);

        AnomalyResult result = mock(AnomalyResult.class);

        // Call bulk method
        bulkIndexHandler.bulk(testIndex, ImmutableList.of(result), configId, null, listener);

        // Verify that listener.onFailure is called with the TimeSeriesException
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(testException, exceptionCaptor.getValue());
    }

    public void testExceptionCaughtInBulk() {
        String testIndex = "testIndex";
        NullPointerException testException = new NullPointerException("Test NullPointerException");

        failResultIndexOrAliasExists(testException);

        AnomalyResult result = mock(AnomalyResult.class);

        // Call bulk method
        bulkIndexHandler.bulk(testIndex, ImmutableList.of(result), configId, null, listener);

        // Verify that listener.onFailure is called with a TimeSeriesException wrapping the original exception
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        Exception capturedException = exceptionCaptor.getValue();
        assertTrue(capturedException instanceof TimeSeriesException);
        assertEquals("Failed to bulk index result", capturedException.getMessage());
        assertEquals(testException, capturedException.getCause());
    }
}
