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

package org.opensearch.timeseries.transport.handler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.transport.ResultBulkRequest;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.Client;

/**
 * Different from ResultIndexingHandler and ResultBulkIndexingHandler, this class uses
 * customized transport action to bulk index results. These transport action will
 * reduce traffic when index memory pressure is high.
 *
 * @param <ResultType> indexed result type
 * @param <ResultWriteRequestType> single-result write request type
 * @param <BatchRequestType> Batch request type
 * @param <BatchResponseType> Batch response type
 * @param <IndexType> forecasting or AD result index
 * @param <DataManagementType> Index management class
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Local host call only. Safe in multitenant.")
public abstract class IndexMemoryPressureAwareResultHandler<ResultType extends IndexableResult, ResultWriteRequestType extends ResultWriteRequest<ResultType>, BatchRequestType extends ResultBulkRequest<ResultType, ResultWriteRequestType>, BatchResponseType, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>> {
    private static final Logger LOG = LogManager.getLogger(IndexMemoryPressureAwareResultHandler.class);

    protected final Client client;
    protected final DataManagementType timeSeriesIndices;
    protected final DiscoveryNodeSelector discoveryNodeSelector;

    public IndexMemoryPressureAwareResultHandler(
        Client client,
        DataManagementType timeSeriesIndices,
        DiscoveryNodeSelector discoveryNodeSelector
    ) {
        this.client = client;
        this.timeSeriesIndices = timeSeriesIndices;
        this.discoveryNodeSelector = discoveryNodeSelector;
    }

    /**
     * Execute the bulk request.
     *
     * @param currentBulkRequest The bulk request
     * @param listener callback after flushing
     */
    public void flush(BatchRequestType currentBulkRequest, String tenantId, ActionListener<BatchResponseType> listener) {
        // we don't check index blocked as ResultIndexingHandler.index did as the batch request can contains different
        // custom index or default indices.
        discoveryNodeSelector.hasGlobalBlock(ActionListener.wrap(hasGlobalBlock -> {
            if (hasGlobalBlock) {
                listener.onFailure(new TimeSeriesException(CommonMessages.CANNOT_SAVE_RESULT_ERR_MSG));
                return;
            }
            proceedWithFlush(currentBulkRequest, tenantId, listener);
        }, e -> {
            LOG.error("Failed to check global block", e);
            listener.onFailure(new TimeSeriesException(CommonMessages.CANNOT_SAVE_RESULT_ERR_MSG, e));
        }));
    }

    private void proceedWithFlush(BatchRequestType currentBulkRequest, String tenantId, ActionListener<BatchResponseType> listener) {
        Set<String> customResultIndexOrAlias = new HashSet<>();
        for (ResultWriteRequestType result : currentBulkRequest.getResults()) {
            if (result.getResultIndex() != null) {
                customResultIndexOrAlias.add(result.getResultIndex());
            }
        }
        List<String> customResultIndexOrAliasList = new ArrayList<>(customResultIndexOrAlias);

        // We create custom result index when creating a detector. Custom result index can be rolled over and thus we may need to create a
        // new one.
        if (!timeSeriesIndices.doesDefaultResultIndexExist()) {
            timeSeriesIndices.initDefaultResultIndexDirectly(ActionListener.wrap(initResponse -> {
                if (initResponse.isAcknowledged()) {
                    initCustomIndices(currentBulkRequest, customResultIndexOrAliasList, tenantId, listener);
                } else {
                    LOG.warn("Creating result index with mappings call not acknowledged.");
                    listener.onFailure(new TimeSeriesException("", "Creating result index with mappings call not acknowledged."));
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    initCustomIndices(currentBulkRequest, customResultIndexOrAliasList, tenantId, listener);
                } else {
                    LOG.warn("Unexpected error creating result index", exception);
                    listener.onFailure(exception);
                }
            }));
        } else {
            initCustomIndices(currentBulkRequest, customResultIndexOrAliasList, tenantId, listener);
        }
    }

    private void initCustomIndices(
        BatchRequestType currentBulkRequest,
        List<String> customResultIndexOrAlias,
        String tenantId,
        ActionListener<BatchResponseType> listener
    ) {
        initCustomIndicesIteration(0, currentBulkRequest, customResultIndexOrAlias, tenantId, listener);
    }

    private void initCustomIndicesIteration(
        int i,
        BatchRequestType currentBulkRequest,
        List<String> customResultIndexOrAlias,
        String tenantId,
        ActionListener<BatchResponseType> listener
    ) {
        if (i >= customResultIndexOrAlias.size()) {
            bulk(currentBulkRequest, listener);
            return;
        }
        String indexOrAliasName = customResultIndexOrAlias.get(i);
        if (!timeSeriesIndices.doesResultIndexExists(indexOrAliasName, tenantId)
            && !timeSeriesIndices.doesResultAliasExists(indexOrAliasName, tenantId)) {
            timeSeriesIndices.initCustomResultIndexDirectly(indexOrAliasName, ActionListener.wrap(initResponse -> {
                if (initResponse.isAcknowledged()) {
                    initCustomIndicesIteration(i + 1, currentBulkRequest, customResultIndexOrAlias, tenantId, listener);
                } else {
                    LOG.warn("Creating result index {} with mappings call not acknowledged.", indexOrAliasName);
                    initCustomIndicesIteration(i + 1, currentBulkRequest, customResultIndexOrAlias, tenantId, listener);
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    initCustomIndicesIteration(i + 1, currentBulkRequest, customResultIndexOrAlias, tenantId, listener);
                } else {
                    LOG.warn("Unexpected error creating result index", exception);
                    initCustomIndicesIteration(i + 1, currentBulkRequest, customResultIndexOrAlias, tenantId, listener);
                }
            }), tenantId);
        } else {
            bulk(currentBulkRequest, listener);
        }
    }

    protected abstract void bulk(BatchRequestType currentBulkRequest, ActionListener<BatchResponseType> listener);
}
