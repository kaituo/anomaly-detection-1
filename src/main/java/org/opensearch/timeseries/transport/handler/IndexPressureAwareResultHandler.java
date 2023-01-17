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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.TimeSeriesIndices;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.IndexUtils;

public abstract class IndexPressureAwareResultHandler<T extends ToXContentObject, BatchRequestType, BatchResponseType> extends TimeSeriesResultIndexingHandler<T> {
    private static final Logger LOG = LogManager.getLogger(IndexPressureAwareResultHandler.class);

    public IndexPressureAwareResultHandler(
            Client client,
            Settings settings,
            ThreadPool threadPool,
            String indexName,
            TimeSeriesIndices timeSeriesIndices,
            ClientUtil clientUtil,
            IndexUtils indexUtils,
            ClusterService clusterService,
            Setting<TimeValue> backOffDelaySetting,
            Setting<Integer> maxRetrySetting
        ) {
        super(client,
                 settings,
                 threadPool,
                 indexName,
                 timeSeriesIndices,
                 clientUtil,
                 indexUtils,
                 clusterService,
                backOffDelaySetting,
                maxRetrySetting);
    }

    /**
     * Execute the bulk request
     * @param currentBulkRequest The bulk request
     * @param listener callback after flushing
     */
    public void flush(BatchRequestType currentBulkRequest, ActionListener<BatchResponseType> listener) {
        if (indexUtils.checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, this.indexName)) {
            listener.onFailure(new TimeSeriesException(CommonMessages.CANNOT_SAVE_RESULT_ERR_MSG));
            return;
        }

        try {
            if (!timeSeriesIndices.doesDefaultResultIndexExist()) {
                timeSeriesIndices.initDefaultResultIndexDirectly(ActionListener.wrap(initResponse -> {
                    if (initResponse.isAcknowledged()) {
                        bulk(currentBulkRequest, listener);
                    } else {
                        LOG.warn("Creating result index with mappings call not acknowledged.");
                        listener.onFailure(new TimeSeriesException("", "Creating result index with mappings call not acknowledged."));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        bulk(currentBulkRequest, listener);
                    } else {
                        LOG.warn("Unexpected error creating result index", exception);
                        listener.onFailure(exception);
                    }
                }));
            } else {
                bulk(currentBulkRequest, listener);
            }
        } catch (Exception e) {
            LOG.warn("Error in bulking results", e);
            listener.onFailure(e);
        }
    }

    public abstract void bulk(BatchRequestType currentBulkRequest, ActionListener<BatchResponseType> listener);
}
