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

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.timeseries.util.RestHandlerUtils;

/**
 * Utility method to bulk index results.
 *
 * @param <ResultType> the indexed result type
 * @param <IndexType> the time series index enum type
 * @param <DataManagementType> the data management implementation type
 */
public class ResultBulkIndexingHandler<ResultType extends IndexableResult, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>>
    extends ResultIndexingHandler<ResultType, IndexType, DataManagementType> {

    private static final Logger LOG = LogManager.getLogger(ResultBulkIndexingHandler.class);

    public ResultBulkIndexingHandler(
        DataAccess dataAccess,
        Settings settings,
        ThreadPool threadPool,
        String indexName,
        DataManagementType dataManagement,
        DiscoveryNodeSelector discoveryNodeSelector,
        Setting<TimeValue> backOffDelaySetting,
        Setting<Integer> maxRetrySetting
    ) {
        super(dataAccess, settings, threadPool, indexName, dataManagement, discoveryNodeSelector, backOffDelaySetting, maxRetrySetting);
    }

    /**
     * Bulk index results. Create result index first if it doesn't exist.
     *
     * @param resultIndexOrAlias result index
     * @param results results to save
     * @param configId Config Id
     * @param listener action listener
     */
    public void bulk(
        String resultIndexOrAlias,
        List<ResultType> results,
        String configId,
        String tenantId,
        ActionListener<BulkResponse> listener
    ) {
        if (results == null || results.size() == 0) {
            listener.onResponse(null);
            return;
        }

        try {
            if (resultIndexOrAlias != null) {
                // We create custom result index when creating a detector. Custom result index can be rolled over and thus we may need to
                // create a new one.
                if (!dataManagement.doesResultIndexExists(resultIndexOrAlias, tenantId)
                    && !dataManagement.doesResultAliasExists(resultIndexOrAlias, tenantId)) {
                    dataManagement.initCustomResultIndexDirectly(resultIndexOrAlias, ActionListener.wrap(response -> {
                        if (response.isAcknowledged()) {
                            bulk(resultIndexOrAlias, results, tenantId, listener);
                        } else {
                            String error = "Creating custom result index with mappings call not acknowledged";
                            LOG.error(error);
                            listener.onFailure(new TimeSeriesException(error));
                        }
                    }, exception -> {
                        if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                            // It is possible the index has been created while we sending the create request
                            bulk(resultIndexOrAlias, results, tenantId, listener);
                        } else {
                            listener.onFailure(exception);
                        }
                    }), tenantId);
                } else {
                    dataManagement.validateResultIndexMapping(resultIndexOrAlias, ActionListener.wrap(valid -> {
                        if (!valid) {
                            throw new EndRunException(configId, "wrong index mapping of custom result index", true);
                        } else {
                            bulk(resultIndexOrAlias, results, tenantId, listener);
                        }
                    }, listener::onFailure), tenantId);
                }
                return;
            } else if (!dataManagement.doesDefaultResultIndexExist()) {
                dataManagement.initDefaultResultIndexDirectly(ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        bulk(results, tenantId, listener);
                    } else {
                        String error = "Creating result index with mappings call not acknowledged";
                        LOG.error(error);
                        listener.onFailure(new TimeSeriesException(error));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        bulk(results, tenantId, listener);
                    } else {
                        listener.onFailure(exception);
                    }
                }));
            } else {
                bulk(results, tenantId, listener);
            }
        } catch (TimeSeriesException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            String error = "Failed to bulk index result";
            LOG.error(error, e);
            listener.onFailure(new TimeSeriesException(configId, error, e));
        }
    }

    private void bulk(List<ResultType> anomalyResults, String tenantId, ActionListener<BulkResponse> listener) {
        bulk(defaultResultIndexName, anomalyResults, tenantId, listener);
    }

    private void bulk(String resultIndex, List<ResultType> results, String tenantId, ActionListener<BulkResponse> listener) {
        BulkRequest bulkRequest = new BulkRequest();
        results.forEach(analysisResult -> {
            try (XContentBuilder builder = jsonBuilder()) {
                IndexRequest indexRequest = new IndexRequest(resultIndex)
                    .source(analysisResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
                bulkRequest.add(indexRequest);
            } catch (Exception e) {
                String error = "Failed to prepare request to bulk index results";
                LOG.error(error, e);
                throw new TimeSeriesException(error);
            }
        });
        dataAccess.bulk(bulkRequest, TenantContext.user(tenantId), ActionListener.wrap(r -> {
            if (r.hasFailures()) {
                String failureMessage = r.buildFailureMessage();
                LOG.warn("Failed to bulk index result " + failureMessage);
                listener.onFailure(new TimeSeriesException(failureMessage));
            } else {
                listener.onResponse(r);
            }

        }, e -> {
            LOG.error("bulk index result failed", e);
            listener.onFailure(e);
        }));
    }
}
