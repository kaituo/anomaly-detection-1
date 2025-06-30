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

import java.util.Iterator;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.util.BulkUtil;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class ResultIndexingHandler<ResultType extends IndexableResult, IndexType extends Enum<IndexType> & TimeSeriesIndex, DataManagementType extends DelegatingDataManagement<IndexType>> {
    private static final Logger LOG = LogManager.getLogger(ResultIndexingHandler.class);
    public static final String FAIL_TO_SAVE_ERR_MSG = "Fail to save %s: ";
    public static final String SUCCESS_SAVING_MSG = "Succeed in saving %s";
    public static final String CANNOT_SAVE_ERR_MSG = "Cannot save %s due to write block.";
    public static final String RETRY_SAVING_ERR_MSG = "Retry in saving %s: ";

    protected final DataAccess dataAccess;
    protected final ThreadPool threadPool;
    protected final BackoffPolicy savingBackoffPolicy;
    protected final String defaultResultIndexName;
    protected final DataManagementType dataManagement;
    // whether save to a specific doc id or not. False by default.
    protected boolean fixedDoc;
    protected final DiscoveryNodeSelector discoveryNodeSelector;

    /**
     * Abstract class for index operation.
     *
     * @param dataAccess data access abstraction (transport or SDK)
     * @param settings accessor for node settings.
     * @param threadPool used to invoke specific threadpool to execute
     * @param indexName name of index to save to
     * @param dataManagement anomaly detection indices
     * @param discoveryNodeSelector node selector for cluster operations
     */
    public ResultIndexingHandler(
        DataAccess dataAccess,
        Settings settings,
        ThreadPool threadPool,
        String indexName,
        DataManagementType dataManagement,
        DiscoveryNodeSelector discoveryNodeSelector,
        Setting<TimeValue> backOffDelaySetting,
        Setting<Integer> maxRetrySetting
    ) {
        this.dataAccess = dataAccess;
        this.threadPool = threadPool;
        this.savingBackoffPolicy = BackoffPolicy.exponentialBackoff(backOffDelaySetting.get(settings), maxRetrySetting.get(settings));
        this.defaultResultIndexName = indexName;
        this.dataManagement = dataManagement;
        this.fixedDoc = false;
        this.discoveryNodeSelector = discoveryNodeSelector;
    }

    /**
     * Since the constructor needs to provide injected value and Guice does not allow Boolean to be there
     * (claiming it does not know how to instantiate it), caller needs to manually set it to true if
     * it want to save to a specific doc.
     * @param fixedDoc whether to save to a specific doc Id
     */
    public void setFixedDoc(boolean fixedDoc) {
        this.fixedDoc = fixedDoc;
    }

    // TODO: check if user has permission to index.
    /**
     * Run async index operation. Cannot guarantee index is done after finishing executing the function as several calls
     * in the method are asynchronous.
     * @param toSave Result to save
     * @param configId config id
     * @param indexOrAliasName custom index or alias name
     * @param tenantId the tenant id for endpoint resolution; must not be {@code null}
     *                 when indexOrAliasName is not null (custom result index)
     */
    public void index(ResultType toSave, String configId, String indexOrAliasName, String tenantId) {
        if (indexOrAliasName != null) {
            indexToCustomIndex(toSave, configId, indexOrAliasName, tenantId);
        } else {
            indexToDefaultIndex(toSave, configId);
        }
    }

    private void indexToCustomIndex(ResultType toSave, String configId, String indexOrAliasName, String tenantId) {
        discoveryNodeSelector
            .hasIndicesBlock(tenantId, ClusterBlockLevel.WRITE, new String[] { indexOrAliasName }, ActionListener.wrap(hasBlock -> {
                if (hasBlock) {
                    LOG.warn(String.format(Locale.ROOT, CANNOT_SAVE_ERR_MSG, configId));
                    return;
                }
                proceedWithCustomIndex(toSave, configId, indexOrAliasName);
            }, e -> { LOG.error(String.format(Locale.ROOT, "Failed to check indices block for %s", indexOrAliasName), e); }));
    }

    private void proceedWithCustomIndex(ResultType toSave, String configId, String indexOrAliasName) {
        String tenantId = toSave.getTenantId();
        // We create custom result index when creating a detector. Custom result index can be rolled over and thus we may need to
        // create a new one.
        dataManagement.doesResultIndexOrAliasExists(indexOrAliasName, ActionListener.wrap(exists -> {
            if (exists == false) {
                dataManagement.initCustomResultIndexDirectly(indexOrAliasName, ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        save(toSave, configId, indexOrAliasName);
                    } else {
                        LOG
                            .error(
                                String
                                    .format(
                                        Locale.ROOT,
                                        "Creating custom result index %s with mappings call not acknowledged",
                                        indexOrAliasName
                                    )
                            );
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        save(toSave, configId, indexOrAliasName);
                    } else {
                        LOG.error(String.format(Locale.ROOT, "cannot create result index %s", indexOrAliasName), exception);
                    }
                }), tenantId);
                return;
            }
            dataManagement.validateResultIndexMapping(indexOrAliasName, ActionListener.wrap(valid -> {
                if (!valid) {
                    LOG.error("wrong index mapping of custom result index");
                } else {
                    save(toSave, configId, indexOrAliasName);
                }
            }, exception -> { LOG.error(String.format(Locale.ROOT, "cannot validate result index %s", indexOrAliasName), exception); }),
                tenantId
            );
        }, exception -> { LOG.error(String.format(Locale.ROOT, "cannot check result index %s", indexOrAliasName), exception); }), tenantId);
    }

    private void indexToDefaultIndex(ResultType toSave, String configId) {
        discoveryNodeSelector
            .hasIndicesBlock(null, ClusterBlockLevel.WRITE, new String[] { this.defaultResultIndexName }, ActionListener.wrap(hasBlock -> {
                if (hasBlock) {
                    LOG.warn(String.format(Locale.ROOT, CANNOT_SAVE_ERR_MSG, configId));
                    return;
                }
                proceedWithDefaultIndex(toSave, configId);
            }, e -> { LOG.error(String.format(Locale.ROOT, "Failed to check indices block for %s", defaultResultIndexName), e); }));
    }

    private void proceedWithDefaultIndex(ResultType toSave, String configId) {
        if (!dataManagement.doesDefaultResultIndexExist()) {
            dataManagement
                .initDefaultResultIndexDirectly(
                    ActionListener.wrap(initResponse -> onCreateIndexResponse(initResponse, toSave, configId), exception -> {
                        if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                            // It is possible the index has been created while we sending the create request
                            save(toSave, configId);
                        } else {
                            LOG.error(String.format(Locale.ROOT, "Unexpected error creating index %s", defaultResultIndexName), exception);
                        }
                    })
                );
        } else {
            save(toSave, configId);
        }
    }

    private void onCreateIndexResponse(CreateIndexResponse response, ResultType toSave, String detectorId) {
        if (response.isAcknowledged()) {
            save(toSave, detectorId);
        } else {
            throw new TimeSeriesException(
                detectorId,
                String.format(Locale.ROOT, "Creating %s with mappings call not acknowledged.", defaultResultIndexName)
            );
        }
    }

    protected void save(ResultType toSave, String detectorId) {
        save(toSave, detectorId, defaultResultIndexName);
    }

    // TODO: Upgrade custom result index mapping to latest version?
    // It may bring some issue if we upgrade the custom result index mapping while user is using that index
    // for other use cases. One easy solution is to tell user only use custom result index for AD plugin.
    // For the first release of custom result index, it's not a issue. Will leave this to next phase.
    protected void save(ResultType toSave, String detectorId, String indexName) {
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(indexName).source(toSave.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            if (fixedDoc) {
                indexRequest.id(detectorId);
            }

            saveIteration(indexRequest, detectorId, savingBackoffPolicy.iterator(), toSave.getTenantId());
        } catch (Exception e) {
            LOG.error(String.format(Locale.ROOT, "Failed to save %s", indexName), e);
            throw new TimeSeriesException(detectorId, String.format(Locale.ROOT, "Cannot save %s", indexName));
        }
    }

    void saveIteration(IndexRequest indexRequest, String configId, Iterator<TimeValue> backoff, String tenantId) {
        dataAccess.index(indexRequest, TenantContext.user(tenantId), ActionListener.wrap(response -> {
            LOG.debug(String.format(Locale.ROOT, SUCCESS_SAVING_MSG, configId));
        }, exception -> {
            Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (!(cause instanceof OpenSearchRejectedExecutionException) || !backoff.hasNext()) {
                LOG.error(String.format(Locale.ROOT, FAIL_TO_SAVE_ERR_MSG, configId), cause);
            } else {
                TimeValue nextDelay = backoff.next();
                LOG.warn(String.format(Locale.ROOT, RETRY_SAVING_ERR_MSG, configId), cause);
                threadPool
                    .schedule(
                        () -> saveIteration(BulkUtil.cloneIndexRequest(indexRequest), configId, backoff, tenantId),
                        nextDelay,
                        ThreadPool.Names.SAME
                    );
            }
        }));
    }
}
