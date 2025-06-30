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

package org.opensearch.timeseries.ml;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.index.store.StoreStats;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.rest.handler.store.DelegatingDataManagement;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.transport.client.Client;

@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public abstract class CheckpointDao<RCFModelType, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends DelegatingDataManagement<IndexType>, CheckpointCodecType extends CheckpointCodec<RCFModelType>>
    implements
        CheckpointDaoInterface<RCFModelType> {
    private static final Logger logger = LogManager.getLogger(CheckpointDao.class);
    public static final String TIMEOUT_LOG_MSG = "Timeout while deleting checkpoints of";
    public static final String BULK_FAILURE_LOG_MSG = "Bulk failure while deleting checkpoints of";
    public static final String SEARCH_FAILURE_LOG_MSG = "Search failure while deleting checkpoints of";
    public static final String DOC_GOT_DELETED_LOG_MSG = "checkpoints docs get deleted";
    public static final String INDEX_DELETED_LOG_MSG = "Checkpoint index has been deleted.  Has nothing to do:";
    public static final String NOT_ABLE_TO_DELETE_CHECKPOINT_MSG = "Cannot delete all checkpoints of detector";
    private static final long MAX_SHARD_SIZE_IN_BYTE = 50 * 1024 * 1024 * 1024L;
    private static final Duration MINIMUM_CHECKPOINT_TTL = Duration.ofDays(1);
    private static final String CHECKPOINT_NOT_EXIST_MSG = "Checkpoint index does not exist.";

    // dependencies
    protected final Client client;
    protected final ClientUtil clientUtil;

    // configuration
    protected final String indexName;

    protected final IndexManagementType indexUtil;
    protected final CheckpointCodecType checkpointCodec;

    public CheckpointDao(
        Client client,
        ClientUtil clientUtil,
        String indexName,
        IndexManagementType indexUtil,
        CheckpointCodecType checkpointCodec
    ) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.indexName = indexName;
        this.indexUtil = indexUtil;
        this.checkpointCodec = checkpointCodec;
    }

    protected void putModelCheckpoint(String modelId, Map<String, Object> source, ActionListener<Void> listener) {
        if (indexUtil.doesCheckpointIndexExist()) {
            saveModelCheckpointAsync(source, modelId, listener);
        } else {
            onCheckpointNotExist(source, modelId, listener);
        }
    }

    /**
     * Update the model doc using fields in source.  This ensures we won't touch
     * the old checkpoint and nodes with old/new logic can coexist in a cluster.
     * This is useful for introducing compact rcf new model format.
     *
     * @param source fields to update
     * @param modelId model Id, used as doc id in the checkpoint index
     * @param listener Listener to return response
     */
    protected void saveModelCheckpointAsync(Map<String, Object> source, String modelId, ActionListener<Void> listener) {

        UpdateRequest updateRequest = new UpdateRequest(indexName, modelId);
        updateRequest.doc(source);
        // If the document does not already exist, the contents of the upsert element are inserted as a new document.
        // If the document exists, update fields in the map
        updateRequest.docAsUpsert(true);
        clientUtil
            .<UpdateRequest, UpdateResponse>asyncRequest(
                updateRequest,
                client::update,
                ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
            );
    }

    protected void onCheckpointNotExist(Map<String, Object> source, String modelId, ActionListener<Void> listener) {
        indexUtil.initCheckpointIndex(ActionListener.wrap(initResponse -> {
            if (initResponse.isAcknowledged()) {
                saveModelCheckpointAsync(source, modelId, listener);

            } else {
                throw new RuntimeException("Creating checkpoint with mappings call not acknowledged.");
            }
        }, exception -> {
            if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                // It is possible the index has been created while we sending the create request
                saveModelCheckpointAsync(source, modelId, listener);
            } else {
                logger.error(String.format(Locale.ROOT, "Unexpected error creating index %s", indexName), exception);
            }
        }));
    }

    /**
     * Deletes the model checkpoint for the model.
     *
     * @param config config of the model
     * @param modelId id of the model
     * @param listener onReponse is called with null when the operation is completed
     */
    @Override
    public void deleteModelCheckpoint(Config config, String modelId, ActionListener<Void> listener) {
        clientUtil
            .<DeleteRequest, DeleteResponse>asyncRequest(
                new DeleteRequest(indexName, modelId),
                client::delete,
                ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
            );
    }

    protected void logFailure(BulkByScrollResponse response, String id) {
        if (response.isTimedOut()) {
            logger.warn(CheckpointDao.TIMEOUT_LOG_MSG + " {}", id);
        } else if (!response.getBulkFailures().isEmpty()) {
            logger.warn(CheckpointDao.BULK_FAILURE_LOG_MSG + " {}", id);
            for (BulkItemResponse.Failure bulkFailure : response.getBulkFailures()) {
                logger.warn(bulkFailure);
            }
        } else {
            logger.warn(CheckpointDao.SEARCH_FAILURE_LOG_MSG + " {}", id);
            for (ScrollableHitSource.SearchFailure searchFailure : response.getSearchFailures()) {
                logger.warn(searchFailure);
            }
        }
    }

    @Override
    public void batchWrite(BulkRequest request, ActionListener<BulkResponse> listener) {
        if (indexUtil.doesCheckpointIndexExist()) {
            clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
        } else {
            indexUtil.initCheckpointIndex(ActionListener.wrap(initResponse -> {
                if (initResponse.isAcknowledged()) {
                    clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
                } else {
                    // create index failure. Notify callers using listener.
                    listener.onFailure(new TimeSeriesException("Creating checkpoint with mappings call not acknowledged."));
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
                } else {
                    logger.error(String.format(Locale.ROOT, "Unexpected error creating checkpoint index"), exception);
                    listener.onFailure(exception);
                }
            }));
        }
    }

    @Override
    public void batchRead(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        clientUtil.<MultiGetRequest, MultiGetResponse>execute(MultiGetAction.INSTANCE, request, listener);
    }

    /**
     * Delete checkpoints associated with a config.  Used in multi-entity detector.
     * @param configId Config Id
     */
    @Override
    public void deleteModelCheckpointByConfigId(String tenantId, String configId) {
        // A bulk delete request is performed for each batch of matching documents. If a
        // search or bulk request is rejected, the requests are retried up to 10 times,
        // with exponential back off. If the maximum retry limit is reached, processing
        // halts and all failed requests are returned in the response. Any delete
        // requests that completed successfully still stick, they are not rolled back.
        DeleteByQueryRequest deleteRequest = createDeleteCheckpointRequest(configId);
        logger.info("Delete checkpoints of config {}", configId);
        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(response -> {
            if (response.isTimedOut() || !response.getBulkFailures().isEmpty() || !response.getSearchFailures().isEmpty()) {
                logFailure(response, configId);
            }
            // can return 0 docs get deleted because:
            // 1) we cannot find matching docs
            // 2) bad stats from OpenSearch. In this case, docs are deleted, but
            // OpenSearch says deleted is 0.
            logger.info("{} " + CheckpointDao.DOC_GOT_DELETED_LOG_MSG, response.getDeleted());
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(CheckpointDao.INDEX_DELETED_LOG_MSG + " {}", configId);
            } else {
                // Gonna eventually delete in daily cron.
                logger.error(NOT_ABLE_TO_DELETE_CHECKPOINT_MSG, exception);
            }
        }));
    }

    @Override
    public Runnable createRetentionTask(Duration checkpointTtl, Clock clock) {
        Objects.requireNonNull(checkpointTtl, "checkpointTtl must not be null");
        Objects.requireNonNull(clock, "clock must not be null");
        return () -> deleteDocsOlderThan(
            checkpointTtl,
            clock,
            ActionListener.wrap(response -> cleanupBasedOnShardSize(checkpointTtl.minusDays(1), clock), exception -> {
                logger.error("delete docs by query fails for checkpoint index", exception);
            })
        );
    }

    private void cleanupBasedOnShardSize(Duration cleanUpTtl, Clock clock) {
        if (!indexUtil.doesCheckpointIndexExist()) {
            logger.debug(CHECKPOINT_NOT_EXIST_MSG);
            return;
        }

        getCheckpointShardStoreStats(ActionListener.wrap(indicesStatsResponse -> {
            boolean cleanupNeeded = Arrays
                .stream(indicesStatsResponse.getShards())
                .map(ShardStats::getStats)
                .filter(Objects::nonNull)
                .map(CommonStats::getStore)
                .filter(Objects::nonNull)
                .map(StoreStats::getSizeInBytes)
                .anyMatch(size -> size > MAX_SHARD_SIZE_IN_BYTE);

            if (!cleanupNeeded) {
                logger.debug("clean up not needed anymore for checkpoint index");
                return;
            }

            deleteDocsOlderThan(cleanUpTtl, clock, ActionListener.wrap(response -> {
                if (cleanUpTtl.equals(MINIMUM_CHECKPOINT_TTL)) {
                    return;
                }

                Duration nextCleanupTtl = cleanUpTtl.minusDays(1);
                if (nextCleanupTtl.compareTo(MINIMUM_CHECKPOINT_TTL) < 0) {
                    nextCleanupTtl = MINIMUM_CHECKPOINT_TTL;
                }
                cleanupBasedOnShardSize(nextCleanupTtl, clock);
            }, this::logRetentionFailure));
        }, this::logRetentionFailure));
    }

    private void getCheckpointShardStoreStats(ActionListener<IndicesStatsResponse> listener) {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.store();
        indicesStatsRequest.indices(indexName);
        client.admin().indices().stats(indicesStatsRequest, listener);
    }

    private void deleteDocsOlderThan(Duration checkpointTtl, Clock clock, ActionListener<BulkByScrollResponse> listener) {
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(indexName)
            .setQuery(
                QueryBuilders
                    .boolQuery()
                    .filter(
                        QueryBuilders
                            .rangeQuery(CommonName.TIMESTAMP)
                            .lte(clock.millis() - checkpointTtl.toMillis())
                            .format(CommonName.EPOCH_MILLIS_FORMAT)
                    )
            )
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setRefresh(true);

        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(response -> {
            long deleted = response.getDeleted();
            if (deleted > 0) {
                logger.info("{} docs are deleted for index:{}", deleted, indexName);
            }
            listener.onResponse(response);
        }, listener::onFailure));
    }

    private void logRetentionFailure(Exception exception) {
        if (exception instanceof IndexNotFoundException || ExceptionUtil.isIndexNotFoundInMessage(exception)) {
            logger.debug(CHECKPOINT_NOT_EXIST_MSG);
        } else {
            logger.error("checkpoint index retention based on shard size fails", exception);
        }
    }

    public Optional<Map<String, Object>> processRawCheckpoint(GetResponse response) {
        try {
            return Optional.ofNullable(response).filter(GetResponse::isExists).map(GetResponse::getSource);
        } catch (Exception e) {
            // Assuming a logger is available
            logger.error("Error processing raw checkpoint", e);
            return Optional.empty();
        }
    }

    /**
     * Process a checkpoint GetResponse and return the EntityModel object
     * @param response Checkpoint Index GetResponse
     * @param modelId  Model Id
     * @param tenantId Tenant Id
     * @return a pair of entity model and its last checkpoint time
     */
    @Override
    public ModelState<RCFModelType> processHCGetResponse(GetResponse response, String modelId, String configId, String tenantId) {
        Optional<Map<String, Object>> checkpointString = processRawCheckpoint(response);
        if (checkpointString.isPresent()) {
            return fromEntityModelCheckpoint(checkpointString.get(), modelId, configId, tenantId);
        } else {
            return null;
        }
    }

    @Override
    public CheckpointCodec<RCFModelType> getCodec() {
        return checkpointCodec;
    }

    protected ModelState<RCFModelType> fromEntityModelCheckpoint(
        Map<String, Object> checkpoint,
        String modelId,
        String configId,
        String tenantId
    ) {
        return checkpointCodec.fromEntityModelCheckpoint(checkpoint, modelId, configId, tenantId);
    }

    protected abstract DeleteByQueryRequest createDeleteCheckpointRequest(String configId);
}
