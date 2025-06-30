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

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.model.Config;

/**
 * Interface for checkpoint data access operations.
 * Provides methods for saving, loading, and managing ML model checkpoints.
 *
 * @param <RCFModelType> The type of RCF model
 */
public interface CheckpointDaoInterface<RCFModelType> {

    /**
     * Deletes the model checkpoint for the model.
     *
     * @param config config of the model
     * @param modelId id of the model
     * @param listener onResponse is called with null when the operation is completed
     */
    void deleteModelCheckpoint(Config config, String modelId, ActionListener<Void> listener);

    /**
     * Determines whether to save the checkpoint based on various conditions.
     *
     * @param modelState The current state of the model, which includes the last checkpoint time.
     * @param forceWrite Indicates if the checkpoint should be saved regardless of other conditions.
     * @param checkpointInterval The interval at which checkpoints should be saved.
     * @param clock The clock used to determine the current time (usually in UTC).
     *
     * @return true if both of the following conditions are met:
     *         1. The model state is valid (the model is non-null or it has non-empty samples), and
     *         2. Either forceWrite is true, or the last checkpoint time is not the minimum instant and the current time exceeds the last checkpoint time by at least the checkpoint interval.
     *         Returns false otherwise.
     */
    default boolean shouldSave(ModelState<RCFModelType> modelState, boolean forceWrite, Duration checkpointInterval, Clock clock) {
        if (modelState == null) {
            return false;
        }

        Instant lastCheckpointTime = modelState.getLastCheckpointTime();
        boolean isTimeForCheckpoint = lastCheckpointTime != null
            && !lastCheckpointTime.equals(Instant.MIN)
            && lastCheckpointTime.plus(checkpointInterval).isBefore(clock.instant());
        boolean hasValidSamples = modelState.getSamples() != null && !modelState.getSamples().isEmpty();
        boolean isModelStateValid = modelState.getModel().isPresent() || hasValidSamples;
        return isModelStateValid && (isTimeForCheckpoint || forceWrite);
    }

    /**
     * Performs a batch write operation.
     *
     * @param request The bulk request containing multiple checkpoint operations
     * @param listener Listener for the bulk response
     */
    void batchWrite(BulkRequest request, ActionListener<BulkResponse> listener);

    /**
     * Performs a batch read operation for multiple checkpoints.
     *
     * @param request The multi-get request for multiple checkpoints
     * @param listener Listener for the multi-get response
     */
    void batchRead(MultiGetRequest request, ActionListener<MultiGetResponse> listener);

    /**
     * Delete checkpoints associated with a config. Used in multi-entity detector.
     *
     * @param tenantId Tenant Id
     * @param configId Config Id
     */
    void deleteModelCheckpointByConfigId(String tenantId, String configId);

    /**
     * Creates the retention task responsible for deleting expired checkpoints for this store.
     *
     * <p>
     * Implementations own their retention strategy so callers only need the checkpoint interface
     * when scheduling cleanup jobs.
     *
     * @param checkpointTtl retention TTL
     * @param clock clock used for cutoff calculations
     * @return runnable retention task for this store implementation
     */
    default Runnable createRetentionTask(Duration checkpointTtl, Clock clock) {
        throw new UnsupportedOperationException("Checkpoint retention is not implemented for " + getClass().getName());
    }

    /**
     * Resolve the store-specific checkpoint identifier used by batched read/write paths.
     *
     * <p>
     * This method exists so the existing OpenSearch request interface can be reused without larger
     * code changes, while still passing the backend-specific information needed to locate
     * checkpoints. The returned value is a compromise adapter between the current request wiring
     * and the actual checkpoint store. For a single-tenant implementation, this is typically the
     * checkpoint index name. For a multi-tenant S3 implementation, it can be an S3 prefix. For a
     * different multi-tenant backend, it might be an application id or another store-specific
     * identifier.
     *
     * @param tenantId tenant id
     * @param configId config id
     * @param modelId model id
     * @param defaultIndexName fallback name used by the existing checkpoint worker wiring
     * @return store-specific checkpoint identifier
     */
    default String resolveCheckpointIndexName(String tenantId, String configId, String modelId, String defaultIndexName) {
        return defaultIndexName;
    }

    default String resolveCheckpointReference(String checkpointIdentifier, String modelId) {
        return null;
    }

    CheckpointCodec<RCFModelType> getCodec();

    default Map<String, Object> toIndexSource(ModelState<RCFModelType> modelState) throws IOException {
        return getCodec().toIndexSource(modelState);
    }

    default Optional<Map<String, Object>> processRawCheckpoint(GetResponse response) {
        try {
            return Optional.ofNullable(response).filter(GetResponse::isExists).map(GetResponse::getSource);
        } catch (Exception e) {
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
    default ModelState<RCFModelType> processHCGetResponse(GetResponse response, String modelId, String configId, String tenantId) {
        Optional<Map<String, Object>> checkpointString = processRawCheckpoint(response);
        if (checkpointString.isPresent()) {
            return getCodec().fromEntityModelCheckpoint(checkpointString.get(), modelId, configId, tenantId);
        } else {
            return null;
        }
    }
}
