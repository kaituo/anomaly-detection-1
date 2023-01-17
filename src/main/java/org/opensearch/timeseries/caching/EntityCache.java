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

package org.opensearch.timeseries.caching;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.timeseries.AnalysisModelSize;
import org.opensearch.timeseries.CleanState;
import org.opensearch.timeseries.MaintenanceState;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.TimeSeriesModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.ModelProfile;

/**
 *
 *
 * @param <T> Model type like RCFCaster or TRCF
 * @param <U> Model state in a cache for both active and inactive entities
 */
public interface EntityCache <T, U extends TimeSeriesModelState<?>> extends MaintenanceState, CleanState, AnalysisModelSize {
    /**
     * Get the ModelState associated with the entity.  May or may not load the
     * ModelState depending on the underlying cache's eviction policy.
     *
     * @param modelId Model Id
     * @param config Analysis config object
     * @return the ModelState associated with the model or null if no cached item
     * for the entity
     */
    U get(String modelId, Config config);

    /**
     * Get the number of active entities of a detector
     * @param detector Detector Id
     * @return The number of active entities
     */
    int getActiveEntities(String detector);

    /**
      *
      * @return total active entities in the cache
    */
    int getTotalActiveEntities();

    /**
     * Whether an entity is active or not
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityModelId Entity model Id
     * @return Whether an entity is active or not
     */
    boolean isActive(String detectorId, String entityModelId);

    /**
     * Get total updates of detector's most active entity's RCF model.
     *
     * @param detectorId detector id
     * @return RCF model total updates of most active entity.
     */
    long getTotalUpdates(String detectorId);

    /**
     * Get RCF model total updates of specific entity
     *
     * @param detectorId detector id
     * @param entityModelId  entity model id
     * @return RCF model total updates of specific entity.
     */
    long getTotalUpdates(String detectorId, String entityModelId);

    /**
     * Gets modelStates of all model hosted on a node
     *
     * @return list of modelStates
     */
    List<U> getAllModels();

    /**
     * Return when the last active time of an entity's state.
     *
     * If the entity's state is active in the cache, the value indicates when the cache
     * is lastly accessed (get/put).  If the entity's state is inactive in the cache,
     * the value indicates when the cache state is created or when the entity is evicted
     * from active entity cache.
     *
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityModelId Entity's Model Id
     * @return if the entity is in the cache, return the timestamp in epoch
     * milliseconds when the entity's state is lastly used.  Otherwise, return -1.
     */
    long getLastActiveMs(String detectorId, String entityModelId);

    /**
     * Release memory when memory circuit breaker is open
     */
    void releaseMemoryForOpenCircuitBreaker();

    /**
     * Select candidate entities for which we can load models
     * @param cacheMissEntities Cache miss entities
     * @param id Config Id
     * @param config Config object
     * @return A list of entities that are admitted into the cache as a result of the
     *  update and the left-over entities
     */
    Pair<List<Entity>, List<Entity>> selectUpdateCandidate(
        Collection<Entity> cacheMissEntities,
        String detectorId,
        Config config
    );

    /**
     *
     * @param config Analysis config
     * @param toUpdate Model state candidate
     * @return if we can host the given model state
     */
    boolean hostIfPossible(Config config, TimeSeriesModelState<EntityModel<T>> toUpdate);

    /**
     *
     * @param detectorId Detector Id
     * @return a detector's model information
     */
    List<ModelProfile> getAllModelProfile(String detectorId);

    /**
     * Gets an entity's model sizes
     *
     * @param detectorId Detector Id
     * @param entityModelId Entity's model Id
     * @return the entity's memory size
     */
    Optional<ModelProfile> getModelProfile(String detectorId, String entityModelId);

    /**
     * Get a model state without incurring priority update. Used in maintenance.
     * @param detectorId Detector Id
     * @param modelId Model Id
     * @return Model state
     */
    Optional<U> getForMaintainance(String detectorId, String modelId);

    /**
     * Remove entity model from active entity buffer and delete checkpoint. Used to clean corrupted model.
     * @param detectorId Detector Id
     * @param entityModelId Model Id
     */
    void removeEntityModel(String detectorId, String entityModelId);
}
