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

import java.util.List;

import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public interface SingleStreamCache<RCFModelType extends ThresholdedRandomCutForest> extends Cache<RCFModelType> {
    /**
     * Get the ModelState associated with the config.  May or may not load the
     * ModelState depending on the underlying cache's memory consumption.
     *
     * @param config config accessor
     * @return the ModelState associated with the config or null if no cached item
     * for the config
     */
    ModelState<RCFModelType> get(Config config);

    /**
     * Checks if a model exists for the given config.
     * @param configId Config Id
     * @return `true` if the model exists, `false` otherwise.
     */
    boolean doesModelExist(String configId);

    /**
      *
      * @return the number of models in the cache
    */
    int getModelCount();

    /**
     * Get total updates of a model.
     *
     * @param configId config id
     * @return RCF model total updates
     */
    long getTotalUpdates(String configId);

    /**
     * Gets modelStates of all model hosted on a node
     *
     * @return list of modelStates
     */
    List<ModelState<RCFModelType>> getAllModels();

    /**
     * Remove model from active entity buffer and delete checkpoint. Used to clean corrupted model.
     * @param configId config Id
     */
    void removeModel(String configId);
}
