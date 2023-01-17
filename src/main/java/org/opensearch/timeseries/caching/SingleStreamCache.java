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

import org.opensearch.timeseries.ml.TimeSeriesModelState;


public interface SingleStreamCache<RCFModelType, ModelState extends TimeSeriesModelState<RCFModelType>> extends Cache<RCFModelType, ModelState> {
    /**
     * Get the ModelState associated with the config.  May or may not load the
     * ModelState depending on the underlying cache's memory consumption.
     *
     * @param modelId model id
     * @return the ModelState associated with the config or null if no cached item
     * for the config
     */
    ModelState get(String modelId);

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
     * @param modelId model id
     * @return RCF model total updates
     */
    long getTotalUpdates(String modelId);

    /**
     * Gets modelStates of all model hosted on a node
     *
     * @return list of modelStates
     */
    List<ModelState> getAllModels();

    /**
     *
     * @param modelId model id
     * @param toUpdate Model state candidate
     * @return if we can host the given model state
     */
    boolean hostIfPossible(String modelId, ModelState toUpdate);
}
