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

package org.opensearch.timeseries.stats.suppliers;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.forecast.caching.ForecastCacheProvider;

/**
 * ModelsOnNodeCountSupplier provides the number of models a node contains
 */
public class ModelsOnNodeCountSupplier implements Supplier<Long> {
    private ADModelManager modelManager;
    private ADCacheProvider adCache;
    private ForecastCacheProvider forecastCache;

    /**
     * Constructor
     *
     * @param modelManager object that manages the model partitions hosted on the node
     * @param cache object that manages multi-entity detectors' models
     */
    public ModelsOnNodeCountSupplier(ADModelManager modelManager, ADCacheProvider adCache, ForecastCacheProvider forecastCache) {
        this.modelManager = modelManager;
        this.adCache = adCache;
        this.forecastCache = forecastCache;
    }

    @Override
    public Long get() {
        return Stream
            .concat(
                Stream.concat(modelManager.getAllModels().stream(), adCache.get().getAllModels().stream()),
                forecastCache.get().getAllModels().stream()
            )
            .count();
    }
}
