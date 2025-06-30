/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.stats.suppliers;

import java.util.function.Supplier;

import org.opensearch.ad.caching.ADCacheProvider;

/**
 * ModelsOnNodeCountSupplier provides the number of models a node contains
 */
public class ADModelsOnNodeCountSupplier implements Supplier<Long> {
    private ADCacheProvider adCache;

    /**
     * Constructor
     *
     * @param adCache object that manages hosted realtime detector models
     */
    public ADModelsOnNodeCountSupplier(ADCacheProvider adCache) {
        this.adCache = adCache;
    }

    @Override
    public Long get() {
        return (long) adCache.get().getAllModels().size();
    }
}
