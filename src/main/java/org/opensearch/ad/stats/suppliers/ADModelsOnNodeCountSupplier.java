/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.stats.suppliers;

import java.util.Objects;

import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.timeseries.stats.suppliers.TenantAwareStatSupplier;

/**
 * ModelsOnNodeCountSupplier provides the number of models a node contains
 */
public class ADModelsOnNodeCountSupplier implements TenantAwareStatSupplier<Long> {
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
    public Long getForTenant(String tenantId) {
        return adCache
            .get()
            .getAllModels()
            .stream()
            .filter(modelState -> tenantId == null || Objects.equals(tenantId, modelState.getTenantId()))
            .count();
    }
}
