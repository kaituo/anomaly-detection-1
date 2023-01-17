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

import org.opensearch.common.inject.Provider;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A wrapper to call concrete implementation of caching.  Used in transport
 * action.  Don't use interface because transport action handler constructor
 * requires a concrete class as input.
 *
 */
public class HCCacheProvider<RCFModelType extends ThresholdedRandomCutForest> implements Provider<EntityCache<RCFModelType>> {
    private EntityCache<RCFModelType> cache;

    public HCCacheProvider() {

    }

    @Override
    public EntityCache<RCFModelType> get() {
        return cache;
    }

    public void set(EntityCache<RCFModelType> cache) {
        this.cache = cache;
    }
}
