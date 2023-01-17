package org.opensearch.timeseries.caching;

import org.opensearch.common.inject.Provider;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A wrapper to call concrete implementation of caching.  Used in transport
 * action.  Don't use interface because transport action handler constructor
 * requires a concrete class as input.
 *
 */
public class SingleStreamCacheProvider<RCFModelType extends ThresholdedRandomCutForest>
    implements
        Provider<SingleStreamCache<RCFModelType>> {
    private SingleStreamCache<RCFModelType> cache;

    public SingleStreamCacheProvider() {

    }

    @Override
    public SingleStreamCache<RCFModelType> get() {
        return cache;
    }

    public void set(SingleStreamCache<RCFModelType> cache) {
        this.cache = cache;
    }
}
