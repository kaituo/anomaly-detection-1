package org.opensearch.timeseries.caching;

import org.opensearch.common.inject.Provider;
import org.opensearch.timeseries.ml.TimeSeriesModelState;

/**
 * A wrapper to call concrete implementation of caching.  Used in transport
 * action.  Don't use interface because transport action handler constructor
 * requires a concrete class as input.
 *
 */
public class SingleStreamCacheProvider<RCFModelType, ModelState extends TimeSeriesModelState<RCFModelType>> implements Provider<SingleStreamCache<RCFModelType, ModelState>> {
    private SingleStreamCache<RCFModelType, ModelState> cache;

    public SingleStreamCacheProvider() {

    }

    @Override
    public SingleStreamCache<RCFModelType, ModelState> get() {
        return cache;
    }

    public void set(SingleStreamCache<RCFModelType, ModelState> cache) {
        this.cache = cache;
    }
}
