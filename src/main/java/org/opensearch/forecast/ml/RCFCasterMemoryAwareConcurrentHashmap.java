package org.opensearch.forecast.ml;

import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.ml.TimeSeriesMemoryAwareConcurrentHashmap;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class RCFCasterMemoryAwareConcurrentHashmap<K> extends TimeSeriesMemoryAwareConcurrentHashmap<K, RCFCaster, ForecastModelState<RCFCaster>> {

    public RCFCasterMemoryAwareConcurrentHashmap(MemoryTracker memoryTracker) {
        super(memoryTracker);
    }

    @Override
    protected long estimateModelSize(RCFCaster model) {
        return memoryTracker.estimateRCFCasterModelSize(model);
    }
}
