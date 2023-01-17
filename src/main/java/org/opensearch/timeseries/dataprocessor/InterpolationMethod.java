package org.opensearch.timeseries.dataprocessor;

public enum InterpolationMethod {
    /**
     * use all 0's
     */
    ZERO,
    /**
     * use a fixed set of specified values (same as input dimension)
     */
    FIXED_VALUES,
    /**
     * last known value in each input dimension
     */
    PREVIOUS,
    /**
     * linear interpolation
     */
    LINEAR
}
