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

package org.opensearch.timeseries.dataprocessor;

import java.util.Arrays;

/**
 * fixing missing value (denoted using Double.NaN) using 0.
 * The 2nd parameter of interpolate is ignored as we infer the number of interpolants
 * using the number of Double.NaN.
 */
public class ZeroInterpolator extends Interpolator {

    @Override
    public double[] singleFeatureInterpolate(double[] samples, int numInterpolants) {
        return Arrays.stream(samples).map(d -> Double.isNaN(d) ? 0.0 : d).toArray();
    }
}
