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
 * fixing missing value (denoted using Double.NaN) using a fixed set of specified values.
 * The 2nd parameter of interpolate is ignored as we infer the number of interpolants
 * using the number of Double.NaN.
 */
public class FixedValueImputer extends Imputer {
    private double[] fixedValue;

    public FixedValueImputer(double[] fixedValue) {
        this.fixedValue = fixedValue;
    }

    /**
     * Given an array of samples, fill with given value.
     * We will ignore the rest of samples beyond the 2nd element.
     *
     * @return an interpolated array of size numInterpolants
     */
    @Override
    public double[][] impute(double[][] samples, int numInterpolants) {
        int numFeatures = samples.length;
        double[][] interpolants = new double[numFeatures][numInterpolants];

        for (int featureIndex = 0; featureIndex < numFeatures; featureIndex++) {
            interpolants[featureIndex] = singleFeatureInterpolate(samples[featureIndex], numInterpolants, fixedValue[featureIndex]);
        }
        return interpolants;
    }

    private double[] singleFeatureInterpolate(double[] samples, int numInterpolants, double defaultVal) {
        return Arrays.stream(samples).map(d -> Double.isNaN(d) ? defaultVal : d).toArray();
    }

    @Override
    protected double[] singleFeatureImpute(double[] samples, int numInterpolants) {
        throw new UnsupportedOperationException("The operation is not supported");
    }
}
