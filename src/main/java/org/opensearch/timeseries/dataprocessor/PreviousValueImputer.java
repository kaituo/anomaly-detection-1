/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

/**
 * Given an array of samples, fill missing values (represented using Double.NaN)
 * with previous value.
 * The return array may be smaller than the input array as we remove leading missing
 * values after interpolation. If the first sample is Double.NaN
 * as there is no last known value to fill in.
 * The 2nd parameter of interpolate is ignored as we infer the number of imputed values
 * using the number of Double.NaN.
 *
 */
public class PreviousValueImputer extends Imputer {

    @Override
    protected double[] singleFeatureImpute(double[] samples, int numImputants) {
        int numSamples = samples.length;
        double[] imputants = new double[numSamples];

        if (numSamples > 0) {
            System.arraycopy(samples, 0, imputants, 0, samples.length);
            if (numSamples > 1) {
                double lastKnownValue = Double.NaN;
                for (int interpolantIndex = 0; interpolantIndex < numSamples; interpolantIndex++) {
                    if (Double.isNaN(imputants[interpolantIndex])) {
                        if (!Double.isNaN(lastKnownValue)) {
                            imputants[interpolantIndex] = lastKnownValue;
                        }
                    } else {
                        lastKnownValue = imputants[interpolantIndex];
                    }
                }
            }
        }
        return imputants;
    }
}
