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

/*
 * An object for interpolating feature vectors.
 *
 * In certain situations, due to time and compute cost, we are only allowed to
 * query a sparse sample of data points / feature vectors from a cluster.
 * However, we need a large sample of feature vectors in order to train our
 * anomaly detection algorithms. An Interpolator approximates the data points
 * between a given, ordered list of samples.
 */
public abstract class Interpolator {

    /**
     * Interpolates the given sample feature vectors.
     *
     * Computes a list `numInterpolants` feature vectors using the ordered list
     * of `numSamples` input sample vectors where each sample vector has size
     * `numFeatures`.
     *
     *
     * @param samples          A `numFeatures x numSamples` list of feature vectors.
     * @param numInterpolants  The desired number of interpolating vectors.
     * @return                 A `numFeatures x numInterpolants` list of feature vectors.
     */
    public double[][] interpolate(double[][] samples, int numInterpolants) {
        int numFeatures = samples.length;
        double[][] interpolants = new double[numFeatures][numInterpolants];

        for (int featureIndex = 0; featureIndex < numFeatures; featureIndex++) {
            interpolants[featureIndex] = singleFeatureInterpolate(samples[featureIndex], numInterpolants);
        }
        return interpolants;
    }


    /**
     *
     * @param arr input array
     * @return the omitted double array of array with no leading Double.NaN.
     *   The return array may be smaller than the input array as we remove leading missing
     *   values after interpolation.
     */
    public double[][] ltrim(double[][] arr) {
        int numRows = arr.length;
        if (numRows == 0) {
            return new double[0][0];
        }

        int numCols = arr[0].length;
        int startIndex = -1;
        for (int i = 0; i < numRows; i++) {
            int j = 0;
            for (; j < numCols; j++) {
                if (Double.isNaN(arr[i][j])) {
                    break;
                }
            }
            if (j == numCols) {
                startIndex = i;
            }
        }

        return Arrays.copyOfRange(arr, startIndex, arr.length);
    }

    /**
     * compute per-feature interpolants
     * @param samples input array
     * @param numInterpolants number of elements in the return array
     * @return input array with missing values interpoalted
     */
    protected abstract double[] singleFeatureInterpolate(double[] samples, int numInterpolants);
}
