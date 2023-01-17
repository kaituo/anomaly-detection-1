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

package org.opensearch.timeseries.ratelimit;


public class FeatureRequest extends QueuedRequest {
    private final double[] currentFeature;
    private final long dataStartTimeMillis;
    private final String modelId;

    public FeatureRequest(
        long expirationEpochMs,
        String detectorId,
        RequestPriority priority,
        double[] currentFeature,
        long dataStartTimeMs,
        String modelId
    ) {
        super(expirationEpochMs, detectorId, priority);
        this.currentFeature = currentFeature;
        this.dataStartTimeMillis = dataStartTimeMs;
        this.modelId = modelId;
    }

    public double[] getCurrentFeature() {
        return currentFeature;
    }

    public long getDataStartTimeMillis() {
        return dataStartTimeMillis;
    }

    public String getModelId() {
        return modelId;
    }
}
