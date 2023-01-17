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

package org.opensearch.timeseries.ml;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IndexableResult;

public abstract class IntermediateResult {
    protected final double confidence;
    protected final long totalUpdates;
    protected final double rcfScore;

    public IntermediateResult(double confidence, long totalUpdates, double rcfScore) {
        this.confidence = confidence;
        this.totalUpdates = totalUpdates;
        this.rcfScore = rcfScore;
    }

    /**
     * Returns the confidence for the result (e.g., anomaly grade in AD).
     *
     * @return confidence for the result
     */
    public double getConfidence() {
        return confidence;
    }

    public long getTotalUpdates() {
        return totalUpdates;
    }

    public double getRcfScore() {
        return rcfScore;
    }

    @Override
    public int hashCode() {
        return Objects.hash(confidence, totalUpdates);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IntermediateResult other = (IntermediateResult) obj;
        return Double.doubleToLongBits(confidence) == Double.doubleToLongBits(other.confidence)
            && totalUpdates == other.totalUpdates
            && Double.doubleToLongBits(rcfScore) == Double.doubleToLongBits(other.rcfScore);
    }

    public abstract Optional<IndexableResult> toIndexableResult(
        Config forecaster,
        Instant dataStartInstant,
        Instant dataEndInstant,
        Instant executionStartInstant,
        Instant executionEndInstant,
        List<FeatureData> featureData,
        Optional<Entity> entity,
        Integer schemaVersion,
        String modelId,
        String taskId,
        String error
    );
}
