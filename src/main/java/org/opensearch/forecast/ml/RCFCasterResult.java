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

package org.opensearch.forecast.ml;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.opensearch.forecast.model.ForecastData;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IndexableResult;

import com.amazon.randomcutforest.returntypes.RangeVector;

public class RCFCasterResult extends IntermediateResult {
    private final RangeVector forecast;

    public RCFCasterResult(RangeVector forecast, double confidence, long totalUpdates, double rcfScore) {
        super(confidence, totalUpdates, rcfScore);
        this.forecast = forecast;
    }

    public RangeVector getForecast() {
        return forecast;
    }

    @Override
    public Optional<IndexableResult> toIndexableResult(
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
    ) {
        if (forecast.values == null || forecast.values.length == 0) {
            return Optional.empty();
        }
        List<ForecastData> forecastSeries = new ArrayList<>();
        List<String> featureIds = forecaster.getEnabledFeatureIds();
        int inputLength = featureIds.size();
        for (int i = 0; i < forecast.values.length / inputLength; i++) {
            for (int j = 0; j < inputLength; j++) {
                FeatureData ithFeatureData = featureData.get(j);
                int k = i * inputLength + j;
                forecastSeries
                    .add(
                        new ForecastData(
                            ithFeatureData.getFeatureId(),
                            forecast.values[k],
                            forecast.lower[k],
                            forecast.upper[k],
                            dataStartInstant,
                            dataEndInstant
                        )
                    );
            }
        }

        return Optional
            .of(
                new ForecastResult(
                    forecaster.getId(),
                    taskId,
                    confidence,
                    featureData,
                    dataStartInstant,
                    dataEndInstant,
                    executionStartInstant,
                    executionEndInstant,
                    error,
                    entity,
                    forecaster.getUser(),
                    schemaVersion,
                    modelId,
                    forecastSeries
                )
            );
    }
}
