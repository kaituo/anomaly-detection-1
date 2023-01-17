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

import org.opensearch.forecast.model.ForecastData;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;

import com.amazon.randomcutforest.returntypes.RangeVector;

public class RCFCasterResult extends IntermediateResult {
    private final RangeVector forecast;

    public RCFCasterResult(
            RangeVector forecast,
            double confidence,
            long totalUpdates,
            double rcfScore
            ) {
        super(confidence, totalUpdates, rcfScore);
        this.forecast = forecast;
    }

    public RangeVector getForecast() {
        return forecast;
    }

    @Override
    public IndexableResult toIndexableResult(
        Config forecaster,
        Instant dataStartInstant,
        Instant dataEndInstant,
        Instant executionStartInstant,
        Instant executionEndInstant,
        List<FeatureData> featureData,
        Entity entity,
        Integer schemaVersion,
        String modelId,
        String taskId,
        String error
    ) {
        List<ForecastData> forecastSeries = new ArrayList<>();
        List<String> featureIds = forecaster.getEnabledFeatureIds();
        int inputLength = featureIds.size();
        for (int i = 0; i < forecast.values.length /inputLength ; i++) {
            for (int j = 0; j < inputLength; j++) {
                FeatureData ithFeatureData = featureData.get(i);
                int k = i * inputLength + j;
                forecastSeries.add(new ForecastData(ithFeatureData.getFeatureId(), ithFeatureData.getData().floatValue(), forecast.lower[k], forecast.upper[k], dataStartInstant, dataEndInstant));
            }
        }

        return new ForecastResult(
                forecaster.getId(),
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
                );
    }
}
