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

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ml.TimeSeriesModelState;

import com.amazon.randomcutforest.parkservices.RCFCaster;

/**
 * A ML model and states such as usage.
 */
public class ForecastModelState<T> extends TimeSeriesModelState<T> {
    /**
     * Constructor.
     *
     * @param model ML model
     * @param modelId Id of model partition
     * @param forecasterId Id of forecaster this model partition is used for
     * @param modelType type of model
     * @param clock UTC clock
     * @param priority Priority of the model state.  Used in multi-entity detectors' cache.
     */
    public ForecastModelState(T model, String modelId, String forecasterId, String modelType, Clock clock, float priority, Sample lastProcessedSample) {
        super(model, modelId, forecasterId, modelType, clock, priority, lastProcessedSample);
    }

    /**
     * Create state with zero priority. Used in single-stream forecaster.
     *
     * @param <T> Model object's type
     * @param model The actual model object
     * @param modelId Model Id
     * @param forecasterId Forecaster Id
     * @param modelType Model type like RCF model
     * @param clock UTC clock
     *
     * @return the created model state
     */
    public static <T> ForecastModelState<T> createSingleStreamModelState(
        T model,
        String modelId,
        String forecasterId,
        String modelType,
        Clock clock,
        Sample lastProcessedSample
    ) {
        return new ForecastModelState<>(model, modelId, forecasterId, modelType, clock, 0f, lastProcessedSample);
    }

    /**
     * Gets the forecaster id of the model
     *
     * @return the id associated with the model
     */
    public String getForecasterId() {
        return id;
    }

    /**
     * Gets the Model State as a map
     *
     * @return Map of ModelStates
     */
    public Map<String, Object> getModelStateAsMap() {
        return new HashMap<String, Object>() {
            {
                put(CommonName.MODEL_ID_FIELD, modelId);
                put(ForecastCommonName.FORECASTER_ID_KEY, id);
                put(MODEL_TYPE_KEY, modelType);
                /* A stats API broadcasts requests to all nodes and renders node responses using toXContent.
                 *
                 * For the local node, the stats API's calls toXContent on the node response directly.
                 * For remote node, the coordinating node gets a serialized content from
                 * ForecastStatsNodeResponse.writeTo, deserializes the content, and renders the result using toXContent.
                 * Since ForecastStatsNodeResponse.writeTo uses StreamOutput::writeGenericValue, we can only use
                 *  a long instead of the Instant object itself as
                 *  StreamOutput::writeGenericValue only recognizes built-in types.
                 *
                 *  TODO: ForecastStatsNodeResponse does not exist yet
                 *  */
                put(LAST_USED_TIME_KEY, lastUsedTime.toEpochMilli());
                if (lastCheckpointTime != Instant.MIN) {
                    put(LAST_CHECKPOINT_TIME_KEY, lastCheckpointTime.toEpochMilli());
                }
                if (model != null && model instanceof EntityModel) {
                    EntityModel<RCFCaster> summary = (EntityModel<RCFCaster>) model;
                    if (summary.getEntity().isPresent()) {
                        put(CommonName.ENTITY_KEY, summary.getEntity().get().toStat());
                    }
                }
            }
        };
    }
}

