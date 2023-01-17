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

package org.opensearch.ad.ml;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.EntityModel;
import org.opensearch.timeseries.ml.Sample;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import org.opensearch.timeseries.ml.TimeSeriesModelState;

/**
 * ML model and states such as usage.
 */
public class ADModelState<T> extends TimeSeriesModelState<T> {
    /**
     * Constructor.
     *
     * @param model ML model
     * @param modelId Id of model partition
     * @param detectorId Id of detector this model partition is used for
     * @param modelType type of model
     * @param clock UTC clock
     * @param priority Priority of the model state.  Used in multi-entity detectors' cache.
     */
    public ADModelState(T model, String modelId, String detectorId, String modelType, Clock clock, float priority) {
        super(model, modelId, detectorId, modelType, clock, priority, new Sample());
    }

    /**
     * Create state with zero priority. Used in single-stream detector.
     *
     * @param <T> Model object's type
     * @param model The actual model object
     * @param modelId Model Id
     * @param detectorId Detector Id
     * @param modelType Model type like RCF model
     * @param clock UTC clock
     *
     * @return the created model state
     */
    public static <T> ADModelState<T> createSingleStreamModelState(
        T model,
        String modelId,
        String detectorId,
        String modelType,
        Clock clock
    ) {
        return new ADModelState<>(model, modelId, detectorId, modelType, clock, 0f);
    }

    /**
     * Gets the detector ID of the model
     *
     * @return the id associated with the model
     */
    public String getDetectorId() {
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
                put(ADCommonName.DETECTOR_ID_KEY, id);
                put(MODEL_TYPE_KEY, modelType);
                /* A stats API broadcasts requests to all nodes and renders node responses using toXContent.
                 *
                 * For the local node, the stats API's calls toXContent on the node response directly.
                 * For remote node, the coordinating node gets a serialized content from
                 * ADStatsNodeResponse.writeTo, deserializes the content, and renders the result using toXContent.
                 * Since ADStatsNodeResponse.writeTo uses StreamOutput::writeGenericValue, we can only use
                 *  a long instead of the Instant object itself as
                 *  StreamOutput::writeGenericValue only recognizes built-in types.*/
                put(LAST_USED_TIME_KEY, lastUsedTime.toEpochMilli());
                if (lastCheckpointTime != Instant.MIN) {
                    put(LAST_CHECKPOINT_TIME_KEY, lastCheckpointTime.toEpochMilli());
                }
                if (model != null && model instanceof EntityModel) {
                    EntityModel<ThresholdedRandomCutForest> summary = (EntityModel<ThresholdedRandomCutForest>) model;
                    if (summary.getEntity().isPresent()) {
                        put(CommonName.ENTITY_KEY, summary.getEntity().get().toStat());
                    }
                }
            }
        };
    }
}
