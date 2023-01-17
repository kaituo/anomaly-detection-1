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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.constant.CommonName;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public abstract class TimeSeriesModelState<T> implements ExpiringState {
    public static String MODEL_TYPE_KEY = "model_type";
    public static String LAST_USED_TIME_KEY = "last_used_time";
    public static String LAST_CHECKPOINT_TIME_KEY = "last_checkpoint_time";
    public static String PRIORITY_KEY = "priority";

    protected T model;
    protected String modelId;
    protected String id;
    protected String modelType;
    // time when the ML model was used last time
    protected Instant lastUsedTime;
    protected Instant lastCheckpointTime;
    protected Clock clock;
    protected float priority;
    protected Sample lastProcessedSample;

    /**
     * Constructor.
     *
     * @param model ML model
     * @param modelId Id of model partition
     * @param id Id of analysis this model partition is used for
     * @param modelType type of model
     * @param clock UTC clock
     * @param priority Priority of the model state.  Used in multi-entity detectors' cache.
     * @param lastProcessedSample last processed sample. Used in interpolation.
     */
    public TimeSeriesModelState(T model, String modelId, String id, String modelType, Clock clock, float priority, Sample lastProcessedSample) {
        this.model = model;
        this.modelId = modelId;
        this.id = id;
        this.modelType = modelType;
        this.lastUsedTime = clock.instant();
        // this is inaccurate until we find the last checkpoint time from disk
        this.lastCheckpointTime = Instant.MIN;
        this.clock = clock;
        this.priority = priority;
        this.lastProcessedSample = lastProcessedSample;
    }

    /**
     * Returns the ML model.
     *
     * @return the ML model.
     */
    public T getModel() {
        return this.model;
    }

    public void setModel(T model) {
        this.model = model;
    }

    /**
     * Gets the model ID
     *
     * @return modelId of model
     */
    public String getModelId() {
        return modelId;
    }

    /**
     * Gets the type of the model
     *
     * @return modelType of the model
     */
    public String getModelType() {
        return modelType;
    }

    /**
     * Returns the time when the ML model was last used.
     *
     * @return the time when the ML model was last used
     */
    public Instant getLastUsedTime() {
        return this.lastUsedTime;
    }

    /**
     * Sets the time when ML model was last used.
     *
     * @param lastUsedTime time when the ML model was used last time
     */
    public void setLastUsedTime(Instant lastUsedTime) {
        this.lastUsedTime = lastUsedTime;
    }

    /**
     * Returns the time when a checkpoint for the ML model was made last time.
     *
     * @return the time when a checkpoint for the ML model was made last time.
     */
    public Instant getLastCheckpointTime() {
        return this.lastCheckpointTime;
    }

    /**
     * Sets the time when a checkpoint for the ML model was made last time.
     *
     * @param lastCheckpointTime time when a checkpoint for the ML model was made last time.
     */
    public void setLastCheckpointTime(Instant lastCheckpointTime) {
        this.lastCheckpointTime = lastCheckpointTime;
    }

    /**
     * Returns priority of the ModelState
     * @return the priority
     */
    public float getPriority() {
        return priority;
    }

    public void setPriority(float priority) {
        this.priority = priority;
    }

    public Sample getLastProcessedSample() {
        return lastProcessedSample;
    }

    public void setLastProcessedSample(Sample lastProcessedSample) {
        this.lastProcessedSample = lastProcessedSample;
    }

    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastUsedTime, stateTtl, clock.instant());
    }

    public String getId() {
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
