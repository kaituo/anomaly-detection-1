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

package org.opensearch.timeseries.model;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.ToXContentObject;

public abstract class AnalysisResult implements Writeable, ToXContentObject {

    protected final String id;
    protected final Double confidence;
    protected final List<FeatureData> featureData;
    protected final Instant dataStartTime;
    protected final Instant dataEndTime;
    protected final Instant executionStartTime;
    protected final Instant executionEndTime;
    protected final String error;
    protected final Entity entity;
    protected User user;
    protected final Integer schemaVersion;
    /*
     * model id for easy aggregations of entities. The front end needs to query
     * for entities ordered by the descending/ascending order of feature values.
     * After supporting multi-category fields, it is hard to write such queries
     * since the entity information is stored in a nested object array.
     * Also, the front end has all code/queries/ helper functions in place to
     * rely on a single key per entity combo. Adding model id to forecast result
     * to help the transition to multi-categorical field less painful.
     */
    protected final String modelId;


    public AnalysisResult(
            String id,
            Double confidence,
            List<FeatureData> featureData,
            Instant dataStartTime,
            Instant dataEndTime,
            Instant executionStartTime,
            Instant executionEndTime,
            String error,
            Entity entity,
            User user,
            Integer schemaVersion,
            String modelId) {
        this.id = id;
        this.confidence = confidence;
        this.featureData = featureData;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
        this.executionStartTime = executionStartTime;
        this.executionEndTime = executionEndTime;
        this.error = error;
        this.entity = entity;
        this.user = user;
        this.schemaVersion = schemaVersion;
        this.modelId = modelId;
    }

    public AnalysisResult(StreamInput input) throws IOException {
        this.id = input.readString();
        this.confidence = input.readDouble();
        int featureSize = input.readVInt();
        this.featureData = new ArrayList<>(featureSize);
        for (int i = 0; i < featureSize; i++) {
            featureData.add(new FeatureData(input));
        }
        this.dataStartTime = input.readInstant();
        this.dataEndTime = input.readInstant();
        this.executionStartTime = input.readInstant();
        this.executionEndTime = input.readInstant();
        this.error = input.readOptionalString();
        if (input.readBoolean()) {
            this.entity = new Entity(input);
        } else {
            this.entity = null;
        }
        if (input.readBoolean()) {
            this.user = new User(input);
        } else {
            user = null;
        }
        this.schemaVersion = input.readInt();
        this.modelId = input.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeDouble(confidence);
        out.writeVInt(featureData.size());
        for (FeatureData feature : featureData) {
            feature.writeTo(out);
        }
        out.writeInstant(dataStartTime);
        out.writeInstant(dataEndTime);
        out.writeInstant(executionStartTime);
        out.writeInstant(executionEndTime);
        out.writeOptionalString(error);
        if (entity != null) {
            out.writeBoolean(true);
            entity.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (user != null) {
            out.writeBoolean(true); // user exists
            user.writeTo(out);
        } else {
            out.writeBoolean(false); // user does not exist
        }
        out.writeInt(schemaVersion);
        out.writeOptionalString(modelId);
    }

    public String getId() {
        return id;
    }

    public List<FeatureData> getFeatureData() {
        return featureData;
    }

    public Double getConfidence() {
        return confidence;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    public Instant getExecutionStartTime() {
        return executionStartTime;
    }

    public Instant getExecutionEndTime() {
        return executionEndTime;
    }

    public String getError() {
        return error;
    }

    public Entity getEntity() {
        return entity;
    }

    public String getModelId() {
        return modelId;
    }

    /**
     * Used to throw away requests when index pressure is high.
     * @return  whether the result is high priority.
     */
    public abstract boolean isHighPriority();
}
