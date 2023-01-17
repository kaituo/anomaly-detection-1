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

package org.opensearch.forecast.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.model.DataByFeatureId;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

public class ForecastData implements ToXContentObject, Writeable {
    public static final String FEATURE_ID_FIELD = "feature_id";
    public static final String DATA_FIELD = "data";
    public static final String LOWER_BOUND_FIELD = "lower_bound";
    public static final String UPPER_BOUND_FIELD = "upper_bound";
    public static final String DATA_START_TIME_FIELD = "data_start_time";
    public static final String DATA_END_TIME_FIELD = "data_end_time";

    private final String featureId;
    private final Float data;
    private final Float lowerBound;
    private final Float upperBound;
    private final Instant dataStartTime;
    private final Instant dataEndTime;

    public ForecastData(String featureId, Float data, Float lowerBound, Float upperBound, Instant dataStartTime, Instant dataEndTime) {
        this.featureId = featureId;
        this.data = data;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
    }

    public ForecastData(StreamInput input) throws IOException {
        this.featureId = input.readString();
        this.data = input.readFloat();
        this.lowerBound = input.readFloat();
        this.upperBound = input.readFloat();
        this.dataStartTime = input.readInstant();
        this.dataEndTime = input.readInstant();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(FEATURE_ID_FIELD, featureId)
            .field(DATA_FIELD, data)
            .field(LOWER_BOUND_FIELD, lowerBound)
            .field(UPPER_BOUND_FIELD, upperBound)
            .field(DATA_START_TIME_FIELD, dataStartTime.toEpochMilli())
            .field(DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        return xContentBuilder.endObject();
    }

    public static ForecastData parse(XContentParser parser) throws IOException {
        String featureId = null;
        Float data = null;
        Float parsedLowerBound = null;
        Float parsedUpperBound = null;
        Instant parsedDataStartTime = null;
        Instant parsedDataEndTime = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case FEATURE_ID_FIELD:
                    featureId = parser.text();
                    break;
                case LOWER_BOUND_FIELD:
                    parsedLowerBound = parser.floatValue();
                    break;
                case UPPER_BOUND_FIELD:
                    parsedUpperBound = parser.floatValue();
                    break;
                case DATA_FIELD:
                    data = parser.floatValue();
                    break;
                case DATA_START_TIME_FIELD:
                    parsedDataStartTime = ParseUtils.toInstant(parser);
                    break;
                case DATA_END_TIME_FIELD:
                    parsedDataEndTime = ParseUtils.toInstant(parser);
                    break;
                default:
                    break;
            }
        }
        return new ForecastData(featureId, data, parsedLowerBound, parsedUpperBound, parsedDataStartTime, parsedDataEndTime);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

            ForecastData that = (ForecastData) o;
            return Objects.equal(featureId, that.featureId)
                    && Objects.equal(data, that.data)
                    && Objects.equal(lowerBound, that.lowerBound)
                    && Objects.equal(upperBound, that.upperBound)
                    && Objects.equal(dataStartTime, that.dataStartTime)
                    && Objects.equal(dataEndTime, that.dataEndTime);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(featureId, data, lowerBound, upperBound, dataStartTime, dataEndTime);
    }

    public String getFeatureId() {
        return featureId;
    }

    public Float getData() {
        return data;
    }

    public Float getLowerBound() {
        return lowerBound;
    }

    public Float getUpperBound() {
        return upperBound;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureId);
        out.writeFloat(data);
        out.writeFloat(lowerBound);
        out.writeFloat(upperBound);
        out.writeInstant(dataStartTime);
        out.writeInstant(dataEndTime);
    }
}
