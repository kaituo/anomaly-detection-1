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
import static org.opensearch.forecast.constant.ForecastCommonName.DUMMY_FORECASTER_ID;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.constant.CommonValue;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

/**
 * Include result returned from RCF model and feature data.
 */
public class ForecastResult extends IndexableResult {
    private static final Logger LOG = LogManager.getLogger(ForecastResult.class);
    public static final String PARSE_FIELD_NAME = "ForecastResult";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        ForecastResult.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String FORECAST_SERIES_FIELD = "forecast_series";

    private final List<ForecastData> forecastSeries;

    // used when indexing exception or error or an empty result
    public ForecastResult(
        String forecasterId,
        String taskId,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        Optional<Entity> entity,
        User user,
        Integer schemaVersion,
        String modelId
    ) {
        this(
            forecasterId,
            taskId,
            Double.NaN,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entity,
            user,
            schemaVersion,
            modelId,
            null
        );
    }

    public ForecastResult(
        String forecasterId,
        String taskId,
        Double confidence,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        Optional<Entity> entity,
        User user,
        Integer schemaVersion,
        String modelId,
        List<ForecastData> forecastSeries
    ) {
        super(
            forecasterId,
            confidence,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entity,
            user,
            schemaVersion,
            modelId,
            taskId
        );
        this.forecastSeries = forecastSeries;
    }

    public static ForecastResult fromRawRCFCasterResult(
        String forecasterId,
        long intervalMillis,
        Double confidence,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        Optional<Entity> entity,
        User user,
        Integer schemaVersion,
        String modelId,
        float[] forecastsValues,
        float[] forecastsUppers,
        float[] forecastsLowers,
        String taskId
    ) {
        int inputLength = featureData.size();
        int numberOfForecasts = forecastsValues.length / inputLength;
        List<ForecastData> convertedForecastValues = new ArrayList<>(numberOfForecasts);
        Instant forecastDataStartTime = dataEndTime;

        for (int i = 0; i < numberOfForecasts; i++) {
            Instant forecastDataEndTime = forecastDataStartTime.plusMillis(intervalMillis);
            for (int j = 0; j < inputLength; j++) {
                int k = i * inputLength + j;
                convertedForecastValues
                    .add(
                        new ForecastData(
                            featureData.get(j).getFeatureId(),
                            forecastsValues[k],
                            forecastsLowers[k],
                            forecastsUppers[k],
                            forecastDataStartTime,
                            forecastDataEndTime
                        )
                    );
            }
            forecastDataStartTime = forecastDataEndTime;
        }

        return new ForecastResult(
            forecasterId,
            taskId,
            confidence,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entity,
            user,
            schemaVersion,
            modelId,
            convertedForecastValues
        );
    }

    public ForecastResult(StreamInput input) throws IOException {
        super(input);

        int seriesLength = input.readVInt();
        if (seriesLength <= 0) {
            this.forecastSeries = null;
        } else {
            this.forecastSeries = new ArrayList<>(seriesLength);
            for (int i = 0; i < seriesLength; i++) {
                forecastSeries.add(new ForecastData(input));
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(ForecastCommonName.FORECASTER_ID_KEY, configId)
            .field(CommonName.SCHEMA_VERSION_FIELD, schemaVersion);

        if (dataStartTime != null) {
            xContentBuilder.field(CommonName.DATA_START_TIME_FIELD, dataStartTime.toEpochMilli());
        }
        if (dataEndTime != null) {
            xContentBuilder.field(CommonName.DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        }
        if (featureData != null) {
            // can be null during preview
            xContentBuilder.field(CommonName.FEATURE_DATA_FIELD, featureData.toArray());
        }
        if (executionStartTime != null) {
            // can be null during preview
            xContentBuilder.field(CommonName.EXECUTION_START_TIME_FIELD, executionStartTime.toEpochMilli());
        }
        if (executionEndTime != null) {
            // can be null during preview
            xContentBuilder.field(CommonName.EXECUTION_END_TIME_FIELD, executionEndTime.toEpochMilli());
        }
        if (error != null) {
            xContentBuilder.field(CommonName.ERROR_FIELD, error);
        }
        if (optionalEntity.isPresent()) {
            xContentBuilder.field(CommonName.ENTITY_FIELD, optionalEntity.get());
        }
        if (user != null) {
            xContentBuilder.field(CommonName.USER_FIELD, user);
        }
        if (modelId != null) {
            xContentBuilder.field(CommonName.MODEL_ID_FIELD, modelId);
        }
        if (confidence != null && !confidence.isNaN()) {
            xContentBuilder.field(CommonName.CONFIDENCE_FIELD, confidence);
        }
        if (forecastSeries != null) {
            xContentBuilder.array(FORECAST_SERIES_FIELD, forecastSeries.toArray());
        }
        if (taskId != null) {
            xContentBuilder.field(CommonName.TASK_ID_FIELD, taskId);
        }
        return xContentBuilder.endObject();
    }

    public static ForecastResult parse(XContentParser parser) throws IOException {
        String forecasterId = null;
        Double confidence = null;
        List<FeatureData> featureData = new ArrayList<>();
        Instant dataStartTime = null;
        Instant dataEndTime = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        String error = null;
        Entity entity = null;
        User user = null;
        Integer schemaVersion = CommonValue.NO_SCHEMA_VERSION;
        String modelId = null;
        List<ForecastData> forecastSeries = new ArrayList<>();
        String taskId = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case ForecastCommonName.FORECASTER_ID_KEY:
                    forecasterId = parser.text();
                    break;
                case CommonName.CONFIDENCE_FIELD:
                    confidence = parser.doubleValue();
                    break;
                case CommonName.FEATURE_DATA_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        featureData.add(FeatureData.parse(parser));
                    }
                    break;
                case CommonName.DATA_START_TIME_FIELD:
                    dataStartTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.DATA_END_TIME_FIELD:
                    dataEndTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.EXECUTION_START_TIME_FIELD:
                    executionStartTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.EXECUTION_END_TIME_FIELD:
                    executionEndTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.ERROR_FIELD:
                    error = parser.text();
                    break;
                case CommonName.ENTITY_FIELD:
                    entity = Entity.parse(parser);
                    break;
                case CommonName.USER_FIELD:
                    user = User.parse(parser);
                    break;
                case CommonName.SCHEMA_VERSION_FIELD:
                    schemaVersion = parser.intValue();
                    break;
                case CommonName.MODEL_ID_FIELD:
                    modelId = parser.text();
                    break;
                case FORECAST_SERIES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        forecastSeries.add(ForecastData.parse(parser));
                    }
                    break;
                case CommonName.TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new ForecastResult(
            forecasterId,
            taskId,
            confidence,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            Optional.ofNullable(entity),
            user,
            schemaVersion,
            modelId,
            forecastSeries
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        ForecastResult that = (ForecastResult) o;
        return Objects.equal(forecastSeries, that.forecastSeries);
    }

    @Generated
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hashCode(forecastSeries);
        return result;
    }

    @Generated
    @Override
    public String toString() {
        return super.toString()
            + ", "
            + new ToStringBuilder(this).append("forecastSeries", StringUtils.join(forecastSeries, "|")).toString();
    }

    public List<ForecastData> getForecastSeries() {
        return forecastSeries;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (forecastSeries != null) {
            out.writeVInt(forecastSeries.size());
            for (ForecastData attribution : forecastSeries) {
                attribution.writeTo(out);
            }
        } else {
            out.writeVInt(0);
        }
    }

    public static ForecastResult getDummyResult() {
        return new ForecastResult(
            DUMMY_FORECASTER_ID,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            CommonValue.NO_SCHEMA_VERSION,
            null
        );
    }

    /**
     * Used to throw away requests when index pressure is high.
     * @return  when the error is there.
     */
    @Override
    public boolean isHighPriority() {
        // AnomalyResult.toXContent won't record Double.NaN and thus make it null
        return getError() != null;
    }
}
