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

package org.opensearch.forecast.transport;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.commons.authuser.User;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.transport.TimeSeriesResultResponse;


public class ForecastResultResponse extends TimeSeriesResultResponse {
    public static final String DATA_QUALITY_JSON_KEY = "dataQuality";
    public static final String ERROR_JSON_KEY = "error";
    public static final String FEATURES_JSON_KEY = "features";
    public static final String FEATURE_VALUE_JSON_KEY = "value";
    public static final String RCF_TOTAL_UPDATES_JSON_KEY = "rcfTotalUpdates";
    public static final String FORECASTER_INTERVAL_IN_MINUTES_JSON_KEY = "forecasterIntervalInMinutes";
    public static final String FORECAST_VALUES_JSON_KEY = "forecastValues";
    public static final String FORECAST_UPPERS_JSON_KEY = "forecastUppers";
    public static final String FORECAST_LOWERS_JSON_KEY = "forecastLowers";

    private Double dataQuality;
    private float[] forecastsValues;
    private float[] forecastsUppers;
    private float[] forecastsLowers;

    // used when returning an error/exception or empty result
    public ForecastResultResponse(
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long forecasterIntervalInMinutes,
        Boolean isHCForecaster
    ) {
        this(
            Double.NaN,
            features,
            error,
            rcfTotalUpdates,
            forecasterIntervalInMinutes,
            isHCForecaster,
            null,
            null,
            null
        );
    }

    public ForecastResultResponse(
        Double confidence,
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long forecasterIntervalInMinutes,
        Boolean isHCForecaster,
        float[] forecastsValues,
        float[] forecastsUppers,
        float[] forecastsLowers
    ) {
        super(features, error, rcfTotalUpdates, forecasterIntervalInMinutes, isHCForecaster);
        this.dataQuality = confidence;
        this.forecastsValues = forecastsValues;
        this.forecastsUppers = forecastsUppers;
        this.forecastsLowers = forecastsLowers;
    }

    public ForecastResultResponse(StreamInput in) throws IOException {
        super(in);
        dataQuality = in.readDouble();
        int size = in.readVInt();
        features = new ArrayList<FeatureData>();
        for (int i = 0; i < size; i++) {
            features.add(new FeatureData(in));
        }
        error = in.readOptionalString();
        rcfTotalUpdates = in.readOptionalLong();
        configIntervalInMinutes = in.readOptionalLong();
        isHC = in.readOptionalBoolean();

        if (in.readBoolean()) {
            forecastsValues = in.readFloatArray();
            forecastsUppers = in.readFloatArray();
            forecastsLowers = in.readFloatArray();
        } else {
            forecastsValues = null;
            forecastsUppers = null;
            forecastsLowers = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(dataQuality);
        out.writeVInt(features.size());
        for (FeatureData feature : features) {
            feature.writeTo(out);
        }
        out.writeOptionalString(error);
        out.writeOptionalLong(rcfTotalUpdates);
        out.writeOptionalLong(configIntervalInMinutes);
        out.writeOptionalBoolean(isHC);

        if (forecastsValues != null) {
            if (forecastsUppers == null || forecastsLowers == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "null value: forecastsUppers: %s, forecastsLowers: %s", forecastsUppers, forecastsLowers));
            }
            out.writeBoolean(true);
            out.writeFloatArray(forecastsValues);
            out.writeFloatArray(forecastsUppers);
            out.writeFloatArray(forecastsLowers);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DATA_QUALITY_JSON_KEY, dataQuality);
        builder.field(ERROR_JSON_KEY, error);
        builder.startArray(FEATURES_JSON_KEY);
        for (FeatureData feature : features) {
            feature.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(RCF_TOTAL_UPDATES_JSON_KEY, rcfTotalUpdates);
        builder.field(FORECASTER_INTERVAL_IN_MINUTES_JSON_KEY, configIntervalInMinutes);
        builder.field(FORECAST_VALUES_JSON_KEY, forecastsValues);
        builder.field(FORECAST_UPPERS_JSON_KEY, forecastsUppers);
        builder.field(FORECAST_LOWERS_JSON_KEY, forecastsLowers);
        builder.endObject();
        return builder;
    }

    /**
    *
    * Convert ForecastResultResponse to ForecastResult
    *
    * @param forecastId Forecaster Id
    * @param dataStartInstant data start time
    * @param dataEndInstant data end time
    * @param executionStartInstant  execution start time
    * @param executionEndInstant execution end time
    * @param schemaVersion Schema version
    * @param user Detector author
    * @param error Error
    * @return converted ForecastResult
    */
    public ForecastResult toForecastResult(
        String forecastId,
        Instant dataStartInstant,
        Instant dataEndInstant,
        Instant executionStartInstant,
        Instant executionEndInstant,
        Integer schemaVersion,
        User user,
        String error
    ) {
        // Forecast interval in milliseconds
        long forecasterIntervalMilli = Duration.between(dataStartInstant, dataEndInstant).toMillis();
        return ForecastResult
            .fromRawRCFCasterResult(
                forecastId,
                forecasterIntervalMilli,
                dataQuality,
                features,
                dataStartInstant,
                dataEndInstant,
                executionStartInstant,
                executionEndInstant,
                error,
                null,
                user,
                schemaVersion,
                null, // single-stream real-time has no model id
                forecastsValues,
                forecastsUppers,
                forecastsLowers
            );
    }
}
