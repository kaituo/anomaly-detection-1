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

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.Locale;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.transport.TimeSeriesSingleStreamResultRequest;

public class SingleStreamForecastResultRequest extends TimeSeriesSingleStreamResultRequest {
    public SingleStreamForecastResultRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SingleStreamForecastResultRequest(String forecasterId, long start, long end) {
        super(forecasterId, start, end);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(configId)) {
            validationException = addValidationError(ForecastCommonMessages.FORECAST_ID_MISSING_MSG, validationException);
        }
        if (start <= 0 || end <= 0 || start > end) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", CommonMessages.INVALID_TIMESTAMP_ERR_MSG, start, end),
                validationException
            );
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ForecastCommonName.ID_JSON_KEY, configId);
        builder.field(CommonName.START_JSON_KEY, start);
        builder.field(CommonName.END_JSON_KEY, end);
        builder.endObject();
        return builder;
    }
}

