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

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;

public class ForecastSingleStreamResultRequest extends ActionRequest implements ToXContentObject {

    protected String forecasterId;
    // data start/end time epoch in milliseconds
    protected long startMillis;
    protected long endMillis;

    public ForecastSingleStreamResultRequest(StreamInput in) throws IOException {
        super(in);
        this.forecasterId = in.readString();
        this.startMillis = in.readLong();
        this.endMillis = in.readLong();
    }

    public ForecastSingleStreamResultRequest(String forecasterId, long startMillis, long endMillis) {
        super();
        this.forecasterId = forecasterId;
        this.startMillis = startMillis;
        this.endMillis = endMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ForecastCommonName.ID_JSON_KEY, forecasterId);
        builder.field(CommonName.START_JSON_KEY, startMillis);
        builder.field(CommonName.END_JSON_KEY, endMillis);
        builder.endObject();
        return builder;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(forecasterId)) {
            validationException = addValidationError(ForecastCommonMessages.FORECAST_ID_MISSING_MSG, validationException);
        }
        if (startMillis <= 0 || endMillis <= 0 || startMillis > endMillis) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", CommonMessages.INVALID_TIMESTAMP_ERR_MSG, startMillis, endMillis),
                validationException
            );
        }
        return validationException;
    }

}
