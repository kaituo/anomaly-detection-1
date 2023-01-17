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

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.timeseries.transport.TimeSeriesResultBulkRequest;

public class ForecastResultBulkRequest extends TimeSeriesResultBulkRequest<ForecastResult, ForecastResultWriteRequest> {

    public ForecastResultBulkRequest() {
        super();
    }

    public ForecastResultBulkRequest(StreamInput in) throws IOException {
        super(in, ForecastResultWriteRequest::new);
    }
}
