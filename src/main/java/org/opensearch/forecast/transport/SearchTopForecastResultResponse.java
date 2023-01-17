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
import java.util.List;

import org.opensearch.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.model.ForecastResultBucket;

/**
 * Response for getting the top anomaly results for HC detectors
 */
public class SearchTopForecastResultResponse extends ActionResponse implements ToXContentObject {
    private List<ForecastResultBucket> forecastResultBuckets;

    public SearchTopForecastResultResponse(StreamInput in) throws IOException {
        super(in);
        forecastResultBuckets = in.readList(ForecastResultBucket::new);
    }

    public SearchTopForecastResultResponse(List<ForecastResultBucket> anomalyResultBuckets) {
        this.forecastResultBuckets = anomalyResultBuckets;
    }

    public List<ForecastResultBucket> getAnomalyResultBuckets() {
        return forecastResultBuckets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(forecastResultBuckets);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(ForecastResultBucket.BUCKETS_FIELD, forecastResultBuckets).endObject();
    }
}
