/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.opensearch.timeseries.annotation.Generated;

import com.google.common.base.Objects;

public class ForecastResultBucket implements ToXContentObject, Writeable {
    public static final String BUCKETS_FIELD = "buckets";
    public static final String KEY_FIELD = "key";
    public static final String DOC_COUNT_FIELD = "doc_count";

    private final Map<String, Object> key;
    private final int docCount;
    private final Map<String, Double> aggregations;

    public ForecastResultBucket(Map<String, Object> key, int docCount, Map<String, Double> aggregations) {
        this.key = key;
        this.docCount = docCount;
        this.aggregations = aggregations;
    }

    public ForecastResultBucket(StreamInput input) throws IOException {
        this.key = input.readMap();
        this.docCount = input.readInt();
        this.aggregations = input.readMap(StreamInput::readString, StreamInput::readDouble);
    }

    public static ForecastResultBucket createForecastResultBucket(Bucket bucket) {
        Map<String, Double> aggregationsMap = new HashMap<>();
        for (Aggregation aggregation : bucket.getAggregations()) {
            if (!(aggregation instanceof NumericMetricsAggregation.SingleValue)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "A single value aggregation is required; received [{}]", aggregation));
            }
            NumericMetricsAggregation.SingleValue singleValueAggregation = (NumericMetricsAggregation.SingleValue)aggregation;
            aggregationsMap.put(aggregation.getName(), singleValueAggregation.value());
        }
        return new ForecastResultBucket(
            bucket.getKey(),
            (int) bucket.getDocCount(),
            aggregationsMap
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(KEY_FIELD, key)
            .field(DOC_COUNT_FIELD, docCount);

        for (Map.Entry<String, Double> entry : aggregations.entrySet()) {
            xContentBuilder.field(entry.getKey(), entry.getValue());
        }
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(key);
        out.writeInt(docCount);
        out.writeMap(aggregations, StreamOutput::writeString, StreamOutput::writeDouble);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ForecastResultBucket that = (ForecastResultBucket) o;
        return Objects.equal(getKey(), that.getKey())
            && Objects.equal(getDocCount(), that.getDocCount())
            && Objects.equal(aggregations, that.getAggregations());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(getKey(), getDocCount(), getAggregations());
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("key", key)
            .append("docCount", docCount)
            .append("aggregations", aggregations)
            .toString();
    }

    public Map<String, Object> getKey() {
        return key;
    }

    public int getDocCount() {
        return docCount;
    }

    public Map<String, Double> getAggregations() {
        return aggregations;
    }
}
