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

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.util.ParseUtils;

public class Sample implements ToXContentObject {
    private final double[] data;
    private final Instant dataStartTime;
    private final Instant dataEndTime;

    public Sample(double[] data, Instant dataStartTime, Instant dataEndTime) {
        super();
        this.data = data;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
    }

    // Invalid sample
    public Sample() {
        this.data = new double[0];
        this.dataStartTime = this.dataEndTime = Instant.MIN;
    }

    public double[] getValueList() {
        return data;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
                .startObject();
        if (data != null) {
            xContentBuilder.array(CommonName.VALUE_LIST_FIELD, data);
        }
        if (dataStartTime != null) {
            xContentBuilder.field(CommonName.DATA_START_TIME_FIELD, dataStartTime.toEpochMilli());
        }
        if (dataEndTime != null) {
            xContentBuilder.field(CommonName.DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        }
        return xContentBuilder.endObject();
    }

    public static Sample parse(XContentParser parser) throws IOException {
        Instant dataStartTime = null;
        Instant dataEndTime = null;
        List<Double> valueList = new ArrayList<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case CommonName.DATA_START_TIME_FIELD:
                    dataStartTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.DATA_END_TIME_FIELD:
                    dataEndTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.VALUE_LIST_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        valueList.add(parser.doubleValue());
                    }
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new Sample(valueList.stream().mapToDouble(Double::doubleValue).toArray(), dataStartTime, dataEndTime);
    }

    public boolean isInvalid() {
        return dataStartTime.compareTo(Instant.MIN) == 0 || dataEndTime.compareTo(Instant.MIN) == 0;
    }
}
