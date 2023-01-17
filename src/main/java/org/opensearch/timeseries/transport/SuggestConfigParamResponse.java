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

package org.opensearch.timeseries.transport;

import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;

public class SuggestConfigParamResponse extends ActionResponse implements ToXContentObject {
    public static final String INTERVAL_FIELD = "interval";

    private final IntervalTimeConfiguration interval;

    public IntervalTimeConfiguration getInterval() {
        return interval;
    }

    public SuggestConfigParamResponse(IntervalTimeConfiguration interval) {
        this.interval = interval;
    }

    public SuggestConfigParamResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            this.interval = IntervalTimeConfiguration.readFrom(in);
        } else {
            this.interval = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (interval != null) {
            out.writeBoolean(true);
            interval.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(INTERVAL_FIELD, interval);

        return xContentBuilder.endObject();
    }
}
