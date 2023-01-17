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

import org.opensearch.action.ActionRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;


public abstract class TimeSeriesSingleStreamResultRequest extends ActionRequest implements ToXContentObject {
    protected String configId;

    // data start/end time epoch in milliseconds
    protected long start;
    protected long end;

    public TimeSeriesSingleStreamResultRequest(String configId, long start, long end) {
        super();
        this.configId = configId;
        this.start = start;
        this.end = end;
    }

    protected TimeSeriesSingleStreamResultRequest(StreamInput in) throws IOException {
        super(in);
        this.configId = in.readString();
        this.start = in.readLong();
        this.end = in.readLong();
    }

    public String getConfigId() {
        return this.configId;
    }

    public long getStart() {
        return this.start;
    }

    public long getEnd() {
        return this.end;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.configId);
        out.writeLong(this.start);
        out.writeLong(this.end);
    }
}
