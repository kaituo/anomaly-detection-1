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
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class JobResponse extends ActionResponse implements ToXContentObject {
    private final String id;

    public JobResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
    }

    public JobResponse(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(RestHandlerUtils._ID, id).endObject();
    }

    public static JobResponse parse(XContentParser parser) throws IOException {
        String id = null;

        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            if (RestHandlerUtils._ID.equals(fieldName)) {
                id = parser.text();
            }
        }

        return new JobResponse(id);
    }
}
