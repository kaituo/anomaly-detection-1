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

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class IndexAnomalyDetectorResponse extends ActionResponse implements ToXContentObject {
    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private final AnomalyDetector detector;
    private final RestStatus restStatus;

    public IndexAnomalyDetectorResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        version = in.readLong();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        detector = new AnomalyDetector(in);
        restStatus = in.readEnum(RestStatus.class);
    }

    public IndexAnomalyDetectorResponse(
        String id,
        long version,
        long seqNo,
        long primaryTerm,
        AnomalyDetector detector,
        RestStatus restStatus
    ) {
        this.id = id;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.detector = detector;
        this.restStatus = restStatus;
    }

    public String getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        detector.writeTo(out);
        out.writeEnum(restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(RestHandlerUtils._ID, id)
            .field(RestHandlerUtils._VERSION, version)
            .field(RestHandlerUtils._SEQ_NO, seqNo)
            .field(RestHandlerUtils.ANOMALY_DETECTOR, detector)
            .field(RestHandlerUtils._PRIMARY_TERM, primaryTerm)
            .endObject();
    }

    public static IndexAnomalyDetectorResponse parse(XContentParser parser) throws IOException {
        String id = null;
        long version = 0;
        long seqNo = 0;
        long primaryTerm = 0;
        AnomalyDetector detector = null;

        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if (RestHandlerUtils._ID.equals(fieldName)) {
                id = parser.text();
            } else if (RestHandlerUtils._VERSION.equals(fieldName)) {
                version = parser.longValue();
            } else if (RestHandlerUtils._SEQ_NO.equals(fieldName)) {
                seqNo = parser.longValue();
            } else if (RestHandlerUtils._PRIMARY_TERM.equals(fieldName)) {
                primaryTerm = parser.longValue();
            } else if (RestHandlerUtils.ANOMALY_DETECTOR.equals(fieldName)) {
                detector = AnomalyDetector.parse(parser);
            }
        }

        return new IndexAnomalyDetectorResponse(id, version, seqNo, primaryTerm, detector, RestStatus.OK);
    }
}
