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

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class ADBatchAnomalyResultResponse extends ActionResponse implements ToXContentObject {
    public static final String NODE_ID_FIELD = "node_id";
    public static final String RUN_TASK_REMOTELY_FIELD = "run_task_remotely";

    public String nodeId;
    public boolean runTaskRemotely;

    public ADBatchAnomalyResultResponse(String nodeId, boolean runTaskRemotely) {
        this.nodeId = nodeId;
        this.runTaskRemotely = runTaskRemotely;
    }

    public ADBatchAnomalyResultResponse(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readString();
        runTaskRemotely = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeBoolean(runTaskRemotely);
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isRunTaskRemotely() {
        return runTaskRemotely;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(NODE_ID_FIELD, nodeId).field(RUN_TASK_REMOTELY_FIELD, runTaskRemotely).endObject();
    }

    public static ADBatchAnomalyResultResponse parse(XContentParser parser) throws IOException {
        String nodeId = null;
        boolean runTaskRemotely = false;

        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case NODE_ID_FIELD:
                    nodeId = parser.text();
                    break;
                case RUN_TASK_REMOTELY_FIELD:
                    runTaskRemotely = parser.booleanValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new ADBatchAnomalyResultResponse(nodeId, runTaskRemotely);
    }

}
