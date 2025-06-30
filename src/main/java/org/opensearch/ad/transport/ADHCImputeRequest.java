/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ADHCImputeRequest extends BaseNodesRequest<ADHCImputeRequest> {
    private final String configId;
    private final String tenantId;
    private final String taskId;
    private final long dataStartMillis;
    private final long dataEndMillis;
    private final String targetNodeId;

    public ADHCImputeRequest(String configId, String tenantId, String taskId, long startMillis, long endMillis, DiscoveryNode... nodes) {
        this(configId, tenantId, taskId, startMillis, endMillis, null, nodes);
    }

    public ADHCImputeRequest(
        String configId,
        String tenantId,
        String taskId,
        long startMillis,
        long endMillis,
        String targetNodeId,
        DiscoveryNode... nodes
    ) {
        super(nodes);
        this.configId = configId;
        this.tenantId = tenantId;
        this.taskId = taskId;
        this.dataStartMillis = startMillis;
        this.dataEndMillis = endMillis;
        this.targetNodeId = targetNodeId;
    }

    public ADHCImputeRequest(StreamInput in) throws IOException {
        super(in);
        this.configId = in.readString();
        this.tenantId = in.readOptionalString();
        this.taskId = in.readOptionalString();
        this.dataStartMillis = in.readLong();
        this.dataEndMillis = in.readLong();
        this.targetNodeId = in.getVersion().onOrAfter(Version.V_3_3_0) ? in.readOptionalString() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configId);
        out.writeOptionalString(tenantId);
        out.writeOptionalString(taskId);
        out.writeLong(dataStartMillis);
        out.writeLong(dataEndMillis);
        if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
            out.writeOptionalString(targetNodeId);
        }
    }

    public String getConfigId() {
        return configId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getTaskId() {
        return taskId;
    }

    public long getDataStartMillis() {
        return dataStartMillis;
    }

    public long getDataEndMillis() {
        return dataEndMillis;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }
}
