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

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;

/**
 * Request should be sent from the handler logic of transport delete detector API
 *
 */
public class DeleteModelRequest extends BaseNodesRequest<DeleteModelRequest> implements ToXContentObject {
    private String configID;
    private String tenantId;

    public String getAdID() {
        return configID;
    }

    public String getTenantId() {
        return tenantId;
    }

    public DeleteModelRequest() {
        super((String[]) null);
    }

    public DeleteModelRequest(StreamInput in) throws IOException {
        super(in);
        this.configID = in.readString();
        this.tenantId = in.readOptionalString();
    }

    public DeleteModelRequest(String adID, DiscoveryNode... nodes) {
        super(nodes);
        this.configID = adID;
    }

    public DeleteModelRequest(String adID, String tenantId, DiscoveryNode... nodes) {
        super(nodes);
        this.configID = adID;
        this.tenantId = tenantId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configID);
        out.writeOptionalString(tenantId);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(configID)) {
            validationException = addValidationError(CommonMessages.CONFIG_ID_MISSING_MSG, validationException);
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonName.CONFIG_ID_KEY, configID);
        builder.endObject();
        return builder;
    }
}
