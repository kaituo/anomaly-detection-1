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
import java.util.Locale;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;

public class SingleStreamResultRequest extends ActionRequest implements ToXContentObject {
    private final String configId;
    private final String modelId;
    private final String configJson;

    // data start/end time epoch in milliseconds
    private final long startMillis;
    private final long endMillis;
    private final double[] datapoint;
    private final String taskId;
    private final String tenantId;

    public SingleStreamResultRequest(
        String configId,
        String modelId,
        long start,
        long end,
        double[] datapoint,
        String taskId,
        String tenantId
    ) {
        this(configId, modelId, start, end, datapoint, taskId, tenantId, null);
    }

    public SingleStreamResultRequest(
        String configId,
        String modelId,
        long start,
        long end,
        double[] datapoint,
        String taskId,
        String tenantId,
        String configJson
    ) {
        super();
        this.configId = configId;
        this.modelId = modelId;
        this.startMillis = start;
        this.endMillis = end;
        this.datapoint = datapoint;
        this.taskId = taskId;
        this.tenantId = tenantId;
        this.configJson = configJson;
    }

    public SingleStreamResultRequest(StreamInput in) throws IOException {
        super(in);
        this.configId = in.readString();
        this.modelId = in.readString();
        this.startMillis = in.readLong();
        this.endMillis = in.readLong();
        this.datapoint = in.readDoubleArray();
        this.taskId = in.readOptionalString();
        if (in.available() > 0) {
            this.tenantId = in.readOptionalString();
        } else {
            this.tenantId = null;
        }
        if (in.available() > 0) {
            this.configJson = in.readOptionalString();
        } else {
            this.configJson = null;
        }
    }

    public String getConfigId() {
        return this.configId;
    }

    public String getModelId() {
        return modelId;
    }

    public String getConfigJson() {
        return configJson;
    }

    public long getStart() {
        return this.startMillis;
    }

    public long getEnd() {
        return this.endMillis;
    }

    public double[] getDataPoint() {
        return this.datapoint;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTenantId() {
        return tenantId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.configId);
        out.writeString(this.modelId);
        out.writeLong(this.startMillis);
        out.writeLong(this.endMillis);
        out.writeDoubleArray(datapoint);
        out.writeOptionalString(this.taskId);
        out.writeOptionalString(this.tenantId);
        out.writeOptionalString(this.configJson);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonName.CONFIG_ID_KEY, configId);
        builder.field(CommonName.MODEL_ID_KEY, modelId);
        builder.field(CommonName.START_JSON_KEY, startMillis);
        builder.field(CommonName.END_JSON_KEY, endMillis);
        builder.array(CommonName.VALUE_LIST_FIELD, datapoint);
        builder.field(CommonName.RUN_ONCE_FIELD, taskId);
        if (tenantId != null) {
            builder.field(CommonName.TENANT_ID_FIELD, tenantId);
        }
        if (configJson != null) {
            builder.field(CommonName.CONFIG_JSON_FIELD, configJson);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(configId)) {
            validationException = addValidationError(CommonMessages.CONFIG_ID_MISSING_MSG, validationException);
        }
        if (Strings.isEmpty(modelId)) {
            validationException = addValidationError(CommonMessages.MODEL_ID_MISSING_MSG, validationException);
        }
        if (startMillis <= 0 || endMillis <= 0 || startMillis > endMillis) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", CommonMessages.INVALID_TIMESTAMP_ERR_MSG, startMillis, endMillis),
                validationException
            );
        }
        return validationException;
    }
}
