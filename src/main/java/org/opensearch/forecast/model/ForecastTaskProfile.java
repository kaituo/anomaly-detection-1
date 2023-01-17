/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.TaskProfile;
import org.opensearch.timeseries.model.EntityTaskProfile;

public class ForecastTaskProfile extends TaskProfile<ForecastTask> {

    public ForecastTaskProfile(
        ForecastTask forecastTask,
        Integer shingleSize,
        Long rcfTotalUpdates,
        Long modelSizeInBytes,
        String nodeId,
        String taskId,
        String taskType
    ) {
        super(forecastTask, shingleSize, rcfTotalUpdates, modelSizeInBytes, nodeId, taskId, taskType);
    }

    public ForecastTaskProfile(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            this.task = new ForecastTask(input);
        } else {
            this.task = null;
        }
        this.shingleSize = input.readOptionalInt();
        this.rcfTotalUpdates = input.readOptionalLong();
        this.modelSizeInBytes = input.readOptionalLong();
        this.nodeId = input.readOptionalString();
        this.taskId = input.readOptionalString();
        this.taskType = input.readOptionalString();
        if (input.readBoolean()) {
            this.entityTaskProfiles = input.readList(EntityTaskProfile::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (task != null) {
            out.writeBoolean(true);
            task.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalInt(shingleSize);
        out.writeOptionalLong(rcfTotalUpdates);
        out.writeOptionalLong(modelSizeInBytes);
        out.writeOptionalString(nodeId);
        out.writeOptionalString(taskId);
        out.writeOptionalString(taskType);
        if (entityTaskProfiles != null && entityTaskProfiles.size() > 0) {
            out.writeBoolean(true);
            out.writeList(entityTaskProfiles);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        super.toXContent(xContentBuilder);
        return xContentBuilder.endObject();
    }

    public static ForecastTaskProfile parse(XContentParser parser) throws IOException {
        ForecastTask forecastTask = null;
        Integer shingleSize = null;
        Long rcfTotalUpdates = null;
        Long modelSizeInBytes = null;
        String nodeId = null;
        String taskId = null;
        String taskType = null;
        List<EntityTaskProfile> entityTaskProfiles = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case ForecastCommonName.FORECAST_TASK:
                    forecastTask = ForecastTask.parse(parser);
                    break;
                case SHINGLE_SIZE_FIELD:
                    shingleSize = parser.intValue();
                    break;
                case RCF_TOTAL_UPDATES_FIELD:
                    rcfTotalUpdates = parser.longValue();
                    break;
                case MODEL_SIZE_IN_BYTES:
                    modelSizeInBytes = parser.longValue();
                    break;
                case NODE_ID_FIELD:
                    nodeId = parser.text();
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case TASK_TYPE_FIELD:
                    taskType = parser.text();
                    break;
                case ENTITY_TASK_PROFILE_FIELD:
                    entityTaskProfiles = new ArrayList<>();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        entityTaskProfiles.add(EntityTaskProfile.parse(parser));
                    }
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new ForecastTaskProfile(forecastTask, shingleSize, rcfTotalUpdates, modelSizeInBytes, nodeId, taskId, taskType);
    }

    @Override
    protected String getTaskFieldName() {
        return ForecastCommonName.FORECAST_TASK;
    }
}
