/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.model.EntityTaskProfile;
import org.opensearch.timeseries.model.TimeSeriesTask;

public abstract class TaskProfile<TaskType extends TimeSeriesTask> implements ToXContentObject, Writeable {

    public static final String SHINGLE_SIZE_FIELD = "shingle_size";
    public static final String RCF_TOTAL_UPDATES_FIELD = "rcf_total_updates";
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    public static final String NODE_ID_FIELD = "node_id";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String TASK_TYPE_FIELD = "task_type";
    public static final String ENTITY_TASK_PROFILE_FIELD = "entity_task_profiles";

    protected TaskType task;
    protected Integer shingleSize;
    protected Long rcfTotalUpdates;
    protected Long modelSizeInBytes;
    protected String nodeId;
    protected String taskId;
    protected String taskType;
    protected List<EntityTaskProfile> entityTaskProfiles;

    public TaskProfile() {

    }

    public TaskProfile(TaskType task) {
        this.task = task;
    }

    public TaskProfile(String taskId, int shingleSize, long rcfTotalUpdates, long modelSizeInBytes, String nodeId) {
        this.taskId = taskId;
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.modelSizeInBytes = modelSizeInBytes;
        this.nodeId = nodeId;
    }

    public TaskProfile(
        TaskType adTask,
        Integer shingleSize,
        Long rcfTotalUpdates,
        Long modelSizeInBytes,
        String nodeId,
        String taskId,
        String adTaskType
    ) {
        this.task = adTask;
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.modelSizeInBytes = modelSizeInBytes;
        this.nodeId = nodeId;
        this.taskId = taskId;
        this.taskType = adTaskType;
    }

    public TaskType getTask() {
        return task;
    }

    public void setTask(TaskType adTask) {
        this.task = adTask;
    }

    public Integer getShingleSize() {
        return shingleSize;
    }

    public void setShingleSize(Integer shingleSize) {
        this.shingleSize = shingleSize;
    }

    public Long getRcfTotalUpdates() {
        return rcfTotalUpdates;
    }

    public void setRcfTotalUpdates(Long rcfTotalUpdates) {
        this.rcfTotalUpdates = rcfTotalUpdates;
    }

    public Long getModelSizeInBytes() {
        return modelSizeInBytes;
    }

    public void setModelSizeInBytes(Long modelSizeInBytes) {
        this.modelSizeInBytes = modelSizeInBytes;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public List<EntityTaskProfile> getEntityTaskProfiles() {
        return entityTaskProfiles;
    }

    public void setEntityTaskProfiles(List<EntityTaskProfile> entityTaskProfiles) {
        this.entityTaskProfiles = entityTaskProfiles;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TaskProfile<TaskType> that = (TaskProfile<TaskType>) o;
        return Objects.equals(task, that.task)
            && Objects.equals(shingleSize, that.shingleSize)
            && Objects.equals(rcfTotalUpdates, that.rcfTotalUpdates)
            && Objects.equals(modelSizeInBytes, that.modelSizeInBytes)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(taskId, that.taskId)
            && Objects.equals(taskType, that.taskType)
            && Objects.equals(entityTaskProfiles, that.entityTaskProfiles);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hash(task, shingleSize, rcfTotalUpdates, modelSizeInBytes, nodeId, taskId, taskType, entityTaskProfiles);
    }

    protected void toXContent(XContentBuilder xContentBuilder) throws IOException {
        if (task != null) {
            xContentBuilder.field(getTaskFieldName(), task);
        }
        if (shingleSize != null) {
            xContentBuilder.field(SHINGLE_SIZE_FIELD, shingleSize);
        }
        if (rcfTotalUpdates != null) {
            xContentBuilder.field(RCF_TOTAL_UPDATES_FIELD, rcfTotalUpdates);
        }
        if (modelSizeInBytes != null) {
            xContentBuilder.field(MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        if (nodeId != null) {
            xContentBuilder.field(NODE_ID_FIELD, nodeId);
        }
        if (taskId != null) {
            xContentBuilder.field(TASK_ID_FIELD, taskId);
        }
        if (taskType != null) {
            xContentBuilder.field(TASK_TYPE_FIELD, taskType);
        }
        if (entityTaskProfiles != null && entityTaskProfiles.size() > 0) {
            xContentBuilder.field(ENTITY_TASK_PROFILE_FIELD, entityTaskProfiles.toArray());
        }
    }

    protected abstract String getTaskFieldName();
}
