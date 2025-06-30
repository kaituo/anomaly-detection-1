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

package org.opensearch.timeseries.ratelimit;

import java.util.Optional;

import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;

public class FeatureRequest extends QueuedRequest {
    private final double[] currentFeature;
    private final long dataStartTimeMillis;
    protected final String modelId;
    private final Optional<Entity> entity;
    private final String taskId;
    private final Optional<Config> config;
    private final long requestTimeMillis;

    // used in HC
    public FeatureRequest(
        long expirationEpochMs,
        String configId,
        RequestPriority priority,
        double[] currentFeature,
        long dataStartTimeMs,
        Entity entity,
        String taskId,
        String tenantId,
        String dataSourceId,
        long requestTimeMillis
    ) {
        super(expirationEpochMs, configId, priority, tenantId, dataSourceId);
        this.currentFeature = currentFeature;
        this.dataStartTimeMillis = dataStartTimeMs;
        this.modelId = entity.getModelId(tenantId, configId).isEmpty() ? null : entity.getModelId(tenantId, configId).get();
        this.entity = Optional.ofNullable(entity);
        this.taskId = taskId;
        this.config = Optional.empty();
        this.requestTimeMillis = requestTimeMillis;
    }

    // used in single-stream
    public FeatureRequest(
        long expirationEpochMs,
        String configId,
        RequestPriority priority,
        String modelId,
        double[] currentFeature,
        long dataStartTimeMs,
        String taskId,
        String tenantId,
        Config config,
        long requestTimeMillis
    ) {
        super(expirationEpochMs, configId, priority, tenantId, config == null ? null : config.getDataSourceId());
        this.currentFeature = currentFeature;
        this.dataStartTimeMillis = dataStartTimeMs;
        this.modelId = modelId;
        this.entity = Optional.empty();
        this.taskId = taskId;
        this.config = Optional.ofNullable(config);
        this.requestTimeMillis = requestTimeMillis;
    }

    public double[] getCurrentFeature() {
        return currentFeature;
    }

    public long getDataStartTimeMillis() {
        return dataStartTimeMillis;
    }

    public String getModelId() {
        return modelId;
    }

    public Optional<Entity> getEntity() {
        return entity;
    }

    public String getTaskId() {
        return taskId;
    }

    public Optional<Config> getConfig() {
        return config;
    }

    public long getRequestTimeMillis() {
        return requestTimeMillis;
    }

    public boolean isRunOnce() {
        return taskId != null;
    }
}
