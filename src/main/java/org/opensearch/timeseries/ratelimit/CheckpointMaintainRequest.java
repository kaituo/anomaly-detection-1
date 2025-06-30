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

public class CheckpointMaintainRequest extends QueuedRequest {
    private String modelId;
    private String tenantId;

    public CheckpointMaintainRequest(long expirationEpochMs, String configId, RequestPriority priority, String entityModelId, String tenantId) {
        super(expirationEpochMs, configId, priority);
        this.modelId = entityModelId;
        this.tenantId = tenantId;
    }

    public String getModelId() {
        return modelId;
    }

    public String getTenantId() {
        return tenantId;
    }
}
