/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.stats.suppliers;

/**
 * Updates counters with tenant-scoped accounting.
 */
public interface TenantAwareCounter {

    /**
     * Increment the counter for a tenant.
     *
     * @param tenantId tenant id; null updates only the aggregate/single-tenant value
     */
    void incrementForTenant(String tenantId);

    /**
     * Decrement the counter for a tenant.
     *
     * @param tenantId tenant id; null updates only the aggregate/single-tenant value
     */
    void decrementForTenant(String tenantId);
}
