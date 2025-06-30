/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.stats.suppliers;

/**
 * Sets stat values with tenant-scoped accounting.
 */
public interface TenantAwareSettable {

    /**
     * Set the value for a tenant.
     *
     * @param tenantId tenant id; null sets the aggregate/single-tenant value
     * @param value value to set
     */
    void setForTenant(String tenantId, Long value);
}
