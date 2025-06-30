/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

/**
 * Explicit tenant context wrapper to force callers to choose between a specific tenant
 * and a system-wide operation.
 */
public class TenantContext {
    private final String tenantId;
    private final boolean systemWide;

    private TenantContext(String tenantId, boolean systemWide) {
        this.tenantId = tenantId;
        this.systemWide = systemWide;
    }

    /**
     * Create a tenant-scoped context.
     * 
     * In single-tenant mode, tenantId is legitimately null -- returns null when isMultiTenancyEnabled == false.
     * It is caller's responsibility to handle this case appropriately (e.g., TenantAwareHelper.validateTenantId().
     * @param tenantId tenant id (null in single-tenant mode)
     * @return TenantContext scoped to the given tenant
     */
    public static TenantContext user(String tenantId) {
        return new TenantContext(tenantId, false);
    }

    /**
     * Create a system-wide context for internal operations spanning all tenants.
     * @return TenantContext representing all tenants
     */
    public static TenantContext systemWide() {
        return new TenantContext(null, true);
    }

    public boolean isSystemWide() {
        return systemWide;
    }

    public String getTenantId() {
        return tenantId;
    }
}
