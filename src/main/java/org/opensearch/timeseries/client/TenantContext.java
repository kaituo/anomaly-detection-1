/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import java.util.Objects;

import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.TenantAwareHelper;
import org.opensearch.timeseries.util.TenantAwareHelper.TenantComponents;

/**
 * Explicit tenant isolation context.
 * <p>
 * This wrapper carries the raw tenant identity and, for user data-plane calls, the data source that owns the endpoint.
 */
public class TenantContext {
    private final String tenantId;
    private final String dataSourceId;
    private final boolean systemWide;

    private TenantContext(String tenantId, String dataSourceId, boolean systemWide) {
        this.tenantId = tenantId;
        this.dataSourceId = dataSourceId;
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
        return user(tenantId, null);
    }

    /**
     * Create a tenant-scoped context with an explicit data source id.
     *
     * @param tenantId tenant id (null in single-tenant mode)
     * @param dataSourceId data source id used for data-plane endpoint resolution
     * @return TenantContext scoped to the given tenant and data source
     */
    public static TenantContext user(String tenantId, String dataSourceId) {
        return new TenantContext(tenantId, dataSourceId, false);
    }

    /**
     * Create a tenant-scoped context from persisted config metadata.
     *
     * @param config config carrying tenant and data source ids
     * @return TenantContext scoped to the config's tenant and data source
     */
    public static TenantContext user(Config config) {
        Objects.requireNonNull(config, "config must not be null");
        return user(config.getTenantId(), config.getDataSourceId());
    }

    /**
     * Create a system-wide context for internal operations spanning all tenants.
     * @return TenantContext representing all tenants
     */
    public static TenantContext systemWide() {
        return new TenantContext(null, null, true);
    }

    public boolean isSystemWide() {
        return systemWide;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    /**
     * Lazily parses the tenant id into account id, application id, and workspace id.
     *
     * @return tenant id components, or {@code null} if this context has no tenant id
     */
    public TenantComponents getTenantComponents() {
        return tenantId == null ? null : TenantAwareHelper.parseTenantId(tenantId);
    }
}
