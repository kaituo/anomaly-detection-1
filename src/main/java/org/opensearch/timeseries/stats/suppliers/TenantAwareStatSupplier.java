/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.stats.suppliers;

import java.util.function.Supplier;

/**
 * Supplies stat values scoped by tenant when a tenant id is available.
 *
 * @param <T> value type
 */
public interface TenantAwareStatSupplier<T> extends Supplier<T> {

    /**
     * Get the value for the provided tenant.
     *
     * @param tenantId tenant id; null means aggregate/single-tenant value
     * @return stat value
     */
    T getForTenant(String tenantId);

    @Override
    default T get() {
        return getForTenant(null);
    }
}
