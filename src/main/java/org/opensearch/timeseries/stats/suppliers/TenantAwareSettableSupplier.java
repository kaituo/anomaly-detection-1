/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.stats.suppliers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Settable supplier that keeps independent values per tenant.
 */
public class TenantAwareSettableSupplier implements TenantAwareStatSupplier<Long>, TenantAwareSettable {
    private final AtomicLong total;
    private final ConcurrentMap<String, AtomicLong> tenantValues;

    public TenantAwareSettableSupplier() {
        this.total = new AtomicLong(0L);
        this.tenantValues = new ConcurrentHashMap<>();
    }

    @Override
    public Long getForTenant(String tenantId) {
        if (tenantId == null) {
            return total.get();
        }
        AtomicLong value = tenantValues.get(tenantId);
        return value == null ? 0L : value.get();
    }

    @Override
    public void setForTenant(String tenantId, Long value) {
        if (tenantId == null) {
            total.set(value);
            return;
        }
        tenantValues.computeIfAbsent(tenantId, ignored -> new AtomicLong(0L)).set(value);
    }
}
