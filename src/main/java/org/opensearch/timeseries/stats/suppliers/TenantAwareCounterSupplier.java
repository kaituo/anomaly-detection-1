/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.stats.suppliers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counter supplier that keeps both aggregate and per-tenant values.
 */
public class TenantAwareCounterSupplier implements TenantAwareStatSupplier<Long>, TenantAwareCounter {
    private final LongAdder total;
    private final ConcurrentMap<String, LongAdder> tenantCounters;

    public TenantAwareCounterSupplier() {
        this.total = new LongAdder();
        this.tenantCounters = new ConcurrentHashMap<>();
    }

    @Override
    public Long getForTenant(String tenantId) {
        if (tenantId == null) {
            return total.longValue();
        }
        LongAdder counter = tenantCounters.get(tenantId);
        return counter == null ? 0L : counter.longValue();
    }

    @Override
    public void incrementForTenant(String tenantId) {
        total.increment();
        if (tenantId != null) {
            tenantCounters.computeIfAbsent(tenantId, ignored -> new LongAdder()).increment();
        }
    }

    @Override
    public void decrementForTenant(String tenantId) {
        total.decrement();
        if (tenantId != null) {
            tenantCounters.computeIfAbsent(tenantId, ignored -> new LongAdder()).decrement();
        }
    }
}
