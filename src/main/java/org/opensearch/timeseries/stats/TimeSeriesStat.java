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

package org.opensearch.timeseries.stats;

import java.util.function.Supplier;

import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.stats.suppliers.SettableSupplier;
import org.opensearch.timeseries.stats.suppliers.TenantAwareCounter;
import org.opensearch.timeseries.stats.suppliers.TenantAwareSettable;
import org.opensearch.timeseries.stats.suppliers.TenantAwareStatSupplier;

/**
 * Class represents a stat the plugin keeps track of
 */
public class TimeSeriesStat<T> {
    private Boolean clusterLevel;
    private Supplier<T> supplier;

    /**
     * Constructor
     *
     * @param clusterLevel whether the stat has clusterLevel scope or nodeLevel scope
     * @param supplier supplier that returns the stat's value
     */
    public TimeSeriesStat(Boolean clusterLevel, Supplier<T> supplier) {
        this.clusterLevel = clusterLevel;
        this.supplier = supplier;
    }

    /**
     * Determines whether the stat is cluster specific or node specific
     *
     * @return true is stat is cluster level; false otherwise
     */
    public Boolean isClusterLevel() {
        return clusterLevel;
    }

    /**
     * Get the value of the statistic
     *
     * @return T value of the stat
     */
    public T getValue() {
        return supplier.get();
    }

    /**
     * Get the tenant-scoped statistic value when the underlying supplier supports it.
     *
     * @param tenantId tenant id; null means aggregate/single-tenant value
     * @return T value of the stat
     */
    public T getValueForTenant(String tenantId) {
        if (supplier instanceof TenantAwareStatSupplier) {
            return ((TenantAwareStatSupplier<T>) supplier).getForTenant(tenantId);
        }
        return getValue();
    }

    /**
     * Set the value of the statistic
     *
     * @param value set value
     */
    public void setValue(Long value) {
        if (supplier instanceof SettableSupplier) {
            ((SettableSupplier) supplier).set(value);
        } else if (supplier instanceof TenantAwareSettable) {
            ((TenantAwareSettable) supplier).setForTenant(null, value);
        }
    }

    /**
     * Set the tenant-scoped statistic value when the underlying supplier supports it.
     *
     * @param tenantId tenant id; null means aggregate/single-tenant value
     * @param value set value
     */
    public void setValueForTenant(String tenantId, Long value) {
        if (supplier instanceof TenantAwareSettable) {
            ((TenantAwareSettable) supplier).setForTenant(tenantId, value);
        } else {
            setValue(value);
        }
    }

    /**
     * Increments the supplier if it can be incremented
     */
    public void increment() {
        if (supplier instanceof CounterSupplier) {
            ((CounterSupplier) supplier).increment();
        } else if (supplier instanceof TenantAwareCounter) {
            ((TenantAwareCounter) supplier).incrementForTenant(null);
        }
    }

    /**
     * Increment the tenant-scoped counter when the underlying supplier supports it.
     *
     * @param tenantId tenant id; null means aggregate/single-tenant value
     */
    public void incrementForTenant(String tenantId) {
        if (supplier instanceof TenantAwareCounter) {
            ((TenantAwareCounter) supplier).incrementForTenant(tenantId);
        } else {
            increment();
        }
    }

    /**
     * Decrease the supplier if it can be decreased.
     */
    public void decrement() {
        if (supplier instanceof CounterSupplier) {
            ((CounterSupplier) supplier).decrement();
        } else if (supplier instanceof TenantAwareCounter) {
            ((TenantAwareCounter) supplier).decrementForTenant(null);
        }
    }

    /**
     * Decrement the tenant-scoped counter when the underlying supplier supports it.
     *
     * @param tenantId tenant id; null means aggregate/single-tenant value
     */
    public void decrementForTenant(String tenantId) {
        if (supplier instanceof TenantAwareCounter) {
            ((TenantAwareCounter) supplier).decrementForTenant(tenantId);
        } else {
            decrement();
        }
    }
}
