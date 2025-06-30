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

package org.opensearch.timeseries;

/**
 * Represent a state organized via detectorId.  When deleting a detector's state,
 * we can remove it from the state.
 *
 *
 */
public interface CleanState {
    /**
     * Remove state associated with a detector Id
     * @param tenantId Tenant Id
     * @param configId Config Id
     */
    void clear(String tenantId, String configId);

    /**
     * Record the time at which a config's local model state was cleared.
     *
     * This is separate from {@link #clear(String, String)} because some callers clear
     * local coordination cache during routing changes. Model deletion/stop workflows
     * use this marker to make in-flight model work drop stale requests that were
     * created before the clear, including requests for future scheduled data windows.
     *
     * @param tenantId Tenant Id
     * @param configId Config Id
     */
    default void markConfigStateCleared(String tenantId, String configId) {}

    /**
     * Whether a request created at {@code requestEpochMillis} is stale
     * because local model state for the config was cleared later.
     *
     * @param tenantId Tenant Id
     * @param configId Config Id
     * @param requestEpochMillis wall-clock request creation timestamp
     * @return true when the request should be dropped as stale
     */
    default boolean isConfigStateClearedAfter(String tenantId, String configId, long requestEpochMillis) {
        return false;
    }
}
