/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import java.util.Optional;
import java.util.function.Consumer;

import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;

/**
 * Interface for managing state of time series analysis configurations.
 * Provides methods for managing config state, jobs, exceptions, and backpressure.
 */
public interface StateManager extends MaintenanceState, CleanState, ExceptionRecorder {

    /**
     * Check if a node is muted for backpressure
     * @param nodeId an ES node's ID
     * @param configId config ID
     * @return true if the node is muted
     */
    boolean isMuted(String nodeId, String configId);

    /**
     * When we have a unsuccessful call with a node, increment the backpressure counter.
     * @param nodeId an ES node's ID
     * @param configId config ID
     */
    void addPressure(String nodeId, String configId);

    /**
     * When we have a successful call with a node, clear the backpressure counter.
     * @param nodeId an ES node's ID
     * @param configId config ID
     */
    void resetBackpressureCounter(String nodeId, String configId);

    /**
     * Get config and execute consumer function.
     * [Important!] Make sure listener returns in function.
     *
     * Compared with the overload below, this variant is for plain retrieval paths such as GET config APIs:
     * it always reads from storage, returns the persisted config even when all features are disabled,
     * preserves the document version during parsing, and reports parse/get failures via {@code listener.onFailure(...)}.
     *
     * @param configId config id
     * @param tenantId tenant id
     * @param analysisType analysis type
     * @param function consumer function.
     * @param listener action listener. Only meant to return failure.
     * @param <T> action listener response type
     */
    <T> void getConfig(
        String configId,
        String tenantId,
        AnalysisType analysisType,
        Consumer<Optional<? extends Config>> function,
        ActionListener<T> listener
    );

    /**
     * Get config with optional caching.
     *
     * Compared with the consumer overload above, this variant is used by execution-oriented paths.
     * {@code cache} only controls whether the in-memory NodeState cache is read/written; setting
     * {@code cache=false} does not make the two overloads equivalent. Even with {@code cache=false},
     * this path still applies execution-time feature validation and may fail when all features are disabled.
     * This path also parses by id only and maps parse failures to {@code Optional.empty()}.
     *
     * @param configID config id
     * @param tenantId tenant id
     * @param context analysis type
     * @param cache whether to cache the config
     * @param listener action listener
     */
    void getConfig(
        String configID,
        String tenantId,
        AnalysisType context,
        boolean cache,
        ActionListener<Optional<? extends Config>> listener
    );

    /**
     * Whether last cold start for the detector is running
     * @param adID detector ID
     * @return running or not
     */
    boolean isColdStartRunning(String adID);

    /**
     * Mark the cold start status of the detector
     * @param adID detector ID
     * @return a callback when cold start is done
     */
    Releasable markColdStartRunning(String adID);

    /**
     * Get job for a config
     * @param configID config ID
     * @param tenantId tenant id
     * @param cache whether to return cached job if present
     * @param listener action listener
     */
    void getJob(String configID, String tenantId, boolean cache, ActionListener<Optional<Job>> listener);
}
