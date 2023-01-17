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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.opensearch.timeseries.model.Config;

/**
 * Storing intermediate state during the execution of transport action
 *
 */
public class NodeState implements ExpiringState {
    private String configId;
    // config definition
    private Config configDef;
    // last access time
    private Instant lastAccessTime;
    // last error.
    private Optional<Exception> exception;
    // clock to get current time
    private final Clock clock;

    public NodeState(String configId, Clock clock) {
        this.configId = configId;
        this.configDef = null;
        this.lastAccessTime = clock.instant();
        this.exception = Optional.empty();
        this.clock = clock;
    }

    public String getConfigId() {
        return configId;
    }

    /**
     *
     * @return Detector configuration object
     */
    public Config getConfigDef() {
        refreshLastUpdateTime();
        return configDef;
    }

    /**
     *
     * @param configDef Analysis configuration object
     */
    public void setConfigDef(Config configDef) {
        this.configDef = configDef;
        refreshLastUpdateTime();
    }

    /**
     *
     * @return last exception if any
     */
    public Optional<Exception> getException() {
        refreshLastUpdateTime();
        return exception;
    }

    /**
     *
     * @param exception exception to record
     */
    public void setException(Exception exception) {
        this.exception = Optional.ofNullable(exception);
        refreshLastUpdateTime();
    }

    /**
     * refresh last access time.
     */
    protected void refreshLastUpdateTime() {
        lastAccessTime = clock.instant();
    }

    /**
     * @param stateTtl time to leave for the state
     * @return whether the transport state is expired
     */
    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastAccessTime, stateTtl, clock.instant());
    }
}
