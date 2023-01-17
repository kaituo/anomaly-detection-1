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

package org.opensearch.forecast;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.ExpiringState;

public class ForecastNodeState implements ExpiringState {
    private final String forecasterId;
    // forecaster definition
    private Forecaster forecasterDef;
    // last access time
    private Instant lastAccessTime;
    // last error.
    private Optional<Exception> exception;
    // clock to get current time
    private final Clock clock;
    // cold start running flag to prevent concurrent cold start
    private boolean coldStartRunning;

    public ForecastNodeState(String forecasterId, Clock clock) {
        this.forecasterId = forecasterId;
        this.forecasterDef = null;
        this.lastAccessTime = clock.instant();
        this.exception = Optional.empty();
        this.clock = clock;
        this.coldStartRunning = false;
    }

    public String getForecasterId() {
        refreshLastUpdateTime();
        return forecasterId;
    }

    public Forecaster getForecasterDef() {
        refreshLastUpdateTime();
        return forecasterDef;
    }

    public void setForecasterDef(Forecaster forecasterDef) {
        this.forecasterDef = forecasterDef;
        refreshLastUpdateTime();
    }

    public Instant getLastAccessTime() {
        refreshLastUpdateTime();
        return lastAccessTime;
    }

    public void setLastAccessTime(Instant lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        refreshLastUpdateTime();
    }

    public Optional<Exception> getException() {
        refreshLastUpdateTime();
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = Optional.ofNullable(exception);
        refreshLastUpdateTime();
    }

    public boolean isColdStartRunning() {
        refreshLastUpdateTime();
        return coldStartRunning;
    }

    public void setColdStartRunning(boolean coldStartRunning) {
        this.coldStartRunning = coldStartRunning;
        refreshLastUpdateTime();
    }

    /**
     * refresh last access time.
     */
    private void refreshLastUpdateTime() {
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
