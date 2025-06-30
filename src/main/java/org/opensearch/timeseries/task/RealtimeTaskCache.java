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

package org.opensearch.timeseries.task;

import java.time.Duration;
import java.time.Instant;

/**
 * realtime task cache which will hold these data
 * 1. task state
 * 2. init progress
 * 3. error
 * 4. last job run time
 * 5. analysis interval
 * 6. latest job enabled time
 * 7. last successful task update time
 */
public class RealtimeTaskCache {

    private static final long RECONCILIATION_INTERVAL_IN_MILLIS = Duration.ofMinutes(10).toMillis();

    // task state
    private String state;

    // init progress
    private Float initProgress;

    // error
    private String error;

    // track last job run time, will clean up cache if no access after 2 intervals
    private long lastJobRunTime;

    // interval in milliseconds.
    private long intervalInMillis;

    // enabled time identifies the current job start generation.
    private Long jobEnabledTime;

    // track the last successful persistent task update.
    private long lastTaskUpdateTime;

    public RealtimeTaskCache(String state, Float initProgress, String error, long detectorIntervalInMillis) {
        this.state = state;
        this.initProgress = initProgress;
        this.error = error;
        this.lastJobRunTime = Instant.now().toEpochMilli();
        this.intervalInMillis = detectorIntervalInMillis;
        this.lastTaskUpdateTime = 0L;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Float getInitProgress() {
        return initProgress;
    }

    public void setInitProgress(Float initProgress) {
        this.initProgress = initProgress;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public void setLastJobRunTime(long lastJobRunTime) {
        this.lastJobRunTime = lastJobRunTime;
    }

    public Long getJobEnabledTime() {
        return jobEnabledTime;
    }

    public void setJobEnabledTime(Long jobEnabledTime) {
        this.jobEnabledTime = jobEnabledTime;
    }

    public void setLastTaskUpdateTime(long lastTaskUpdateTime) {
        this.lastTaskUpdateTime = lastTaskUpdateTime;
    }

    /**
     * Returns whether the persistent realtime task should be refreshed. This forces an update
     * after the reconciliation interval even when no cached fields have changed, helping correct
     * persistent-state drift and refresh fields such as the coordinating node. The timestamp is
     * advanced only after a successful update, so failed updates remain eligible for retry.
     */
    public boolean needsReconciliation() {
        return lastTaskUpdateTime + RECONCILIATION_INTERVAL_IN_MILLIS < Instant.now().toEpochMilli();
    }

    public boolean expired() {
        return lastJobRunTime + 2 * intervalInMillis < Instant.now().toEpochMilli();
    }
}
