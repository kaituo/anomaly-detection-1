/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad;

import java.time.Clock;

import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.timeseries.NodeState;

/**
 * Storing intermediate state during the execution of transport action
 *
 */
public class ADNodeState extends NodeState {
    // number of partitions
    private int partitonNumber;

    // flag indicating whether checkpoint for the detector exists
    private boolean checkPointExists;

    // cold start running flag to prevent concurrent cold start
    private boolean coldStartRunning;
    // detector job
    private AnomalyDetectorJob detectorJob;

    public ADNodeState(String detectorId, Clock clock) {
        super(detectorId, clock);
        this.partitonNumber = -1;
        this.checkPointExists = false;
        this.coldStartRunning = false;
        this.detectorJob = null;
    }

    /**
     *
     * @return RCF partition number of the detector
     */
    public int getPartitonNumber() {
        refreshLastUpdateTime();
        return partitonNumber;
    }

    /**
     *
     * @param partitonNumber RCF partition number
     */
    public void setPartitonNumber(int partitonNumber) {
        this.partitonNumber = partitonNumber;
        refreshLastUpdateTime();
    }

    /**
     * Used to indicate whether cold start succeeds or not
     * @return whether checkpoint of models exists or not.
     */
    public boolean doesCheckpointExists() {
        refreshLastUpdateTime();
        return checkPointExists;
    }

    /**
     *
     * @param checkpointExists mark whether checkpoint of models exists or not.
     */
    public void setCheckpointExists(boolean checkpointExists) {
        refreshLastUpdateTime();
        this.checkPointExists = checkpointExists;
    };

    /**
     * Used to prevent concurrent cold start
     * @return whether cold start is running or not
     */
    public boolean isColdStartRunning() {
        refreshLastUpdateTime();
        return coldStartRunning;
    }

    /**
     *
     * @param coldStartRunning  whether cold start is running or not
     */
    public void setColdStartRunning(boolean coldStartRunning) {
        this.coldStartRunning = coldStartRunning;
        refreshLastUpdateTime();
    }

    /**
     *
     * @return Detector configuration object
     */
    public AnomalyDetectorJob getDetectorJob() {
        refreshLastUpdateTime();
        return detectorJob;
    }

    /**
     *
     * @param detectorJob Detector job
     */
    public void setDetectorJob(AnomalyDetectorJob detectorJob) {
        this.detectorJob = detectorJob;
        refreshLastUpdateTime();
    }
}
