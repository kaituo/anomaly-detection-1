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

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.task;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.TIME_DECAY;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;

import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * AD batch task cache which will mainly hold these for one task:
 * 1. RCF
 * 2. threshold model
 * 3. shingle
 * 4. training data
 * 5. entity if task is for HC detector
 */
public class ADBatchTaskCache {
    private final String detectorId;
    private final String taskId;
    private ThresholdedRandomCutForest rcfModel;
    private boolean thresholdModelTrained;
    private Deque<Map.Entry<Long, Optional<double[]>>> shingle;
    private AtomicInteger thresholdModelTrainingDataSize = new AtomicInteger(0);
    private AtomicBoolean cancelled = new AtomicBoolean(false);
    private AtomicLong cacheMemorySize = new AtomicLong(0);
    private String cancelReason;
    private String cancelledBy;
    private Entity entity;

    protected ADBatchTaskCache(ADTask adTask) {
        this.detectorId = adTask.getDetectorId();
        this.taskId = adTask.getTaskId();
        this.entity = adTask.getEntity();

        AnomalyDetector detector = adTask.getDetector();
        int numberOfTrees = NUM_TREES;
        int shingleSize = detector.getShingleSize();
        this.shingle = new ArrayDeque<>(shingleSize);
        int dimensions = detector.getShingleSize() * detector.getEnabledFeatureIds().size();

        rcfModel = ThresholdedRandomCutForest
            .builder()
            .dimensions(dimensions)
            .numberOfTrees(numberOfTrees)
            .timeDecay(TIME_DECAY)
            .sampleSize(NUM_SAMPLES_PER_TREE)
            .outputAfter(NUM_MIN_SAMPLES)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(AnomalyDetectorSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            // for external shingling, rcf does not recognize shingle. Thus, shingle size
            // is 1 here.
            // shingle in detector config and shingle size here are different things.
            // shingle size in detector config impacts dimensions.
            .shingleSize(1)
            .anomalyRate(1 - AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE)
            .build();

        this.thresholdModelTrained = false;
    }

    protected String getDetectorId() {
        return detectorId;
    }

    protected String getTaskId() {
        return taskId;
    }

    protected ThresholdedRandomCutForest getTRcfModel() {
        return rcfModel;
    }

    protected Deque<Map.Entry<Long, Optional<double[]>>> getShingle() {
        return shingle;
    }

    protected void setThresholdModelTrained(boolean thresholdModelTrained) {
        this.thresholdModelTrained = thresholdModelTrained;
    }

    protected boolean isThresholdModelTrained() {
        return thresholdModelTrained;
    }

    public AtomicInteger getThresholdModelTrainingDataSize() {
        return thresholdModelTrainingDataSize;
    }

    protected AtomicLong getCacheMemorySize() {
        return cacheMemorySize;
    }

    protected boolean isCancelled() {
        return cancelled.get();
    }

    protected String getCancelReason() {
        return cancelReason;
    }

    protected String getCancelledBy() {
        return cancelledBy;
    }

    public Entity getEntity() {
        return entity;
    }

    protected void cancel(String reason, String userName) {
        this.cancelled.compareAndSet(false, true);
        this.cancelReason = reason;
        this.cancelledBy = userName;
    }
}
