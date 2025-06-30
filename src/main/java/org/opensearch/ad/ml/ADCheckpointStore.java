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

package org.opensearch.ad.ml;

import java.util.Optional;

import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.ml.CheckpointDaoInterface;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * AD-specific checkpoint store abstraction that surfaces the additional helpers required by the
 * AD runtime on top of the generic checkpoint DAO contract.
 */
public interface ADCheckpointStore extends CheckpointDaoInterface<ThresholdedRandomCutForest> {

    /**
     * Persist a TRCF checkpoint.
     *
     * @param modelId id of the model
     * @param forest the TRCF model
     * @param listener callback for completion
     */
    void putTRCFCheckpoint(String modelId, ThresholdedRandomCutForest forest, ActionListener<Void> listener);

    /**
     * Persist a threshold model checkpoint.
     *
     * @param modelId id of the model
     * @param threshold thresholding model
     * @param listener callback for completion
     */
    void putThresholdCheckpoint(String modelId, ThresholdingModel threshold, ActionListener<Void> listener);

    /**
     * Load the TRCF checkpoint for the provided model id.
     *
     * @param modelId id of the model
     * @param listener callback receiving the model or empty when not found
     */
    void getTRCFModel(String modelId, ActionListener<Optional<ThresholdedRandomCutForest>> listener);

    /**
     * Load the threshold model checkpoint for the provided model id.
     *
     * @param modelId id of the model
     * @param listener callback receiving the model or empty when not found
     */
    void getThresholdModel(String modelId, ActionListener<Optional<ThresholdingModel>> listener);
}
