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

package org.opensearch.timeseries.ml;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.timeseries.constant.CommonName;

import io.protostuff.LinkedBuffer;

/**
 * CheckpointCodec is responsible for encoding and decoding the checkpoint data.
 * @param <RCFModelType> The type of RCF model
 */
public abstract class CheckpointCodec<RCFModelType> {
    private static final Logger logger = LogManager.getLogger(CheckpointCodec.class);

    protected final GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    protected final int serializeRCFBufferSize;

    public CheckpointCodec(GenericObjectPool<LinkedBuffer> serializeRCFBufferPool, int serializeRCFBufferSize) {
        this.serializeRCFBufferPool = serializeRCFBufferPool;
        this.serializeRCFBufferSize = serializeRCFBufferSize;
    }

    public Deque<Sample> loadSampleQueue(Map<String, Object> checkpoint, String modelId) {
        Deque<Sample> sampleQueue = new ArrayDeque<>();
        // Even though we we save sample_queue using array, after ser/der, we need to read it as List
        // we start using SAMPLE_QUEUE after forecasting refactoring. Previously in AD, we use CommonName.ENTITY_SAMPLE
        // to store samples. The refactoring moves samples out of EntityModel and makes it a first-level field.
        List<Map<String, Object>> samples = (List<Map<String, Object>>) checkpoint.get(CommonName.SAMPLE_QUEUE);
        if (samples != null) {
            samples.forEach(sampleMap -> {
                try {
                    Sample sample = Sample.extractSample(sampleMap);
                    if (sample != null) {
                        sampleQueue.add(sample);
                    }
                } catch (Exception e) {
                    logger.warn("Exception while deserializing samples for " + modelId, e);
                }
            });
        }
        // can be null when checkpoint corrupted (e.g., a checkpoint not recognized by current code
        // due to bugs). Better redo training.
        return sampleQueue;
    }

    public Map.Entry<LinkedBuffer, Boolean> checkoutOrNewBuffer() {
        LinkedBuffer buffer = null;
        boolean isCheckout = true;
        try {
            buffer = serializeRCFBufferPool.borrowObject();
        } catch (Exception e) {
            logger.warn("Failed to borrow a buffer from pool", e);
        }
        if (buffer == null) {
            buffer = LinkedBuffer.allocate(serializeRCFBufferSize);
            isCheckout = false;
        }
        return new SimpleImmutableEntry<LinkedBuffer, Boolean>(buffer, isCheckout);
    }

    /**
     * Serialized samples
     * @param samples input samples
     * @return serialized object
     */
    protected Optional<Sample[]> toCheckpoint(Queue<Sample> samples) {
        if (samples == null || samples.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(samples.toArray(new Sample[0]));
    }

    public abstract ModelState<RCFModelType> fromEntityModelCheckpoint(
        Map<String, Object> checkpoint,
        String modelId,
        String configId,
        String tenantId
    );

    public abstract Map<String, Object> toIndexSource(ModelState<RCFModelType> modelState) throws IOException;
}
