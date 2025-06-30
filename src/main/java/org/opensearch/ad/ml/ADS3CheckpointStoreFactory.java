/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

/**
 * Factory for the S3-backed AD checkpoint store.
 */
public class ADS3CheckpointStoreFactory implements ADCheckpointStoreFactory {

    @Override
    public ADCheckpointStore create(ADCheckpointStoreFactoryContext context) {
        return new ADS3CheckpointDao(
            context.getSettings(),
            context.getMaxCheckpointBytes(),
            context.getTrcfSchema(),
            context.getTrcfMapper(),
            context.getConverter(),
            context.getGson(),
            context.getRcfMapper(),
            context.getThresholdingModelClass(),
            context.getAnomalyRate(),
            context.getClock(),
            context.getSerializeRCFBufferPool(),
            context.getSerializeRCFBufferSize(),
            context.getDataManagement()
        );
    }
}
