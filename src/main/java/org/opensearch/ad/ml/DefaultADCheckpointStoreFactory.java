/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

/**
 * Default AD checkpoint store factory that returns the index-backed implementation.
 */
public class DefaultADCheckpointStoreFactory implements ADCheckpointStoreFactory {

    @Override
    public ADCheckpointStore create(ADCheckpointStoreFactoryContext context) {
        return new ADCheckpointDao(
            context.getClient(),
            context.getClientUtil(),
            context.getGson(),
            context.getRcfMapper(),
            context.getConverter(),
            context.getTrcfMapper(),
            context.getTrcfSchema(),
            context.getThresholdingModelClass(),
            context.getDataManagement(),
            context.getMaxCheckpointBytes(),
            context.getSerializeRCFBufferPool(),
            context.getSerializeRCFBufferSize(),
            context.getAnomalyRate(),
            context.getClock()
        );
    }
}
