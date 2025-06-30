/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

/**
 * Factory for pluggable AD checkpoint store implementations.
 */
public interface ADCheckpointStoreFactory {
    ADCheckpointStore create(ADCheckpointStoreFactoryContext context);
}
