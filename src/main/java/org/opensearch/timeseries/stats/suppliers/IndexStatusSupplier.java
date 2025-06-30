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

package org.opensearch.timeseries.stats.suppliers;

import java.util.function.Supplier;

import org.opensearch.timeseries.util.IndexOperations;

/**
 * IndexStatusSupplier provides the status of an index as the value
 */
public class IndexStatusSupplier implements Supplier<String> {
    private IndexOperations indexOperations;
    private String indexName;

    public static final String UNABLE_TO_RETRIEVE_HEALTH_MESSAGE = "unable to retrieve health";

    /**
     * Constructor
     *
     * @param indexOperations Operations for getting index health status
     * @param indexName Name of index to extract stats from
     */
    public IndexStatusSupplier(IndexOperations indexOperations, String indexName) {
        this.indexOperations = indexOperations;
        this.indexName = indexName;
    }

    @Override
    public String get() {
        try {
            return indexOperations.getIndexHealthStatus(null, indexName);
        } catch (IllegalArgumentException e) {
            return UNABLE_TO_RETRIEVE_HEALTH_MESSAGE;
        }

    }
}
