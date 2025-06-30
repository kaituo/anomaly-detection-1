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

package org.opensearch.timeseries.util;

/**
 * Interface for index-related operations including health checking and index name resolution.
 */
public interface IndexOperations {
    /**
     * Status string of index that does not exist
     */
    String NONEXISTENT_INDEX_STATUS = "non-existent";

    /**
     * Status string when an alias exists, but does not point to an index
     */
    String ALIAS_EXISTS_NO_INDICES_STATUS = "alias exists, but does not point to any indices";

    /**
     * Gets the cluster index health for a particular index or the index an alias points to.
     *
     * If an alias is passed in, it will only return the health status of an index it points to if it only points to a
     * single index. If it points to multiple indices, it will throw an exception.
     *
     * @param indexOrAliasName String of the index or alias name to get health of.
     * @return String represents the status of the index: "red", "yellow" or "green"
     * @throws IllegalArgumentException Thrown when an alias is passed in that points to more than one index
     */
    String getIndexHealthStatus(String indexOrAliasName) throws IllegalArgumentException;

    /**
     * Resolve the dynamic index name based on tenant context.
     * <p>
     * For single-tenant environments, this returns the defaultIndexName.
     * For multi-tenant environments, this builds an S3 key prefix using tenant/config/entity hierarchy.
     *
     * @param tenantId tenant id
     * @param configId config id
     * @param modelId model id whose id provides the entity-level prefixes
     * @param defaultIndexName fallback index name when tenant/config info is not available
     * @return resolved index name or S3 key prefix
     */
    String resolveIndexName(String tenantId, String configId, String modelId, String defaultIndexName);
}
