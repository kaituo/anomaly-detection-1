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

import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;

/**
 * Abstraction for filtering discovery nodes.
 */
public interface DiscoveryNodeSelector {
    DiscoveryNode[] getEligibleDataNodes();

    boolean isEligibleDataNode(DiscoveryNode node);

    boolean isEligibleNode(DiscoveryNode node);

    /**
     * Check if a node with the given ID exists in the cluster.
     *
     * @param nodeId the node ID to check
     * @return true if the node exists, false otherwise
     */
    boolean nodeExists(String nodeId);

    /**
     * Check if there is a global read or write block on the cluster.
     *
     * @param tenantId the tenant id for endpoint resolution; may be {@code null}
     *                 for operations that don't target a specific tenant
     * @param listener the listener to receive the result (true if blocked, false otherwise)
     */
    void hasGlobalBlock(String tenantId, ActionListener<Boolean> listener);

    /**
     * Check if the specified indices have a block at the given level.
     *
     * @param tenantId the tenant id for endpoint resolution; must not be {@code null}
     *                 when checking user indices (result index, source index)
     * @param level the block level to check (e.g., READ, WRITE)
     * @param indices the indices to check for blocks
     * @param listener the listener to receive the result (true if blocked, false otherwise)
     */
    void hasIndicesBlock(String tenantId, ClusterBlockLevel level, String[] indices, ActionListener<Boolean> listener);

    default int getNumberOfEligibleDataNodes() {
        return getEligibleDataNodes().length;
    }
}
