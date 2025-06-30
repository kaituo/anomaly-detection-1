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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.constant.CommonName;

/**
 * Util class to filter unwanted node types
 *
 */
@SuppressForbidden(reason = "org.opensearch.cluster.service.ClusterService#state usage: Only meant to be used in single-tenant.")
public class DiscoveryNodeFilterer implements DiscoveryNodeSelector {
    private static final Logger LOG = LogManager.getLogger(DiscoveryNodeFilterer.class);
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final HotDataNodePredicate eligibleNodeFilter;

    public DiscoveryNodeFilterer(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        eligibleNodeFilter = new HotDataNodePredicate();
    }

    /**
     * Find nodes that are elibile to be used by us.  For example, Ultrawarm
     *  introduces warm nodes into the ES cluster. Currently, we distribute
     *  model partitions to all data nodes in the cluster randomly, which
     *  could cause a model performance downgrade issue once warm nodes
     *  are throttled due to resource limitations. The PR excludes warm nodes
     *  to place model partitions.
     * @return an array of eligible data nodes
     */
    public DiscoveryNode[] getEligibleDataNodes() {
        ClusterState state = this.clusterService.state();
        final List<DiscoveryNode> eligibleNodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (eligibleNodeFilter.test(node)) {
                eligibleNodes.add(node);
            }
        }
        return eligibleNodes.toArray(new DiscoveryNode[0]);
    }

    public boolean isEligibleDataNode(DiscoveryNode node) {
        return eligibleNodeFilter.test(node);
    }

    /**
     * @param node a discovery node
     * @return whether we should use this node for AD
     */
    public boolean isEligibleNode(DiscoveryNode node) {
        return eligibleNodeFilter.test(node);
    }

    @Override
    public boolean nodeExists(String nodeId) {
        return clusterService.state().nodes().nodeExists(nodeId);
    }

    @Override
    public void hasGlobalBlock(ActionListener<Boolean> listener) {
        try {
            ClusterState state = clusterService.state();
            boolean blocked = state.blocks().globalBlockedException(ClusterBlockLevel.READ) != null
                || state.blocks().globalBlockedException(ClusterBlockLevel.WRITE) != null;
            listener.onResponse(blocked);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void hasIndicesBlock(ClusterBlockLevel level, String[] indices, ActionListener<Boolean> listener) {
        try {
            ClusterState state = clusterService.state();
            // The original index might be an index expression with wildcards like "log*",
            // so we need to expand the expression to concrete index names
            String[] concreteIndices = indexNameExpressionResolver
                .concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);
            boolean blocked = state.blocks().indicesBlockedException(level, concreteIndices) != null;
            listener.onResponse(blocked);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    static class HotDataNodePredicate implements Predicate<DiscoveryNode> {
        @Override
        public boolean test(DiscoveryNode discoveryNode) {
            return discoveryNode.isDataNode()
                && discoveryNode
                    .getAttributes()
                    .getOrDefault(CommonName.BOX_TYPE_KEY, CommonName.HOT_BOX_TYPE)
                    .equals(CommonName.HOT_BOX_TYPE);
        }
    }
}
