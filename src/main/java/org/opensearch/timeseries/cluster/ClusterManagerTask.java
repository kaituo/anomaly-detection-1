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

package org.opensearch.timeseries.cluster;

import java.time.Clock;

import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

/**
 * A pluggable unit of work that is started / stopped
 * whenever the local node becomes (or stops being)
 * the cluster-manager node.
 */
public interface ClusterManagerTask extends LocalNodeClusterManagerListener {

    /** Called once immediately after instantiation. */
    void init(ClusterService cs, ThreadPool pool, Clock clock, DiscoveryNodeSelector filterer, Settings settings, DataAccess dataAccess);
}
