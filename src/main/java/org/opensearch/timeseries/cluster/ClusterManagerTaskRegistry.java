/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster;

import java.time.Clock;
import java.util.List;

import org.opensearch.ad.cluster.ADClusterMembershipReaderTask;
import org.opensearch.ad.cluster.ADSQSConsumerTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;

/*
 * Instead of SPI, ClusterManagerTaskRegistry is of program‑to‑interface + explicit wiring style.
 * Java SPI shines when third‑party JARs can appear (JDBC drivers, logging back‑ends, etc.).
 * Since we already ship the concrete classes ourselves, SPI only adds indirection and class‑loader headaches.
 */
public final class ClusterManagerTaskRegistry {

    private final List<ClusterManagerTask> tasks;

    public ClusterManagerTaskRegistry(
        ClusterService cs,
        ThreadPool pool,
        Client client,
        Clock clock,
        ClientUtil util,
        DiscoveryNodeFilterer filterer,
        Settings settings,
        HashRing hashRing,
        NodeStateManager nodeStateManager
    ) {

        tasks = List
            .of(
                new ClusterManagerEventListener(),   // checkpoint + hourly jobs
                new CloudMapWatcherTask(),            // cloud-map watcher
                new ADSQSConsumerTask(nodeStateManager), // SQS consumer
                new ADClusterMembershipReaderTask(hashRing)      // ad cluster membership reader
            );

        // initialise and let each task register itself as a listener
        tasks.forEach(t -> t.init(cs, pool, client, clock, util, filterer, settings));
    }

    public List<ClusterManagerTask> all() {
        return tasks;
    }
}
