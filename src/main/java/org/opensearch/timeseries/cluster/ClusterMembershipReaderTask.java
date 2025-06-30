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
import java.util.List;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

public abstract class ClusterMembershipReaderTask implements ClusterManagerTask {

    private ThreadPool pool;
    private ClusterService cs;
    private Settings settings;
    private Clock clock;
    private TimeValue period;
    private Cancellable cron;
    private String threadPoolName;
    private HashRing hashRing;

    public ClusterMembershipReaderTask(String threadPoolName, HashRing hashRing) {
        this.threadPoolName = threadPoolName;
        this.hashRing = hashRing;
    }

    @Override
    public void init(ClusterService cs, ThreadPool pool, Clock clock, DiscoveryNodeSelector f, Settings settings, DataAccess dataAccess) {
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        // Multi-tenant model nodes need the same membership snapshot as coordinators so
        // model-side HC ownership checks can resolve hash-ring owners locally.
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE) && !roles.contains(TimeSeriesSettings.MODEL_ROLE)) {
            return;
        }

        this.cs = cs;
        this.pool = pool;
        this.clock = clock;
        this.settings = settings;

        // important as we depend on being notified when the cluster manager changes
        // without this, we will not be notified when the cluster manager changes
        cs.addLocalNodeClusterManagerListener(this);

        period = TimeSeriesSettings.CLUSTER_MEMBERSHIP_READER_TTL.get(settings);

        cs.getClusterSettings().addSettingsUpdateConsumer(TimeSeriesSettings.CLUSTER_MEMBERSHIP_READER_TTL, v -> {
            period = v;
            restart();
        });
    }

    @Override
    public void onClusterManager() {
        restart();
    }

    @Override
    public void offClusterManager() {
        cancel(cron);
        cron = null;
    }

    private void restart() {
        cancel(cron);
        cron = pool.scheduleWithFixedDelay(new ClusterMembershipReader(settings, clock, hashRing), period, threadPoolName);
        cs.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                cancel(cron);
                cron = null;
            }
        });
    }

    private static void cancel(Cancellable c) {
        if (c != null) {
            c.cancel();
        }
    }

    public Cancellable getClusterMembershipReaderCron() {
        return cron;
    }
}
