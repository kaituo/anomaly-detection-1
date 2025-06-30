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

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.DynamicStringSetting;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;

public abstract class ClusterMembershipReaderTask implements ClusterManagerTask {

    private ThreadPool pool;
    private ClusterService cs;
    private Settings settings;
    private Clock clock;
    private TimeValue period;
    private Cancellable cron;
    private String threadPoolName;

    public ClusterMembershipReaderTask(String threadPoolName) {
        this.threadPoolName = threadPoolName;
    }

    @Override
    public void init(
        ClusterService cs,
        ThreadPool pool,
        Client client,
        Clock clock,
        ClientUtil util,
        DiscoveryNodeFilterer f,
        Settings settings
    ) {
        if (!DynamicStringSetting.getInstance().isCoordinatorNode()) {
            return;
        }

        this.cs = cs;
        this.pool = pool;
        this.clock = clock;
        this.settings = settings;

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
        cron = pool.scheduleWithFixedDelay(new ClusterMembershipReader(settings, clock), period, CommonName.MEMBERSHIP_READER_THREAD_POOL_NAME);
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