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

public final class CloudMapWatcherTask implements ClusterManagerTask {

    private ThreadPool pool;
    private ClusterService cs;
    private Settings settings;
    private Clock clock;
    private TimeValue period;
    private Cancellable cron;

    public CloudMapWatcherTask() {}

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
        if (!DynamicStringSetting.getInstance().isCloudmapWatcherNode()) {
            return;
        }

        this.cs = cs;
        this.pool = pool;
        this.clock = clock;
        this.settings = settings;

        period = TimeSeriesSettings.CLOUD_MAP_TTL.get(settings);

        // important as we depend on being notified when the cluster manager changes
        // without this, we will not be notified when the cluster manager changes
        cs.addLocalNodeClusterManagerListener(this);

        cs.getClusterSettings().addSettingsUpdateConsumer(TimeSeriesSettings.CLOUD_MAP_TTL, v -> {
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
        cron = pool.scheduleWithFixedDelay(new CloudMapWatcher(settings, clock), period, CommonName.CLOUD_MAP_WATCHER_THREAD_POOL_NAME);
        cs.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                cancel(cron);
                cron = null;
            }
        });
    }

    private static void cancel(Cancellable c) {
        if (c != null)
            c.cancel();
    }

    public Cancellable getAdCloudMapWatcherCron() {
        return cron;
    }
}
