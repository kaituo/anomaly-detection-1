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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.opensearch.ad.cluster.diskcleanup.ADCheckpointIndexRetention;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.cluster.diskcleanup.ForecastCheckpointIndexRetention;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.cluster.diskcleanup.IndexCleanup;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DateUtils;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;

import com.google.common.annotations.VisibleForTesting;

public class ClusterManagerEventListener implements ClusterManagerTask {

    private Cancellable adCheckpointIndexRetentionCron;
    private Cancellable forecastCheckpointIndexRetentionCron;
    private Cancellable hourlyCron;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private Clock clock;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private Duration adCheckpointTtlDuration;
    private Duration forecastCheckpointTtlDuration;

    /* zero-arg ctor for ServiceLoader */
    public ClusterManagerEventListener() {}

    @Override
    public void init(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        Clock clock,
        ClientUtil clientUtil,
        DiscoveryNodeFilterer nodeFilter,
        Settings settings
    ) {
        if (AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings)
            || ForecastEnabledSetting.isForecastMultiTenancyEnabled(settings)) {
            return;
        }

        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService.addLocalNodeClusterManagerListener(this);
        this.clock = clock;
        this.clientUtil = clientUtil;
        this.nodeFilter = nodeFilter;

        this.adCheckpointTtlDuration = DateUtils.toDuration(AnomalyDetectorSettings.AD_CHECKPOINT_TTL.get(settings));
        this.forecastCheckpointTtlDuration = DateUtils.toDuration(ForecastSettings.FORECAST_CHECKPOINT_TTL.get(settings));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.AD_CHECKPOINT_TTL, it -> {
            this.adCheckpointTtlDuration = DateUtils.toDuration(it);
            cancel(adCheckpointIndexRetentionCron);
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            adCheckpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ADCheckpointIndexRetention(adCheckpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
        });

        clusterService.getClusterSettings().addSettingsUpdateConsumer(ForecastSettings.FORECAST_CHECKPOINT_TTL, it -> {
            this.forecastCheckpointTtlDuration = DateUtils.toDuration(it);
            cancel(forecastCheckpointIndexRetentionCron);
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            forecastCheckpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ForecastCheckpointIndexRetention(forecastCheckpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
        });
    }

    @Override
    public void onClusterManager() {
        if (hourlyCron == null) {
            hourlyCron = threadPool.scheduleWithFixedDelay(new HourlyCron(client, nodeFilter), TimeValue.timeValueHours(1), executorName());
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel(hourlyCron);
                    hourlyCron = null;
                }
            });
        }

        if (adCheckpointIndexRetentionCron == null) {
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            adCheckpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ADCheckpointIndexRetention(adCheckpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel(adCheckpointIndexRetentionCron);
                    adCheckpointIndexRetentionCron = null;
                }
            });
        }

        if (forecastCheckpointIndexRetentionCron == null) {
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            forecastCheckpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ForecastCheckpointIndexRetention(forecastCheckpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel(forecastCheckpointIndexRetentionCron);
                    forecastCheckpointIndexRetentionCron = null;
                }
            });
        }
    }

    @Override
    public void offClusterManager() {
        cancel(hourlyCron);
        hourlyCron = null;
        cancel(adCheckpointIndexRetentionCron);
        adCheckpointIndexRetentionCron = null;
        cancel(forecastCheckpointIndexRetentionCron);
        forecastCheckpointIndexRetentionCron = null;
    }

    private void cancel(Cancellable cron) {
        if (cron != null) {
            cron.cancel();
        }
    }

    @VisibleForTesting
    public List<Cancellable> getCheckpointIndexRetentionCron() {
        return Arrays.asList(adCheckpointIndexRetentionCron, forecastCheckpointIndexRetentionCron);
    }

    public Cancellable getHourlyCron() {
        return hourlyCron;
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }
}
