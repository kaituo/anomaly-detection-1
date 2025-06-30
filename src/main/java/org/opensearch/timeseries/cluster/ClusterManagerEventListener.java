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
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.ml.CheckpointDaoInterface;
import org.opensearch.timeseries.util.DateUtils;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.Client;

import com.google.common.annotations.VisibleForTesting;

@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Only meant to be used in single-tenant.")
public class ClusterManagerEventListener implements ClusterManagerTask {

    private static final Logger LOG = LogManager.getLogger(ClusterManagerEventListener.class);

    private Cancellable adCheckpointIndexRetentionCron;
    private Cancellable forecastCheckpointIndexRetentionCron;
    private Cancellable hourlyCron;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private final Client client;
    private final CheckpointDaoInterface<?> adCheckpointStore;
    private final CheckpointDaoInterface<?> forecastCheckpointStore;
    private Clock clock;
    private DiscoveryNodeSelector nodeFilter;
    private Duration adCheckpointTtlDuration;
    private Duration forecastCheckpointTtlDuration;

    public ClusterManagerEventListener(
        Client client,
        CheckpointDaoInterface<?> adCheckpointStore,
        CheckpointDaoInterface<?> forecastCheckpointStore
    ) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.adCheckpointStore = Objects.requireNonNull(adCheckpointStore, "adCheckpointStore must not be null");
        this.forecastCheckpointStore = Objects.requireNonNull(forecastCheckpointStore, "forecastCheckpointStore must not be null");
    }

    @Override
    public void init(
        ClusterService clusterService,
        ThreadPool threadPool,
        Clock clock,
        DiscoveryNodeSelector nodeFilter,
        Settings settings,
        DataAccess dataAccess
    ) {
        boolean adMultiTenancyEnabled = AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings);
        boolean forecastMultiTenancyEnabled = ForecastEnabledSetting.isForecastMultiTenancyEnabled(settings);

        boolean multiTenancyEnabled = adMultiTenancyEnabled || forecastMultiTenancyEnabled;
        // only used in single-tenant mode
        if (multiTenancyEnabled) {
            return;
        }

        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeClusterManagerListener(this);
        this.clock = clock;
        this.nodeFilter = nodeFilter;

        this.adCheckpointTtlDuration = DateUtils.toDuration(AnomalyDetectorSettings.AD_CHECKPOINT_TTL.get(settings));
        this.forecastCheckpointTtlDuration = DateUtils.toDuration(ForecastSettings.FORECAST_CHECKPOINT_TTL.get(settings));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.AD_CHECKPOINT_TTL, it -> {
            this.adCheckpointTtlDuration = DateUtils.toDuration(it);
            cancel(adCheckpointIndexRetentionCron);
            adCheckpointIndexRetentionCron = scheduleAdCheckpointRetention();
        });

        clusterService.getClusterSettings().addSettingsUpdateConsumer(ForecastSettings.FORECAST_CHECKPOINT_TTL, it -> {
            this.forecastCheckpointTtlDuration = DateUtils.toDuration(it);
            cancel(forecastCheckpointIndexRetentionCron);
            forecastCheckpointIndexRetentionCron = scheduleForecastCheckpointRetention();
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
            adCheckpointIndexRetentionCron = scheduleAdCheckpointRetention();
            if (adCheckpointIndexRetentionCron != null) {
                clusterService.addLifecycleListener(new LifecycleListener() {
                    @Override
                    public void beforeStop() {
                        cancel(adCheckpointIndexRetentionCron);
                        adCheckpointIndexRetentionCron = null;
                    }
                });
            }
        }

        if (forecastCheckpointIndexRetentionCron == null) {
            forecastCheckpointIndexRetentionCron = scheduleForecastCheckpointRetention();
            if (forecastCheckpointIndexRetentionCron != null) {
                clusterService.addLifecycleListener(new LifecycleListener() {
                    @Override
                    public void beforeStop() {
                        cancel(forecastCheckpointIndexRetentionCron);
                        forecastCheckpointIndexRetentionCron = null;
                    }
                });
            }
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

    private Cancellable scheduleAdCheckpointRetention() {
        return threadPool
            .scheduleWithFixedDelay(
                adCheckpointStore.createRetentionTask(adCheckpointTtlDuration, clock),
                TimeValue.timeValueHours(24),
                executorName()
            );
    }

    private Cancellable scheduleForecastCheckpointRetention() {
        return threadPool
            .scheduleWithFixedDelay(
                forecastCheckpointStore.createRetentionTask(forecastCheckpointTtlDuration, clock),
                TimeValue.timeValueHours(24),
                executorName()
            );
    }
}
