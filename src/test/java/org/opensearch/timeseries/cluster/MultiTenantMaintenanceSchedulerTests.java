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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

public class MultiTenantMaintenanceSchedulerTests extends AbstractTimeSeriesTest {

    private ClusterSettings clusterSettings;
    private ClusterService clusterService;
    private EventBridgeHandler eventBridgeHandler;
    private MultiTenantMaintenanceScheduler scheduler;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ, AnomalyDetectorSettings.AD_DAILY_S3_CLEANUP_INTERVAL)
                    )
                )
        );
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        eventBridgeHandler = mock(EventBridgeHandler.class);
        scheduler = new MultiTenantMaintenanceScheduler(eventBridgeHandler);
        scheduler
            .init(
                clusterService,
                mock(ThreadPool.class),
                Clock.systemUTC(),
                mock(DiscoveryNodeSelector.class),
                Settings.builder().putList(TimeSeriesSettings.NODE_ROLE.getKey(), TimeSeriesSettings.MASTER_ROLE).build(),
                mock(DataAccess.class)
            );
    }

    public void testCheckpointSavingFreqUpdateRefreshesMaintenanceSchedule() {
        scheduler.onClusterManager();
        clearInvocations(eventBridgeHandler);

        Settings newSettings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ.getKey(), TimeValue.timeValueMinutes(1))
            .build();
        Settings.Builder target = Settings.builder();
        clusterSettings.updateDynamicSettings(newSettings, target, Settings.builder(), "test");
        clusterSettings.applySettings(target.build());

        verify(eventBridgeHandler).setMaintenanceScheduleInterval(Duration.ofMinutes(1));
        verify(eventBridgeHandler).startHourlyCron(any());
    }

    public void testDailyS3CleanupIntervalUpdateRefreshesDailySchedule() {
        scheduler.onClusterManager();
        clearInvocations(eventBridgeHandler);

        Settings newSettings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_DAILY_S3_CLEANUP_INTERVAL.getKey(), TimeValue.timeValueMinutes(1))
            .build();
        Settings.Builder target = Settings.builder();
        clusterSettings.updateDynamicSettings(newSettings, target, Settings.builder(), "test");
        clusterSettings.applySettings(target.build());

        verify(eventBridgeHandler).setDailyS3CleanupInterval(Duration.ofMinutes(1));
        verify(eventBridgeHandler).startDailyS3CheckpointCleanup(any());
    }
}
