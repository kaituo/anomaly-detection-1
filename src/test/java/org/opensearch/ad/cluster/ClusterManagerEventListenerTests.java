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

package org.opensearch.ad.cluster;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.cluster.ClusterManagerEventListener;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.Client;

public class ClusterManagerEventListenerTests extends AbstractTimeSeriesTest {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private Clock clock;
    private DataAccess dataAccess;
    private DiscoveryNodeSelector nodeFilter;
    private RunContext runContext;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_CHECKPOINT_TTL, ForecastSettings.FORECAST_CHECKPOINT_TTL))
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        threadPool = mock(ThreadPool.class);
        client = mock(Client.class);
        clock = mock(Clock.class);
        dataAccess = mock(DataAccess.class);
        nodeFilter = mock(DiscoveryNodeSelector.class);
        runContext = mock(RunContext.class);
    }

    public void testOnOffClusterManager() {
        Cancellable hourlyCancellable = mock(Cancellable.class);
        Cancellable adCheckpointIndexRetentionCancellable = mock(Cancellable.class);
        Cancellable forecastCheckpointIndexRetentionCancellable = mock(Cancellable.class);

        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), any(String.class)))
            .thenReturn(hourlyCancellable, adCheckpointIndexRetentionCancellable, forecastCheckpointIndexRetentionCancellable);

        ClusterManagerEventListener clusterManagerService = new ClusterManagerEventListener(client, runContext);
        clusterManagerService.init(clusterService, threadPool, clock, nodeFilter, singleTenantSettings(), dataAccess);

        clusterManagerService.onClusterManager();
        assertThat(clusterManagerService.getHourlyCron(), is(notNullValue()));
        List<Cancellable> checkpointIndexRetention = clusterManagerService.getCheckpointIndexRetentionCron();
        for (Cancellable cancellable : checkpointIndexRetention) {
            assertThat(cancellable, is(notNullValue()));
        }

        clusterManagerService.offClusterManager();
        for (Cancellable cancellable : clusterManagerService.getCheckpointIndexRetentionCron()) {
            assertThat(cancellable, is(nullValue()));
        }
        assertThat(clusterManagerService.getHourlyCron(), is(nullValue()));
    }

    public void testInitRegistersListenerInSingleTenantMode() {
        ClusterManagerEventListener clusterManagerService = new ClusterManagerEventListener(client, runContext);
        clusterManagerService.init(clusterService, threadPool, clock, nodeFilter, singleTenantSettings(), dataAccess);

        verify(clusterService).addLocalNodeClusterManagerListener(clusterManagerService);
    }

    public void testInitDoesNotRegisterListenerInAdMultiTenantMode() {
        ClusterManagerEventListener clusterManagerService = new ClusterManagerEventListener(client, runContext);
        Settings settings = Settings
            .builder()
            .put(singleTenantSettings())
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), true)
            .build();
        clusterManagerService.init(clusterService, threadPool, clock, nodeFilter, settings, dataAccess);

        verify(clusterService, never()).addLocalNodeClusterManagerListener(clusterManagerService);
    }

    public void testInitDoesNotRegisterListenerInForecastMultiTenantMode() {
        ClusterManagerEventListener clusterManagerService = new ClusterManagerEventListener(client, runContext);
        Settings settings = Settings
            .builder()
            .put(singleTenantSettings())
            .put(ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.getKey(), true)
            .build();
        clusterManagerService.init(clusterService, threadPool, clock, nodeFilter, settings, dataAccess);

        verify(clusterService, never()).addLocalNodeClusterManagerListener(clusterManagerService);
    }

    private Settings singleTenantSettings() {
        return Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), false)
            .put(ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.getKey(), false)
            .put(AnomalyDetectorSettings.AD_CHECKPOINT_TTL.getKey(), TimeValue.timeValueHours(24))
            .put(ForecastSettings.FORECAST_CHECKPOINT_TTL.getKey(), TimeValue.timeValueHours(24))
            .build();
    }
}
