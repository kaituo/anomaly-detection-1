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

import static org.mockito.Mockito.mock;
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
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.cluster.CloudMapWatcherTask;
import org.opensearch.timeseries.cluster.ClusterManagerEventListener;
import org.opensearch.timeseries.cluster.ClusterManagerTask;
import org.opensearch.timeseries.cluster.ClusterManagerTaskRegistry;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.cluster.MultiTenantMaintenanceScheduler;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.Client;

public class ClusterManagerTaskRegistryTests extends AbstractTimeSeriesTest {

    private ThreadPool threadPool;
    private Client client;
    private Clock clock;
    private DataAccess dataAccess;
    private DiscoveryNodeSelector nodeFilter;
    private HashRing hashRing;
    private StateManager stateManager;
    private NamedXContentRegistry xContentRegistry;
    private RunContext runContext;
    private EventBridgeHandler eventBridgeHandler;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        client = mock(Client.class);
        clock = mock(Clock.class);
        dataAccess = mock(DataAccess.class);
        nodeFilter = mock(DiscoveryNodeSelector.class);
        hashRing = mock(HashRing.class);
        stateManager = mock(StateManager.class);
        xContentRegistry = NamedXContentRegistry.EMPTY;
        runContext = mock(RunContext.class);
        eventBridgeHandler = mock(EventBridgeHandler.class);
    }

    public void testSingleTenantRegistryContainsOnlyClusterManagerEventListener() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_CHECKPOINT_TTL, ForecastSettings.FORECAST_CHECKPOINT_TTL))
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ClusterManagerTaskRegistry registry = new ClusterManagerTaskRegistry(
            clusterService,
            threadPool,
            client,
            clock,
            dataAccess,
            nodeFilter,
            singleTenantSettings(),
            hashRing,
            stateManager,
            xContentRegistry,
            runContext,
            eventBridgeHandler
        );

        List<ClusterManagerTask> tasks = registry.all();
        assertEquals(1, tasks.size());
        assertTrue(tasks.get(0) instanceof ClusterManagerEventListener);
    }

    public void testMultiTenantRegistryContainsExpectedTasksWhenAdMultiTenancyEnabled() {
        ClusterManagerTaskRegistry registry = new ClusterManagerTaskRegistry(
            mock(ClusterService.class),
            threadPool,
            client,
            clock,
            dataAccess,
            nodeFilter,
            adMultiTenantSettings(),
            hashRing,
            stateManager,
            xContentRegistry,
            runContext,
            eventBridgeHandler
        );

        assertMultiTenantTasks(registry.all());
    }

    public void testMultiTenantRegistryContainsExpectedTasksWhenForecastMultiTenancyEnabled() {
        ClusterManagerTaskRegistry registry = new ClusterManagerTaskRegistry(
            mock(ClusterService.class),
            threadPool,
            client,
            clock,
            dataAccess,
            nodeFilter,
            forecastMultiTenantSettings(),
            hashRing,
            stateManager,
            xContentRegistry,
            runContext,
            eventBridgeHandler
        );

        assertMultiTenantTasks(registry.all());
    }

    private void assertMultiTenantTasks(List<ClusterManagerTask> tasks) {
        assertEquals(4, tasks.size());
        assertTrue(tasks.stream().anyMatch(MultiTenantMaintenanceScheduler.class::isInstance));
        assertTrue(tasks.stream().anyMatch(CloudMapWatcherTask.class::isInstance));
        assertTrue(tasks.stream().anyMatch(ADSQSConsumerTask.class::isInstance));
        assertTrue(tasks.stream().anyMatch(ADClusterMembershipReaderTask.class::isInstance));
        assertFalse(tasks.stream().anyMatch(ClusterManagerEventListener.class::isInstance));
    }

    private Settings singleTenantSettings() {
        return Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), false)
            .put(ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.getKey(), false)
            .build();
    }

    private Settings adMultiTenantSettings() {
        return Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), true)
            .put(ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.getKey(), false)
            .build();
    }

    private Settings forecastMultiTenantSettings() {
        return Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), false)
            .put(ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.getKey(), true)
            .build();
    }
}
