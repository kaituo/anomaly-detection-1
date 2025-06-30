/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster;

import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.cluster.ADClusterMembershipReaderTask;
import org.opensearch.ad.cluster.ADSQSConsumerTask;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.annotation.SuppressForbidden;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.Client;

/*
 * Instead of SPI, ClusterManagerTaskRegistry is of program‑to‑interface + explicit wiring style.
 * Java SPI shines when third‑party JARs can appear (JDBC drivers, logging back‑ends, etc.).
 * Since we already ship the concrete classes ourselves, SPI only adds indirection and class‑loader headaches.
 */
@SuppressForbidden(reason = "org.opensearch.transport.client.Client usage: Pass parameter for single-tenant-only components.")
public final class ClusterManagerTaskRegistry {
    private static final Logger LOG = LogManager.getLogger(ClusterManagerTaskRegistry.class);

    private final List<ClusterManagerTask> tasks;

    public ClusterManagerTaskRegistry(
        ClusterService cs,
        ThreadPool pool,
        Client client,
        Clock clock,
        DataAccess dataAccess,
        DiscoveryNodeSelector filterer,
        Settings settings,
        HashRing hashRing,
        StateManager nodeStateManager,
        NamedXContentRegistry xContentRegistry,
        RunContext runContext,
        EventBridgeHandler eventBridgeHandler
    ) {
        boolean multiTenancyEnabled = AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.get(settings)
            || ForecastSettings.FORECAST_MULTI_TENANCY_ENABLED.get(settings);
        boolean localMetadataStoreEnabled = TimeSeriesSettings.LOCAL_METADATA_STORE_ENABLED.get(settings);

        if (multiTenancyEnabled) {
            if (localMetadataStoreEnabled) {
                tasks = List.of(new ADClusterMembershipReaderTask(hashRing));
            } else {
                tasks = List
                    .of(
                        new MultiTenantMaintenanceScheduler(eventBridgeHandler), // hourly maintenance (multi-tenant)
                        new CloudMapWatcherTask(), // cloud-map watcher
                        new ADSQSConsumerTask(nodeStateManager, hashRing), // SQS consumer
                        new ADClusterMembershipReaderTask(hashRing) // ad cluster membership reader
                    );
            }
        } else {
            tasks = List.of(new ClusterManagerEventListener(client, runContext)); // checkpoint + hourly jobs (single-tenant)
        }

        LOG
            .info(
                "Initialize ClusterManagerTaskRegistry. multiTenancyEnabled: {}, tasks: {}",
                multiTenancyEnabled,
                tasks.stream().map(t -> t.getClass().getSimpleName()).collect(Collectors.toList())
            );

        // initialise and let each task register itself as a listener
        tasks.forEach(t -> t.init(cs, pool, clock, filterer, settings, dataAccess));
    }

    public List<ClusterManagerTask> all() {
        return tasks;
    }
}
