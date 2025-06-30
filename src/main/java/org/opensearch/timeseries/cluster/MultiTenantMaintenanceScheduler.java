/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster;

import java.time.Clock;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DateUtils;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

/**
 * Schedules hourly maintenance tasks via EventBridge for multi-tenant mode.
 * This class is only active on master nodes and creates an EventBridge schedule
 * that triggers every hour to perform maintenance operations like checkpoint cleanup.
 * 
 * If eventBridgeHandler is null, this scheduler will not initialize and will be
 * effectively disabled.
 */
public class MultiTenantMaintenanceScheduler implements ClusterManagerTask {

    private static final Logger LOG = LogManager.getLogger(MultiTenantMaintenanceScheduler.class);

    private final EventBridgeHandler eventBridgeHandler;
    private boolean initialized = false;
    private boolean clusterManager = false;

    /**
     * Creates a new MultiTenantMaintenanceScheduler.
     * 
     * @param eventBridgeHandler the EventBridge handler for scheduling, can be null
     *                           if EventBridge support is not enabled on this node
     */
    public MultiTenantMaintenanceScheduler(EventBridgeHandler eventBridgeHandler) {
        this.eventBridgeHandler = eventBridgeHandler;
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
        if (eventBridgeHandler == null) {
            LOG.info("Skipping MultiTenantMaintenanceScheduler initialization - eventBridgeHandler is null");
            return;
        }

        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.MASTER_ROLE)) {
            LOG.info("Skipping MultiTenantMaintenanceScheduler initialization - node does not have MASTER_ROLE");
            return;
        }

        eventBridgeHandler.setMaintenanceScheduleInterval(DateUtils.toDuration(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ.get(settings)));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ, it -> {
            eventBridgeHandler.setMaintenanceScheduleInterval(DateUtils.toDuration(it));
            if (clusterManager) {
                refreshMaintenanceSchedule();
            }
        });
        eventBridgeHandler.setDailyS3CleanupInterval(DateUtils.toDuration(AnomalyDetectorSettings.AD_DAILY_S3_CLEANUP_INTERVAL.get(settings)));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.AD_DAILY_S3_CLEANUP_INTERVAL, it -> {
            eventBridgeHandler.setDailyS3CleanupInterval(DateUtils.toDuration(it));
            if (clusterManager) {
                refreshDailyS3CleanupSchedule();
            }
        });

        this.initialized = true;

        // Register as cluster manager listener to be notified when node becomes/stops being master
        clusterService.addLocalNodeClusterManagerListener(this);

        LOG.info("MultiTenantMaintenanceScheduler initialized for master node");
    }

    @Override
    public void onClusterManager() {
        if (!initialized) {
            return;
        }

        clusterManager = true;
        LOG.info("Node became cluster manager, starting maintenance schedule via EventBridge");
        refreshMaintenanceSchedule();

        LOG.info("Starting daily S3 checkpoint cleanup schedule via EventBridge");
        refreshDailyS3CleanupSchedule();
    }

    @Override
    public void offClusterManager() {
        if (!initialized) {
            return;
        }

        clusterManager = false;
        LOG.info("Node stopped being cluster manager, stopping maintenance schedule via EventBridge");
        eventBridgeHandler
            .stopHourlyCron(
                ActionListener
                    .wrap(
                        response -> LOG.info("Successfully stopped maintenance schedule: {}", response.getId()),
                        e -> LOG.error("Failed to stop maintenance schedule via EventBridge", e)
                    )
            );

        LOG.info("Stopping daily S3 checkpoint cleanup schedule via EventBridge");
        eventBridgeHandler
            .stopDailyS3CheckpointCleanup(
                ActionListener
                    .wrap(
                        response -> LOG.info("Successfully stopped daily S3 checkpoint cleanup schedule: {}", response.getId()),
                        e -> LOG.error("Failed to stop daily S3 checkpoint cleanup schedule via EventBridge", e)
                    )
            );
    }

    private void refreshMaintenanceSchedule() {
        eventBridgeHandler
            .startHourlyCron(
                ActionListener
                    .wrap(
                        response -> LOG.info("Successfully started maintenance schedule: {}", response.getId()),
                        e -> LOG.error("Failed to start maintenance schedule via EventBridge", e)
                    )
            );
    }

    private void refreshDailyS3CleanupSchedule() {
        eventBridgeHandler
            .startDailyS3CheckpointCleanup(
                ActionListener
                    .wrap(
                        response -> LOG.info("Successfully started daily S3 checkpoint cleanup schedule: {}", response.getId()),
                        e -> LOG.error("Failed to start daily S3 checkpoint cleanup schedule via EventBridge", e)
                    )
            );
    }
}
