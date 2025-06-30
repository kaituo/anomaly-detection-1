/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.cluster;

import java.time.Clock;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.rest.handler.EventBridgeHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

/**
 * Schedules hourly maintenance tasks via EventBridge for multi-tenant mode.
 * This class is only active on master nodes and creates an EventBridge schedule
 * that triggers every hour to perform maintenance operations like checkpoint cleanup.
 * 
 * If eventBridgeHandler is null (e.g., when the node doesn't have coordinator role),
 * this scheduler will not initialize and will be effectively disabled.
 */
public class MultiTenantMaintenanceScheduler implements ClusterManagerTask {

    private static final Logger LOG = LogManager.getLogger(MultiTenantMaintenanceScheduler.class);

    private final EventBridgeHandler eventBridgeHandler;
    private boolean initialized = false;

    /**
     * Creates a new MultiTenantMaintenanceScheduler.
     * 
     * @param eventBridgeHandler the EventBridge handler for scheduling, can be null 
     *                           if the node doesn't have the coordinator role
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

        LOG.info("Node became cluster manager, starting hourly cron schedule via EventBridge");
        eventBridgeHandler.startHourlyCron(ActionListener.wrap(
            response -> LOG.info("Successfully started hourly cron schedule: {}", response.getId()),
            e -> LOG.error("Failed to start hourly cron schedule via EventBridge", e)
        ));

        LOG.info("Starting daily S3 checkpoint cleanup schedule via EventBridge");
        eventBridgeHandler.startDailyS3CheckpointCleanup(ActionListener.wrap(
            response -> LOG.info("Successfully started daily S3 checkpoint cleanup schedule: {}", response.getId()),
            e -> LOG.error("Failed to start daily S3 checkpoint cleanup schedule via EventBridge", e)
        ));
    }

    @Override
    public void offClusterManager() {
        if (!initialized) {
            return;
        }

        LOG.info("Node stopped being cluster manager, stopping hourly cron schedule via EventBridge");
        eventBridgeHandler.stopHourlyCron(ActionListener.wrap(
            response -> LOG.info("Successfully stopped hourly cron schedule: {}", response.getId()),
            e -> LOG.error("Failed to stop hourly cron schedule via EventBridge", e)
        ));

        LOG.info("Stopping daily S3 checkpoint cleanup schedule via EventBridge");
        eventBridgeHandler.stopDailyS3CheckpointCleanup(ActionListener.wrap(
            response -> LOG.info("Successfully stopped daily S3 checkpoint cleanup schedule: {}", response.getId()),
            e -> LOG.error("Failed to stop daily S3 checkpoint cleanup schedule via EventBridge", e)
        ));
    }
}
