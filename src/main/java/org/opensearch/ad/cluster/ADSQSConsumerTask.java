/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.cluster;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.sqs.ADSQSService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.cluster.SQSConsumerTask;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

/**
 * AD-specific SQS Consumer Task that extends the base SQSConsumerTask
 * and initializes all AD-specific settings.
 */
public class ADSQSConsumerTask extends SQSConsumerTask {

    private static final Logger logger = LogManager.getLogger(ADSQSConsumerTask.class);
    private final HashRing hashRing;
    private final AtomicBoolean waitingForRealtimeHashRing = new AtomicBoolean(false);

    public ADSQSConsumerTask(StateManager nodeStateManager, HashRing hashRing) {
        super(nodeStateManager);
        this.hashRing = hashRing;
    }

    @Override
    public void init(ClusterService cs, ThreadPool pool, Clock clock, DiscoveryNodeSelector f, Settings settings, DataAccess dataAccess) {
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE)) {
            return;
        }

        logger.info("Initializing AD SQS Consumer Task");

        // Get AD-specific SQS configuration
        TimeValue pollingInterval = AnomalyDetectorSettings.SQS_POLLING_INTERVAL.get(settings);
        // the number of concurrent SQS messages processed.
        // We need this setting to decide how many messages to request from SQS
        int maxConcurrentProcessors = AnomalyDetectorSettings.SQS_MAX_CONCURRENT_PROCESSORS.get(settings);

        // Initialize parent with AD-specific configuration
        super.initWithSQSConfig(
            cs,
            pool,
            clock,
            f,
            settings,
            pollingInterval,
            maxConcurrentProcessors,
            ADCommonName.AD_COORDINATOR_THREAD_POOL_NAME,
            new ADSQSService(settings)
        );

        // Set initial values in parent
        this.maxMessages = AnomalyDetectorSettings.SQS_MAX_MESSAGES.get(settings);
        this.waitTime = AnomalyDetectorSettings.SQS_WAIT_TIME.get(settings);
        this.visibilityTimeout = AnomalyDetectorSettings.SQS_VISIBILITY_TIMEOUT.get(settings);

        // Listen for AD-specific settings changes
        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_POLLING_INTERVAL, v -> {
            // Update polling interval and restart
            this.pollingInterval = v;
            restart();
        });

        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_MAX_CONCURRENT_PROCESSORS, v -> {
            // Recreate semaphore with new size
            this.concurrencySemaphore = new Semaphore(v);
        });

        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_MAX_MESSAGES, v -> { this.maxMessages = v; });

        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_WAIT_TIME, v -> { this.waitTime = v; });

        cs
            .getClusterSettings()
            .addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_VISIBILITY_TIMEOUT, v -> { this.visibilityTimeout = v; });
    }

    @Override
    protected boolean isReadyForPolling() {
        hashRing.buildCirclesForRealtime();
        if (!hashRing.hasRealtimeHashRing()) {
            if (waitingForRealtimeHashRing.compareAndSet(false, true)) {
                logger.info("Realtime hash ring is empty. Waiting before consuming SQS messages.");
            }
            return false;
        }

        if (waitingForRealtimeHashRing.compareAndSet(true, false)) {
            logger.info("Realtime hash ring is ready. Resuming SQS consumption.");
        }
        return true;
    }
}
