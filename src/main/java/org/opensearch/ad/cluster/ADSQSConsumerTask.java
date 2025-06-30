/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.cluster;

import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ADCheckpointStore;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.sqs.ADEventBridgeTargetResolver;
import org.opensearch.ad.sqs.ADSQSService;
import org.opensearch.ad.sqs.ADSqsAccountTarget;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.DataPlaneClientFactory;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.cluster.SQSConsumerTask;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

/**
 * AD-specific SQS Consumer Task that polls one queue per discovered EventBridge account.
 */
public class ADSQSConsumerTask extends SQSConsumerTask {

    private static final Logger logger = LogManager.getLogger(ADSQSConsumerTask.class);

    private final HashRing hashRing;
    private final DataPlaneClientFactory configuredSqsSigningFactory;
    private final AtomicBoolean waitingForRealtimeHashRing = new AtomicBoolean(false);
    private final JobQueueAccountIdProvider accountProvider;
    private final ADEventBridgeTargetResolver targetResolver;
    private final Map<String, QueueContext> queueContexts = new ConcurrentHashMap<>();
    private volatile TimeValue accountReconcileInterval;
    private volatile Cancellable reconcileCron;

    public ADSQSConsumerTask(
        StateManager nodeStateManager,
        HashRing hashRing,
        ADCheckpointStore checkpointStore,
        JobQueueAccountIdProvider accountProvider,
        ADEventBridgeTargetResolver targetResolver
    ) {
        this(nodeStateManager, hashRing, checkpointStore, accountProvider, targetResolver, null);
    }

    public ADSQSConsumerTask(
        StateManager nodeStateManager,
        HashRing hashRing,
        ADCheckpointStore checkpointStore,
        JobQueueAccountIdProvider accountProvider,
        ADEventBridgeTargetResolver targetResolver,
        DataPlaneClientFactory sqsSigningFactory
    ) {
        super(nodeStateManager, checkpointStore);
        this.hashRing = hashRing;
        this.accountProvider = accountProvider;
        this.targetResolver = targetResolver;
        this.configuredSqsSigningFactory = sqsSigningFactory;
    }

    @Override
    public void init(
        ClusterService cs,
        ThreadPool pool,
        Clock clock,
        DiscoveryNodeSelector filterer,
        Settings settings,
        DataAccess dataAccess
    ) {
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE)) {
            return;
        }

        logger.info("Initializing AD SQS Consumer Task");

        TimeValue pollingInterval = AnomalyDetectorSettings.SQS_POLLING_INTERVAL.get(settings);
        TimeValue accountReconcileInterval = AnomalyDetectorSettings.SQS_ACCOUNT_RECONCILE_INTERVAL.get(settings);
        int maxConcurrentProcessors = AnomalyDetectorSettings.SQS_MAX_CONCURRENT_PROCESSORS.get(settings);

        super.initWithSQSConfig(
            cs,
            pool,
            clock,
            filterer,
            settings,
            pollingInterval,
            maxConcurrentProcessors,
            ADCommonName.AD_COORDINATOR_THREAD_POOL_NAME,
            configuredSqsSigningFactory
        );

        this.maxMessages = AnomalyDetectorSettings.SQS_MAX_MESSAGES.get(settings);
        this.waitTime = AnomalyDetectorSettings.SQS_WAIT_TIME.get(settings);
        this.visibilityTimeout = AnomalyDetectorSettings.SQS_VISIBILITY_TIMEOUT.get(settings);
        this.accountReconcileInterval = accountReconcileInterval;

        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_POLLING_INTERVAL, value -> {
            this.pollingInterval = value;
            restart();
        });
        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_ACCOUNT_RECONCILE_INTERVAL, value -> {
            this.accountReconcileInterval = value;
            restartReconcileCron();
        });
        cs.getClusterSettings().addSettingsUpdateConsumer(TimeSeriesSettings.SQS_ACCOUNT_IDS, value -> {
            if (isClusterManagerNode()) {
                pool.schedule(this::safeReconcileQueueContexts, TimeValue.ZERO, CommonName.SQS_CONSUMER_THREAD_POOL_NAME);
            }
        });
        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_MAX_CONCURRENT_PROCESSORS, value -> {
            this.concurrencySemaphore = new Semaphore(value);
        });
        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_MAX_MESSAGES, value -> this.maxMessages = value);
        cs.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_WAIT_TIME, value -> this.waitTime = value);
        cs
            .getClusterSettings()
            .addSettingsUpdateConsumer(AnomalyDetectorSettings.SQS_VISIBILITY_TIMEOUT, value -> this.visibilityTimeout = value);
    }

    @Override
    protected void restart() {
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE) || !isClusterManagerNode()) {
            return;
        }

        safeReconcileQueueContexts(true);
        restartReconcileCron();
    }

    private void restartReconcileCron() {
        List<String> roles = TimeSeriesSettings.NODE_ROLE.get(settings);
        if (!roles.contains(TimeSeriesSettings.COORDINATOR_ROLE) || !isClusterManagerNode()) {
            return;
        }

        cancel(reconcileCron);
        reconcileCron = pool
            .scheduleWithFixedDelay(this::safeReconcileQueueContexts, accountReconcileInterval, CommonName.SQS_CONSUMER_THREAD_POOL_NAME);
    }

    @Override
    protected void stopPolling() {
        cancel(reconcileCron);
        reconcileCron = null;
        queueContexts.values().forEach(this::closeQueueContext);
        queueContexts.clear();
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

    int getPolledQueueCount() {
        return queueContexts.size();
    }

    Set<String> getPolledAccountIds() {
        return Set.copyOf(queueContexts.keySet());
    }

    private void safeReconcileQueueContexts() {
        safeReconcileQueueContexts(false);
    }

    private void safeReconcileQueueContexts(boolean rescheduleExistingPollers) {
        try {
            reconcileQueueContexts(rescheduleExistingPollers);
        } catch (Exception e) {
            logger.error("Failed to reconcile AD SQS queue pollers", e);
        }
    }

    private void reconcileQueueContexts(boolean rescheduleExistingPollers) {
        List<String> discoveredAccounts = getDiscoveredAccounts();
        Set<String> desiredAccounts = new LinkedHashSet<>(discoveredAccounts);

        queueContexts.entrySet().removeIf(entry -> {
            if (!desiredAccounts.contains(entry.getKey())) {
                logger.info("Stopping AD SQS poller for removed account {}", entry.getKey());
                closeQueueContext(entry.getValue());
                return true;
            }
            return false;
        });

        for (String accountId : discoveredAccounts) {
            QueueContext existingContext = queueContexts.get(accountId);
            boolean created = false;
            if (existingContext == null) {
                QueueContext newContext = createQueueContext(accountId);
                QueueContext raced = queueContexts.putIfAbsent(accountId, newContext);
                if (raced == null) {
                    logger.info("Starting AD SQS poller for account {}", accountId);
                    newContext.schedulePoller();
                    created = true;
                } else {
                    closeQueueContext(newContext);
                    existingContext = raced;
                }
            }

            if (rescheduleExistingPollers && !created) {
                QueueContext context = queueContexts.get(accountId);
                if (context != null) {
                    context.reschedulePoller();
                }
            }
        }
    }

    private List<String> getDiscoveredAccounts() {
        return new LinkedHashSet<>(accountProvider.getAccountIds()).stream().sorted().collect(Collectors.toList());
    }

    private QueueContext createQueueContext(String accountId) {
        ADSqsAccountTarget target = targetResolver.resolveAccount(accountId);
        return new QueueContext(
            accountId,
            new ADSQSService(settings, target.getQueueUrl(), target.getScheduleManagementRoleArn(), "ad-sqs-consumer-" + accountId),
            createQueuePollingState()
        );
    }

    private void closeQueueContext(QueueContext context) {
        cancel(context.poller);
        context.poller = null;
        context.sqsService.close();
    }

    private final class QueueContext {
        private final String accountId;
        private final ADSQSService sqsService;
        private final QueuePollingState state;
        private volatile Cancellable poller;

        private QueueContext(String accountId, ADSQSService sqsService, QueuePollingState state) {
            this.accountId = accountId;
            this.sqsService = sqsService;
            this.state = state;
        }

        private void schedulePoller() {
            cancel(poller);
            poller = scheduleQueuePoller(sqsService, state, CommonName.SQS_CONSUMER_THREAD_POOL_NAME);
        }

        private void reschedulePoller() {
            logger.debug("Rescheduling AD SQS poller for account {}", accountId);
            schedulePoller();
        }
    }
}
