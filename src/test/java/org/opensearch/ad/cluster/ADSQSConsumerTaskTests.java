/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.cluster;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ADCheckpointStore;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.sqs.ADEventBridgeTargetResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.StateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;

public class ADSQSConsumerTaskTests extends AbstractTimeSeriesTest {

    public void testOnClusterManagerSchedulesReconcileCronWhenProviderThrows() {
        Settings settings = Settings
            .builder()
            .putList(TimeSeriesSettings.NODE_ROLE.getKey(), TimeSeriesSettings.COORDINATOR_ROLE)
            .put(AnomalyDetectorSettings.SQS_ACCOUNT_RECONCILE_INTERVAL.getKey(), TimeValue.timeValueSeconds(30))
            .build();

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections.unmodifiableSet(new HashSet<>(new TimeSeriesAnalyticsPlugin().getSettings()))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ADCommonName.AD_COORDINATOR_THREAD_POOL_NAME)).thenReturn(mock(ExecutorService.class));
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), eq(CommonName.SQS_CONSUMER_THREAD_POOL_NAME)))
            .thenReturn(mock(Cancellable.class));

        JobQueueAccountIdProvider accountProvider = mock(JobQueueAccountIdProvider.class);
        when(accountProvider.getAccountIds()).thenThrow(new IllegalStateException("No job queue accounts discovered"));

        ADSQSConsumerTask task = new ADSQSConsumerTask(
            mock(StateManager.class),
            mock(HashRing.class),
            mock(ADCheckpointStore.class),
            accountProvider,
            mock(ADEventBridgeTargetResolver.class)
        );
        task.init(clusterService, threadPool, Clock.systemUTC(), mock(DiscoveryNodeSelector.class), settings, mock(DataAccess.class));

        task.onClusterManager();

        verify(threadPool)
            .scheduleWithFixedDelay(any(Runnable.class), eq(TimeValue.timeValueSeconds(30)), eq(CommonName.SQS_CONSUMER_THREAD_POOL_NAME));
    }
}
