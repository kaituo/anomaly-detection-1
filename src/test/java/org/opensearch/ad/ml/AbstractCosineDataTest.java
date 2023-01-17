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

package org.opensearch.ad.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.dataprocessor.LinearUniformImputer;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.ml.CheckpointDao;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.ImmutableList;

public class AbstractCosineDataTest extends AbstractTimeSeriesTest {
    int numMinSamples;
    String modelId;
    String entityName;
    String detectorId;
    ADModelState<createFromValueOnlySamples<ThresholdedRandomCutForest>> modelState;
    Clock clock;
    float priority;
    ADEntityColdStart entityColdStarter;
    ADNodeStateManager stateManager;
    SearchFeatureDao searchFeatureDao;
    Imputer imputer;
    CheckpointDao checkpoint;
    FeatureManager featureManager;
    Settings settings;
    ThreadPool threadPool;
    AtomicBoolean released;
    Runnable releaseSemaphore;
    ActionListener<Void> listener;
    CountDownLatch inProgressLatch;
    ADCheckpointWriteWorker checkpointWriteQueue;
    Entity entity;
    AnomalyDetector detector;
    long rcfSeed;
    ADModelManager modelManager;
    ClientUtil clientUtil;
    ClusterService clusterService;
    ClusterSettings clusterSettings;
    DiscoveryNode discoveryNode;
    Set<Setting<?>> nodestateSetting;

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        numMinSamples = TimeSeriesSettings.NUM_MIN_SAMPLES;

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        threadPool = mock(ThreadPool.class);
        setUpADThreadPool(threadPool);

        settings = Settings.EMPTY;

        Client client = mock(Client.class);
        clientUtil = mock(ClientUtil.class);

        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance()
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .build();
        when(clock.millis()).thenReturn(1602401500000L);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, CommonName.CONFIG_INDEX));

            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        nodestateSetting = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        nodestateSetting.add(TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE);
        nodestateSetting.add(TimeSeriesSettings.BACKOFF_MINUTES);
        nodestateSetting.add(CHECKPOINT_SAVING_FREQ);
        clusterSettings = new ClusterSettings(Settings.EMPTY, nodestateSetting);

        discoveryNode = new DiscoveryNode(
            "node1",
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);

        stateManager = new ADNodeStateManager(
            client,
            xContentRegistry(),
            settings,
            clientUtil,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            clusterService,
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES
        );

        imputer = new LinearUniformImputer(true);

        searchFeatureDao = mock(SearchFeatureDao.class);
        checkpoint = mock(ADCheckpointDao.class);

        featureManager = new FeatureManager(
            searchFeatureDao,
            imputer,
            clock,
            AnomalyDetectorSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
            AnomalyDetectorSettings.MIN_TRAIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
            AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
            AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
            AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            threadPool,
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME
        );

        checkpointWriteQueue = mock(ADCheckpointWriteWorker.class);

        rcfSeed = 2051L;
        entityColdStarter = new ADEntityColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.TIME_DECAY,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            imputer,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            settings,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS
        );

        detectorId = "123";
        modelId = "123_entity_abc";
        entityName = "abc";
        priority = 0.3f;
        entity = Entity.createSingleAttributeEntity("field", entityName);

        released = new AtomicBoolean();

        inProgressLatch = new CountDownLatch(1);
        releaseSemaphore = () -> {
            released.set(true);
            inProgressLatch.countDown();
        };
        listener = ActionListener.wrap(releaseSemaphore);

        modelManager = new ADModelManager(
            mock(ADCheckpointDao.class),
            mock(Clock.class),
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.TIME_DECAY,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ,
            entityColdStarter,
            mock(FeatureManager.class),
            mock(ADMemoryTracker.class),
            settings,
            clusterService
        );
    }

    protected void checkSemaphoreRelease() throws InterruptedException {
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        assertTrue(released.get());
    }

    public int searchInsert(long[] timestamps, long target) {
        int pivot, left = 0, right = timestamps.length - 1;
        while (left <= right) {
            pivot = left + (right - left) / 2;
            if (timestamps[pivot] == target)
                return pivot;
            if (target < timestamps[pivot])
                right = pivot - 1;
            else
                left = pivot + 1;
        }
        return left;
    }
}
