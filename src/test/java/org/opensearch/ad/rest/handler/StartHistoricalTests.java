/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.timeseries.TestHelpers.randomDetector;
import static org.opensearch.timeseries.TestHelpers.randomFeature;
import static org.opensearch.timeseries.TestHelpers.randomUser;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.ad.ADTaskProfileRunner;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.ThreadRunContext;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableList;

public class StartHistoricalTests extends AbstractTimeSeriesTest {
    private static ADDelegatingDataManagement anomalyDetectionIndices;
    private static NamedXContentRegistry xContentRegistry;
    private static DiscoveryNodeFilterer nodeFilter;

    private NodeStateManager nodeStateManager;
    private Client client;
    private RunContext.RestorableContext context;
    private DateRange detectionDateRange;
    private TransportService transportService;
    private ADIndexJobActionHandler handler;
    private ADTaskManager adTaskManager;
    private ADTaskCacheManager adTaskCacheManager;
    private HashRing hashRing;
    private ADTaskProfileRunner taskProfileRunner;
    private DiscoveryNode node1;
    private ActionListener<JobResponse> listener;
    private Clock clock;

    @BeforeClass
    public static void setOnce() throws IOException {
        setUpThreadPool(StartHistoricalTests.class.getSimpleName());
        anomalyDetectionIndices = mock(ADDelegatingDataManagement.class);
        xContentRegistry = NamedXContentRegistry.EMPTY;
        when(anomalyDetectionIndices.doesJobIndexExist()).thenReturn(true);
        // make sure getAndExecuteOnLatestConfigLevelTask called in startConfig
        when(anomalyDetectionIndices.doesStateIndexExist()).thenReturn(true);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
    }

    @AfterClass
    public static void terminate() {
        tearDownThreadPool();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        node1 = createDiscoverynode("node1");

        Set<Setting<?>> nodestateSetting = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        nodestateSetting.add(TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE);
        nodestateSetting.add(TimeSeriesSettings.BACKOFF_MINUTES);
        nodestateSetting.add(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ);
        nodestateSetting.add(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR);
        nodestateSetting.add(BATCH_TASK_PIECE_INTERVAL_SECONDS);
        nodestateSetting.add(AD_REQUEST_TIMEOUT);
        nodestateSetting.add(DELETE_AD_RESULT_WHEN_DELETE_DETECTOR);
        nodestateSetting.add(MAX_BATCH_TASK_PER_NODE);
        nodestateSetting.add(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS);

        ClusterService clusterService = createClusterServiceForNode(threadPool, node1, nodestateSetting);
        nodeStateManager = mock(NodeStateManager.class);
        Instant now = Instant.now();
        Instant startTime = now.minus(10, ChronoUnit.DAYS);
        Instant endTime = now.minus(1, ChronoUnit.DAYS);
        detectionDateRange = new DateRange(startTime, endTime);

        Settings settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .build();
        context = () -> {};
        transportService = mock(TransportService.class);

        hashRing = mock(HashRing.class);
        taskProfileRunner = new ADTaskProfileRunner(hashRing, client);
        adTaskCacheManager = mock(ADTaskCacheManager.class);
        DataAccess taskSearcher = mock(DataAccess.class);
        adTaskManager = spy(
            new ADTaskManager(
                settings,
                clusterService,
                client,
                TestHelpers.xContentRegistry(),
                nodeFilter,
                hashRing,
                adTaskCacheManager,
                threadPool,
                nodeStateManager,
                taskSearcher,
                anomalyDetectionIndices,
                taskProfileRunner
            )
        );

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<JobResponse> listener = (ActionListener<JobResponse>) args[4];

            JobResponse response = mock(JobResponse.class);
            listener.onResponse(response);

            return null;
        }).when(adTaskManager).getAndExecuteOnLatestConfigLevelTask(any(), anyString(), any(), eq(false), any(), any(), any());

        clock = mock(Clock.class);

        ExecuteADResultResponseRecorder recorder = mock(ExecuteADResultResponseRecorder.class);
        RunContext runContext = new ThreadRunContext(threadPool.getThreadContext());

        handler = new ADIndexJobActionHandler(
            client,
            anomalyDetectionIndices,
            xContentRegistry,
            adTaskManager,
            recorder,
            nodeStateManager,
            Settings.EMPTY,
            runContext
        );

        listener = spy(new ActionListener<JobResponse>() {
            @Override
            public void onResponse(JobResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });
    }

    public void testStartDetectorWithNoEnabledFeature() throws IOException {
        AnomalyDetector detector = randomDetector(
            ImmutableList.of(randomFeature(false)),
            randomAlphaOfLength(5),
            1,
            randomAlphaOfLength(5)
        );
        mockGetConfig(detector);

        handler
            .startConfig(
                detector.getId(),
                detector.getTenantId(),
                detectionDateRange,
                randomUser(),
                transportService,
                context,
                clock,
                listener
            );
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
    }

    private void setupHashRingWithOwningNode() {
        doAnswer(invocation -> {
            Consumer<Optional<DiscoveryNode>> function = invocation.getArgument(1);
            function.accept(Optional.of(node1));
            return null;
        }).when(hashRing).buildAndGetOwningNodeWithSameLocalVersion(any(), any(), any());
    }

    public void testStartDetectorForHistoricalAnalysis() throws IOException {
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        mockGetConfig(detector);
        setupHashRingWithOwningNode();

        handler
            .startConfig(
                detector.getId(),
                detector.getTenantId(),
                detectionDateRange,
                randomUser(),
                transportService,
                context,
                clock,
                listener
            );
        verify(adTaskManager, times(1)).forwardRequestToLeadNode(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private void mockGetConfig(AnomalyDetector detector) {
        doAnswer(invocation -> {
            Consumer<Optional<? extends Config>> function = invocation.getArgument(3);
            function.accept(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(eq(detector.getId()), eq(detector.getTenantId()), eq(AnalysisType.AD), any(), any());
    }
}
