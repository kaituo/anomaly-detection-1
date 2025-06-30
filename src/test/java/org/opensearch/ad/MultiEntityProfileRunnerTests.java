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

package org.opensearch.ad;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyResultTests;
import org.opensearch.ad.util.*;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.client.AwsSigV4ThreadContext;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.NodeCommunicator;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.ConfigProfile;
import org.opensearch.timeseries.model.ConfigState;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.transport.ProfileNodeResponse;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class MultiEntityProfileRunnerTests extends AbstractTimeSeriesTest {
    private AnomalyDetectorProfileRunner runner;
    private Client client;
    private NodeCommunicator nodeCommunicator;
    private DataAccess dataAccess;
    private DiscoveryNodeFilterer nodeFilter;
    private int requiredSamples;
    private AnomalyDetector detector;
    private String detectorId;
    private Set<ProfileName> stateNError;
    private DetectorInternalState.Builder result;
    private String node1;
    private String nodeName1;
    private DiscoveryNode discoveryNode1;

    private String node2;
    private String nodeName2;
    private DiscoveryNode discoveryNode2;

    private long modelSize;
    private String model1Id;
    private String model0Id;

    private int shingleSize;
    private Job job;
    private TransportService transportService;
    private ADTaskManager adTaskManager;
    private ADTaskProfileRunner taskProfileRunner;
    private NodeStateManager stateManager;

    enum InittedEverResultStatus {
        INITTED,
        NOT_INITTED,
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings("unchecked")
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        taskProfileRunner = mock(ADTaskProfileRunner.class);
        nodeCommunicator = mock(NodeCommunicator.class);
        dataAccess = mock(DataAccess.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        requiredSamples = 128;

        detectorId = "A69pa3UBHuCbh-emo9oR";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a"));
        result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now());
        job = TestHelpers.randomJob(true);
        adTaskManager = mock(ADTaskManager.class);
        stateManager = mock(NodeStateManager.class);
        when(adTaskManager.getStateManager()).thenReturn(stateManager);
        when(nodeFilter.getEligibleDataNodes()).thenReturn(new DiscoveryNode[0]);
        transportService = mock(TransportService.class);
        doAnswer(invocation -> {
            Consumer<Optional<ADTask>> function = invocation.getArgument(3);
            function.accept(Optional.empty());
            return null;
        }).when(adTaskManager).getAndExecuteOnLatestConfigLevelTask(anyString(), any(), any(), any(), any(), anyBoolean(), any());
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(4);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getConfig(anyString(), any(), eq(AnalysisType.AD), anyBoolean(), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<Optional<Job>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(job));
            return null;
        }).when(stateManager).getJob(anyString(), any(), anyBoolean(), any(ActionListener.class));
        runner = new AnomalyDetectorProfileRunner(
            nodeCommunicator,
            xContentRegistry(),
            nodeFilter,
            requiredSamples,
            transportService,
            adTaskManager,
            taskProfileRunner,
            dataAccess
        );

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ADCommonName.CONFIG_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), ADCommonName.CONFIG_INDEX));
            } else if (indexName.equals(ADCommonName.DETECTION_STATE_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(result.build(), detector.getId(), ADCommonName.DETECTION_STATE_INDEX));
            } else if (indexName.equals(CommonName.JOB_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(job, detector.getId(), CommonName.JOB_INDEX));
            }

            return null;
        }).when(client).get(any(), any());

        stateNError = new HashSet<ProfileName>();
        stateNError.add(ProfileName.ERROR);
        stateNError.add(ProfileName.STATE);
    }

    @SuppressWarnings("unchecked")
    public void testProfileRestoresRequestThreadContextBeforeFollowOnSearches() throws InterruptedException {
        ThreadContext threadContext = threadPool.getThreadContext();
        String accessKey = "request-access";
        AtomicReference<Object> totalEntitiesAccessKey = new AtomicReference<>();
        AtomicReference<Object> realtimeResultAccessKey = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        when(transportService.getThreadPool()).thenReturn(threadPool);
        doAnswer(invocation -> {
            ActionListener<Optional<Job>> listener = invocation.getArgument(3);
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                listener.onResponse(Optional.of(job));
            }
            return null;
        }).when(stateManager).getJob(anyString(), any(), anyBoolean(), any(ActionListener.class));
        doAnswer(invocation -> {
            totalEntitiesAccessKey.set(threadContext.getTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY));
            ActionListener<SearchResponse> listener = invocation.getArgument(4);
            listener.onFailure(new RuntimeException("stop after capturing total-entities context"));
            return null;
        }).when(dataAccess).searchWithInjectedSecurity(any(SearchRequest.class), anyString(), any(), any(), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<ProfileResponse> listener = invocation.getArgument(1);
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                listener
                    .onResponse(
                        new ProfileResponse(new ClusterName("test-cluster-name"), Collections.emptyList(), Collections.emptyList())
                    );
            }
            return null;
        }).when(nodeCommunicator).profile(any(), any());
        doAnswer(invocation -> {
            realtimeResultAccessKey.set(threadContext.getTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY));
            ActionListener<SearchResponse> listener = invocation.getArgument(2);
            SearchResponse response = mock(SearchResponse.class);
            when(response.getHits()).thenReturn(TestHelpers.createSearchHits(0));
            listener.onResponse(response);
            return null;
        }).when(dataAccess).search(any(SearchRequest.class), any(), any(ActionListener.class));

        try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(false)) {
            threadContext.putTransient(AwsSigV4ThreadContext.AWS_ACCESS_KEY_CONTEXT_KEY, accessKey);
            runner
                .profile(
                    detectorId,
                    null,
                    ActionListener.wrap(response -> latch.countDown(), exception -> latch.countDown()),
                    new HashSet<>(Arrays.asList(ProfileName.TOTAL_ENTITIES, ProfileName.STATE))
                );
            assertTrue(latch.await(100, TimeUnit.SECONDS));
        }

        assertEquals(accessKey, totalEntitiesAccessKey.get());
        assertEquals(accessKey, realtimeResultAccessKey.get());
    }

    @SuppressWarnings("unchecked")
    private void setUpClientExecuteProfileAction(InittedEverResultStatus initted) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ProfileResponse> listener = (ActionListener<ProfileResponse>) args[1];

            node1 = "node1";
            nodeName1 = "nodename1";
            discoveryNode1 = new DiscoveryNode(
                nodeName1,
                node1,
                new TransportAddress(TransportAddress.META_ADDRESS, 9300),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            );

            node2 = "node2";
            nodeName2 = "nodename2";
            discoveryNode2 = new DiscoveryNode(
                nodeName2,
                node2,
                new TransportAddress(TransportAddress.META_ADDRESS, 9301),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            );

            modelSize = 712480L;
            model1Id = "A69pa3UBHuCbh-emo9oR_entity_host1";
            model0Id = "A69pa3UBHuCbh-emo9oR_entity_host0";

            shingleSize = -1;

            String clusterName = "test-cluster-name";

            Map<String, Long> modelSizeMap1 = new HashMap<String, Long>() {
                {
                    put(model1Id, modelSize);
                }
            };

            Map<String, Long> modelSizeMap2 = new HashMap<String, Long>() {
                {
                    put(model0Id, modelSize);
                }
            };

            // one model in each node; all fully initialized
            long updates = requiredSamples - 1;
            if (InittedEverResultStatus.INITTED == initted) {
                updates = requiredSamples + 1;
            }
            ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(
                discoveryNode1,
                modelSizeMap1,
                1L,
                updates,
                new ArrayList<>(),
                modelSizeMap1.size(),
                false
            );
            ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(
                discoveryNode2,
                modelSizeMap2,
                1L,
                updates,
                new ArrayList<>(),
                modelSizeMap2.size(),
                false
            );
            List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1, profileNodeResponse2);
            List<FailedNodeException> failures = Collections.emptyList();
            ProfileResponse profileResponse = new ProfileResponse(new ClusterName(clusterName), profileNodeResponses, failures);

            listener.onResponse(profileResponse);

            return null;
        }).when(nodeCommunicator).profile(any(), any());

    }

    @SuppressWarnings("unchecked")
    private void setUpClientSearch(InittedEverResultStatus inittedEverResultStatus) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[2];

            AnomalyResult result = null;
            if (request.source().query().toString().contains(AnomalyResult.ANOMALY_SCORE_FIELD)) {
                switch (inittedEverResultStatus) {
                    case INITTED:
                        result = TestHelpers.randomAnomalyDetectResult(0.87);
                        listener.onResponse(TestHelpers.createSearchResponse(result));
                        break;
                    case NOT_INITTED:
                        listener.onResponse(TestHelpers.createEmptySearchResponse());
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            }

            return null;
        }).when(dataAccess).search(any(), any(), any());
    }

    public void testInit() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpClientSearch(InittedEverResultStatus.NOT_INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        AtomicReference<ConfigProfile> actualProfile = new AtomicReference<>();
        AtomicReference<Exception> actualException = new AtomicReference<>();

        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.INIT).build();
        runner.profile(detectorId, null, ActionListener.wrap(response -> {
            actualProfile.set(response);
            inProgressLatch.countDown();
        }, exception -> {
            actualException.set(exception);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        assertNull("Unexpected exception: " + actualException.get(), actualException.get());
        assertEquals(expectedProfile, actualProfile.get());
    }

    public void testRunning() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.INITTED);
        setUpClientSearch(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        AtomicReference<ConfigProfile> actualProfile = new AtomicReference<>();
        AtomicReference<Exception> actualException = new AtomicReference<>();

        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.RUNNING).build();
        runner.profile(detectorId, null, ActionListener.wrap(response -> {
            actualProfile.set(response);
            inProgressLatch.countDown();
        }, exception -> {
            actualException.set(exception);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        assertNull("Unexpected exception: " + actualException.get(), actualException.get());
        assertEquals(expectedProfile, actualProfile.get());
    }

    /**
     * Although profile action results indicate initted, we trust what result index tells us
     * @throws InterruptedException if CountDownLatch is interrupted while waiting
     */
    public void testResultIndexFinalTruth() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpClientSearch(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        AtomicReference<ConfigProfile> actualProfile = new AtomicReference<>();
        AtomicReference<Exception> actualException = new AtomicReference<>();

        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.RUNNING).build();
        runner.profile(detectorId, null, ActionListener.wrap(response -> {
            actualProfile.set(response);
            inProgressLatch.countDown();
        }, exception -> {
            actualException.set(exception);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        assertNull("Unexpected exception: " + actualException.get(), actualException.get());
        assertEquals(expectedProfile, actualProfile.get());
    }
}
