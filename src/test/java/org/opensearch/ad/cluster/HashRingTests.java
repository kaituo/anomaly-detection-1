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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_COOLDOWN_MINUTES;

import java.net.UnknownHostException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.cluster.ADDataMigrator;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import test.org.opensearch.ad.util.ClusterCreation;

public class HashRingTests extends ADUnitTestCase {

    private ClusterService clusterService;
    private DiscoveryNodeFilterer nodeFilter;
    private Settings settings;
    private Clock clock;
    private DataAccess dataAccess;
    private ADDataMigrator dataMigrator;
    private HashRing hashRing;
    private DiscoveryNodes.Delta delta;
    private String localNodeId;
    private String newNodeId;
    private String warmNodeId;
    private DiscoveryNode localNode;
    private DiscoveryNode newNode;
    private DiscoveryNode warmNode;
    private ADCacheProvider cacheProvider;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        localNodeId = "localNode";
        localNode = createNode(localNodeId, "127.0.0.1", 9200, emptyMap());
        newNodeId = "newNode";
        newNode = createNode(newNodeId, "127.0.0.2", 9201, emptyMap());
        warmNodeId = "warmNode";
        warmNode = createNode(warmNodeId, "127.0.0.3", 9202, ImmutableMap.of(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE));

        settings = Settings.builder().put(AD_COOLDOWN_MINUTES.getKey(), TimeValue.timeValueSeconds(5)).build();
        ClusterSettings clusterSettings = clusterSetting(settings, AD_COOLDOWN_MINUTES);
        clusterService = spy(new ClusterService(settings, clusterSettings, mock(ThreadPool.class), null));
        setClusterState(localNode, localNode);

        nodeFilter = spy(
            new DiscoveryNodeFilterer(clusterService, mock(org.opensearch.cluster.metadata.IndexNameExpressionResolver.class))
        );
        dataMigrator = mock(ADDataMigrator.class);
        dataAccess = mock(DataAccess.class);

        clock = mock(Clock.class);
        when(clock.millis()).thenReturn(700000L);

        delta = mock(DiscoveryNodes.Delta.class);

        String modelId = "123_model_threshold";
        cacheProvider = mock(ADCacheProvider.class);
        ADPriorityCache cache = mock(ADPriorityCache.class);
        when(cacheProvider.get()).thenReturn(cache);
        ModelState<ThresholdedRandomCutForest> hostedModel = new ModelState<>(
            mock(ThresholdedRandomCutForest.class),
            modelId,
            "detector-1",
            null,
            ModelManager.ModelType.TRCF.getName(),
            Clock.systemUTC()
        );
        when(cache.getAllModels()).thenReturn(singletonList(hostedModel));

        hashRing = spy(new HashRing(nodeFilter, clock, settings, dataAccess, clusterService, dataMigrator, cacheProvider));
    }

    public void testGetOwningNodeWithEmptyResult() throws UnknownHostException {
        DiscoveryNode node1 = createNode(Integer.toString(1), "127.0.0.4", 9204, emptyMap());
        doReturn(node1).when(clusterService).localNode();

        Optional<DiscoveryNode> node = hashRing.getOwningNodeWithSameLocalVersionForRealtime("http-latency-rcf-1");
        assertFalse(node.isPresent());
    }

    public void testGetOwningNode() throws UnknownHostException {
        List<DiscoveryNode> addedNodes = setupNodeDelta();

        // Add first node,
        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {
            Optional<DiscoveryNode> node = hashRing.getOwningNodeWithSameLocalVersionForRealtime("http-latency-rcf-1");
            assertTrue(node.isPresent());
            assertTrue(asList(newNodeId, localNodeId).contains(node.get().getId()));
            DiscoveryNode[] nodesWithSameLocalAdVersion = hashRing.getNodesWithSameLocalVersion();
            Set<String> nodesWithSameLocalAdVersionIds = new HashSet<>();
            for (DiscoveryNode n : nodesWithSameLocalAdVersion) {
                nodesWithSameLocalAdVersionIds.add(n.getId());
            }
            assertFalse("Should not build warm node into hash ring", nodesWithSameLocalAdVersionIds.contains(warmNodeId));
            assertEquals("Wrong hash ring size", 2, nodesWithSameLocalAdVersion.length);
            assertEquals(
                "Wrong hash ring size for historical analysis",
                2,
                hashRing.getNodesWithSameVersion(Version.V_2_1_0, false).size()
            );
            // Circles for realtime AD will change as it's eligible to build for when its empty
            assertEquals("Wrong hash ring size for realtime AD", 2, hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size());
        }, e -> {
            logger.error("building hash ring failed", e);
            assertFalse("Build hash ring failed", true);
        }));

        // Second new node joins cluster, test realtime circles will not update.
        String newNodeId2 = "newNode2";
        DiscoveryNode newNode2 = createNode(newNodeId2, "127.0.0.4", 9200, emptyMap());
        addedNodes.add(newNode2);
        when(delta.addedNodes()).thenReturn(addedNodes);
        setupClusterAdminClient(localNode, newNode, newNode2);
        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {
            assertEquals(
                "Wrong hash ring size for historical analysis",
                3,
                hashRing.getNodesWithSameVersion(Version.V_2_1_0, false).size()
            );
            // Circles for realtime AD will not change as it's eligible to rebuild
            assertEquals("Wrong hash ring size for realtime AD", 2, hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size());
        }, e -> {
            logger.error("building hash ring failed", e);

            assertFalse("Build hash ring failed", true);
        }));

        // Mock it's eligible to rebuild circles for realtime AD, then add new node. Realtime circles should change.
        when(hashRing.eligibleToRebuildCirclesForRealtimeAD()).thenReturn(true);
        String newNodeId3 = "newNode3";
        DiscoveryNode newNode3 = createNode(newNodeId3, "127.0.0.5", 9200, emptyMap());
        addedNodes.add(newNode3);
        when(delta.addedNodes()).thenReturn(addedNodes);
        setupClusterAdminClient(localNode, newNode, newNode2, newNode3);
        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {
            assertEquals(
                "Wrong hash ring size for historical analysis",
                4,
                hashRing.getNodesWithSameVersion(Version.V_2_1_0, false).size()
            );
            assertEquals("Wrong hash ring size for realtime AD", 4, hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size());
        }, e -> {
            logger.error("building hash ring failed", e);
            assertFalse("Failed to build hash ring", true);
        }));
    }

    public void testGetAllEligibleDataNodesWithKnownAdVersionAndGetNodeByAddress() {
        setupNodeDelta();
        hashRing.getAllEligibleDataNodesWithKnownVersion(nodes -> {
            assertEquals("Wrong hash ring size for historical analysis", 2, nodes.length);
            Optional<DiscoveryNode> node = hashRing.getNodeByAddress(newNode.getAddress());
            assertTrue(node.isPresent());
            assertEquals(newNodeId, node.get().getId());
        }, ActionListener.wrap(r -> {}, e -> { assertFalse("Failed to build hash ring", true); }));
    }

    public void testBuildAndGetOwningNodeWithSameLocalAdVersion() {
        setupNodeDelta();
        hashRing
            .buildAndGetOwningNodeWithSameLocalVersion(
                "testModelId",
                node -> { assertTrue(node.isPresent()); },
                ActionListener.wrap(r -> {}, e -> {
                    assertFalse("Failed to build hash ring", true);
                })
            );
    }

    public void testRebuildRealtimeCirclesImmediatelyWhenRealtimeCirclesEmpty() {
        doReturn(localNode).when(clusterService).localNode();
        doReturn(false).when(nodeFilter).nodeExists(anyString());
        doReturn(new DiscoveryNode[0]).when(nodeFilter).getEligibleDataNodes();

        hashRing.buildCircles(ActionListener.wrap(r -> {}, e -> fail("Failed initial build with empty membership: " + e.getMessage())));
        assertTrue("Expected hash ring to be marked initialized after empty successful build", hashRing.isHashRingInited());
        assertEquals(
            "Expected empty realtime hash ring after initial empty build",
            0,
            hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size()
        );

        setupNodeDelta();
        hashRing.addNodeChangeEvent();

        hashRing.buildCircles(delta, ActionListener.wrap(r -> {
            assertEquals(
                "Historical hash ring should include the two hot nodes",
                2,
                hashRing.getNodesWithSameVersion(Version.V_2_1_0, false).size()
            );
            assertEquals(
                "Realtime hash ring should rebuild immediately even within cooldown when currently empty",
                2,
                hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size()
            );
        }, e -> fail("Failed to build hash ring after membership update: " + e.getMessage())));
    }

    public void testGetOwningNodeRefreshesRealtimeRingBeforeLookupWhenRingEmpty() {
        doReturn(localNode).when(clusterService).localNode();
        doReturn(false).when(nodeFilter).nodeExists(anyString());
        doReturn(new DiscoveryNode[0]).when(nodeFilter).getEligibleDataNodes();

        hashRing.buildCircles(ActionListener.wrap(r -> {}, e -> fail("Failed initial empty build: " + e.getMessage())));
        assertEquals(0, hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size());

        doReturn(true).when(nodeFilter).nodeExists(localNodeId);
        setupNodeDelta();
        hashRing.addNodeChangeEvent();

        Optional<DiscoveryNode> owningNode = hashRing.getOwningNodeWithSameLocalVersionForRealtime("http-latency-rcf-1");

        assertTrue("Expected realtime lookup to refresh the ring before returning", owningNode.isPresent());
        assertEquals(
            "Realtime hash ring should be populated during the bootstrap lookup",
            2,
            hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size()
        );
    }

    public void testInitialSnapshotSeedsRealtimeBootstrapEvent() {
        setupNodeDelta();

        // No explicit addNodeChangeEvent() here: this exercises the non-delta snapshot path that
        // discovers initial membership from getEligibleDataNodes() while nodeVersions is still empty.
        hashRing.buildCircles(ActionListener.wrap(r -> {
            assertEquals(
                "Historical hash ring should include the two hot nodes after initial snapshot build",
                2,
                hashRing.getNodesWithSameVersion(Version.V_2_1_0, false).size()
            );
            assertEquals(
                "Realtime hash ring should also be initialized from that first discovered membership",
                2,
                hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size()
            );
        }, e -> fail("Failed to build hash ring from initial snapshot: " + e.getMessage())));
    }

    public void testBuildCirclesDoesNotForceAddLocalUuidWhenAddressKeyExists() throws UnknownHostException {
        String localUuid = "local-uuid";
        DiscoveryNode localUuidNode = createNode(localUuid, "127.0.0.1", 9201, emptyMap());
        DiscoveryNode localAddressNode = createNode("127.0.0.1:9201", "127.0.0.1", 9201, emptyMap());
        DiscoveryNode remoteAddressNode = createNode("127.0.0.2:9201", "127.0.0.2", 9201, emptyMap());

        doReturn(localUuidNode).when(clusterService).localNode();
        setClusterState(localUuidNode, localUuidNode, remoteAddressNode);
        doReturn(new DiscoveryNode[] { localAddressNode, remoteAddressNode }).when(nodeFilter).getEligibleDataNodes();

        List<String[]> requestedNodeIds = new ArrayList<>();
        doAnswer(invocation -> {
            NodesInfoRequest request = invocation.getArgument(0);
            requestedNodeIds.add(request.nodesIds().clone());
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (String requestedNodeId : request.nodesIds()) {
                if (localAddressNode.getId().equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(localAddressNode, "2.1.0.0"));
                } else if (remoteAddressNode.getId().equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(remoteAddressNode, "2.1.0.0"));
                }
            }
            listener.onResponse(new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of()));
            return null;
        }).when(dataAccess).nodesInfo(any(), any());

        hashRing.buildCircles(ActionListener.wrap(r -> {}, e -> fail("Failed to build hash ring: " + e.getMessage())));
        assertEquals("Expected one nodesInfo call on first build", 1, requestedNodeIds.size());
        assertTrue(
            "First build should still include local uuid key before map is populated",
            asList(requestedNodeIds.get(0)).contains(localUuid)
        );

        hashRing.buildCircles(ActionListener.wrap(r -> {}, e -> fail("Failed to build hash ring: " + e.getMessage())));
        assertEquals("Second build should not call nodesInfo for local uuid again", 1, requestedNodeIds.size());
    }

    public void testLocalVersionLookupsWorkWhenLocalVersionStoredByAddressKey() throws UnknownHostException {
        String localUuid = "local-uuid";
        DiscoveryNode localUuidNode = createNode(localUuid, "127.0.0.1", 9201, emptyMap());
        DiscoveryNode localAddressNode = createNode("127.0.0.1:9201", "127.0.0.1", 9201, emptyMap());
        DiscoveryNode remoteAddressNode = createNode("127.0.0.2:9201", "127.0.0.2", 9201, emptyMap());

        doReturn(localUuidNode).when(clusterService).localNode();
        setClusterState(localUuidNode, localUuidNode, remoteAddressNode);
        doReturn(new DiscoveryNode[] { localAddressNode, remoteAddressNode }).when(nodeFilter).getEligibleDataNodes();

        doAnswer(invocation -> {
            NodesInfoRequest request = invocation.getArgument(0);
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (String requestedNodeId : request.nodesIds()) {
                if (localAddressNode.getId().equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(localAddressNode, "2.1.0.0"));
                } else if (remoteAddressNode.getId().equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(remoteAddressNode, "2.1.0.0"));
                }
            }
            listener.onResponse(new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of()));
            return null;
        }).when(dataAccess).nodesInfo(any(), any());

        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(ActionListener.wrap(r -> {}, e -> fail("Failed to build hash ring: " + e.getMessage())));

        Optional<DiscoveryNode> owningNode = hashRing.getOwningNodeWithSameLocalVersionForRealtime("http-latency-rcf-1");
        assertTrue("Expected owning node for realtime lookup", owningNode.isPresent());

        DiscoveryNode[] sameVersionNodes = hashRing.getNodesWithSameLocalVersion();
        assertEquals("Expected all address-keyed nodes in same local version circle", 2, sameVersionNodes.length);

        hashRing.getNodesWithSameLocalVersion(nodes -> {
            assertEquals("Expected all address-keyed nodes in same local version circle", 2, nodes.length);
        }, ActionListener.wrap(r -> {}, e -> fail("Failed to get nodes with same local version: " + e.getMessage())));

        hashRing
            .buildAndGetOwningNodeWithSameLocalVersion(
                "testModelId",
                node -> assertTrue("Expected owning node for historical lookup", node.isPresent()),
                ActionListener.wrap(r -> {}, e -> fail("Failed to get owning node with same local version: " + e.getMessage()))
            );
    }

    public void testLocalVersionLookupsStayUnresolvedWhenSameIpMatchesAreAmbiguous() throws UnknownHostException {
        String localUuid = "local-uuid";
        DiscoveryNode localUuidNode = createNode(localUuid, "127.0.0.1", 9300, emptyMap());
        DiscoveryNode localAddressNode1 = createNode("127.0.0.1:9200", "127.0.0.1", 9200, emptyMap());
        DiscoveryNode localAddressNode2 = createNode("127.0.0.1:9201", "127.0.0.1", 9201, emptyMap());
        DiscoveryNode remoteAddressNode = createNode("127.0.0.2:9201", "127.0.0.2", 9201, emptyMap());

        doReturn(localUuidNode).when(clusterService).localNode();
        setClusterState(localUuidNode, localUuidNode, remoteAddressNode);
        doReturn(new DiscoveryNode[] { localAddressNode1, localAddressNode2, remoteAddressNode }).when(nodeFilter).getEligibleDataNodes();

        doAnswer(invocation -> {
            NodesInfoRequest request = invocation.getArgument(0);
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (String requestedNodeId : request.nodesIds()) {
                if (localAddressNode1.getId().equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(localAddressNode1, "2.1.0.0"));
                } else if (localAddressNode2.getId().equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(localAddressNode2, "2.1.0.0"));
                } else if (remoteAddressNode.getId().equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(remoteAddressNode, "2.1.0.0"));
                }
            }
            listener.onResponse(new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of()));
            return null;
        }).when(dataAccess).nodesInfo(any(), any());

        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(ActionListener.wrap(r -> {}, e -> fail("Failed to build hash ring: " + e.getMessage())));

        DiscoveryNode[] sameVersionNodes = hashRing.getNodesWithSameLocalVersion();
        assertEquals("Ambiguous same-IP matches should not pick an arbitrary local hash-ring key", 0, sameVersionNodes.length);
    }

    public void testDeferredNodesRetryWithBackoffUntilReady() {
        AtomicLong currentTime = new AtomicLong(700000L);
        doAnswer(invocation -> currentTime.get()).when(clock).millis();
        doReturn(true).when(nodeFilter).nodeExists(localNodeId);

        setupNodeDelta();

        List<Set<String>> requestedNodeIds = new ArrayList<>();
        AtomicInteger newNodeAttempts = new AtomicInteger(0);
        doAnswer(invocation -> {
            NodesInfoRequest request = invocation.getArgument(0);
            requestedNodeIds.add(new HashSet<>(asList(request.nodesIds())));
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (String requestedNodeId : request.nodesIds()) {
                if (localNodeId.equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(localNode, "2.1.0.0"));
                } else if (newNodeId.equals(requestedNodeId) && newNodeAttempts.incrementAndGet() >= 2) {
                    nodeInfos.add(createNodeInfo(newNode, "2.1.0.0"));
                }
            }
            listener.onResponse(new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of()));
            return null;
        }).when(dataAccess).nodesInfo(any(), any());

        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {}, e -> fail("Failed initial build: " + e.getMessage())));

        assertEquals(1, requestedNodeIds.size());
        assertEquals(new HashSet<>(asList(localNodeId, newNodeId)), requestedNodeIds.get(0));
        assertEquals(
            "Only the ready local node should be admitted initially",
            1,
            hashRing.getNodesWithSameVersion(Version.V_2_1_0, false).size()
        );
        assertEquals(
            "Realtime ring should contain only the ready local node initially",
            1,
            hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size()
        );

        currentTime.set(719999L);
        hashRing.buildCirclesForRealtime();
        assertEquals("Deferred node should not be retried before the 20s backoff expires", 1, requestedNodeIds.size());

        currentTime.set(720001L);
        hashRing.buildCirclesForRealtime();

        assertEquals("Deferred node should be retried once backoff expires", 2, requestedNodeIds.size());
        assertEquals(new HashSet<>(singletonList(newNodeId)), requestedNodeIds.get(1));
        assertEquals(
            "Deferred node should join the historical ring after it becomes ready",
            2,
            hashRing.getNodesWithSameVersion(Version.V_2_1_0, false).size()
        );
        assertEquals(
            "Realtime ring should refresh after cooldown once deferred node becomes ready",
            2,
            hashRing.getNodesWithSameVersion(Version.V_2_1_0, true).size()
        );
    }

    public void testRealtimeLookupForcesDeferredRetryWhenRingIsEmpty() {
        AtomicInteger newNodeAttempts = new AtomicInteger(0);
        doReturn(localNode).when(clusterService).localNode();
        setClusterState(localNode, localNode, newNode);
        doReturn(false).when(nodeFilter).nodeExists(anyString());
        doReturn(new DiscoveryNode[] { newNode }).when(nodeFilter).getEligibleDataNodes();

        doAnswer(invocation -> {
            NodesInfoRequest request = invocation.getArgument(0);
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (String requestedNodeId : request.nodesIds()) {
                if (newNodeId.equals(requestedNodeId) && newNodeAttempts.incrementAndGet() >= 2) {
                    nodeInfos.add(createNodeInfo(newNode, Version.CURRENT.toString()));
                }
            }
            listener.onResponse(new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of()));
            return null;
        }).when(dataAccess).nodesInfo(any(), any());

        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(ActionListener.wrap(r -> {}, e -> fail("Failed initial deferred build: " + e.getMessage())));
        assertEquals(0, hashRing.getNodesWithSameVersion(Version.CURRENT, true).size());

        Optional<DiscoveryNode> owningNode = hashRing.getOwningNodeWithSameLocalVersionForRealtime("http-latency-rcf-1");

        assertTrue("Expected lookup to force a retry for deferred nodes when realtime ring is empty", owningNode.isPresent());
        assertEquals(1, hashRing.getNodesWithSameVersion(Version.CURRENT, true).size());
    }

    public void testDeferredBackoffDoesNotKeepRealtimeBuildHot() {
        AtomicLong currentTime = new AtomicLong(700000L);
        doAnswer(invocation -> currentTime.get()).when(clock).millis();
        doReturn(true).when(nodeFilter).nodeExists(localNodeId);

        setupNodeDelta();

        doAnswer(invocation -> {
            NodesInfoRequest request = invocation.getArgument(0);
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (String requestedNodeId : request.nodesIds()) {
                if (localNodeId.equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(localNode, "2.1.0.0"));
                }
            }
            listener.onResponse(new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of()));
            return null;
        }).when(dataAccess).nodesInfo(any(), any());

        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {}, e -> fail("Failed initial build: " + e.getMessage())));

        clearInvocations(nodeFilter);

        currentTime.set(719999L);
        hashRing.buildCirclesForRealtime();

        verify(nodeFilter, never()).getEligibleDataNodes();
    }

    public void testDeferredNodeStateClearedWhenMembershipRemovesNode() {
        AtomicLong currentTime = new AtomicLong(700000L);
        doAnswer(invocation -> currentTime.get()).when(clock).millis();
        doReturn(true).when(nodeFilter).nodeExists(localNodeId);

        setupNodeDelta();

        List<Set<String>> requestedNodeIds = new ArrayList<>();
        doAnswer(invocation -> {
            NodesInfoRequest request = invocation.getArgument(0);
            requestedNodeIds.add(new HashSet<>(asList(request.nodesIds())));
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (String requestedNodeId : request.nodesIds()) {
                if (localNodeId.equals(requestedNodeId)) {
                    nodeInfos.add(createNodeInfo(localNode, "2.1.0.0"));
                }
            }
            listener.onResponse(new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of()));
            return null;
        }).when(dataAccess).nodesInfo(any(), any());

        hashRing.addNodeChangeEvent();
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {}, e -> fail("Failed initial build: " + e.getMessage())));
        assertEquals(1, requestedNodeIds.size());
        assertEquals(new HashSet<>(asList(localNodeId, newNodeId)), requestedNodeIds.get(0));

        doReturn(new DiscoveryNode[] { localNode }).when(nodeFilter).getEligibleDataNodes();
        hashRing.buildCircles(ActionListener.wrap(r -> {}, e -> fail("Failed rebuild after membership removal: " + e.getMessage())));

        currentTime.set(730000L);
        hashRing.buildCirclesForRealtime();

        assertEquals("Removed deferred node should not be retried again", 1, requestedNodeIds.size());
        assertEquals(
            "Only the remaining local node should stay in the historical ring",
            1,
            hashRing.getNodesWithSameVersion(Version.V_2_1_0, false).size()
        );
    }

    private List<DiscoveryNode> setupNodeDelta() {
        List<DiscoveryNode> addedNodes = new ArrayList<>();
        addedNodes.add(newNode);

        List<DiscoveryNode> removedNodes = asList();

        when(delta.removed()).thenReturn(false);
        when(delta.added()).thenReturn(true);
        when(delta.removedNodes()).thenReturn(removedNodes);
        when(delta.addedNodes()).thenReturn(addedNodes);

        doReturn(localNode).when(clusterService).localNode();
        setClusterState(localNode, localNode, newNode, warmNode);
        setupClusterAdminClient(localNode, newNode, warmNode);

        doReturn(new DiscoveryNode[] { localNode, newNode }).when(nodeFilter).getEligibleDataNodes();
        return addedNodes;
    }

    private void setClusterState(DiscoveryNode localNode, DiscoveryNode... nodes) {
        doReturn(ClusterCreation.state(ClusterName.DEFAULT, localNode, localNode, asList(nodes))).when(clusterService).state();
    }

    private void setupClusterAdminClient(DiscoveryNode... nodes) {
        doAnswer(invocation -> {
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (DiscoveryNode node : nodes) {
                nodeInfos.add(createNodeInfo(node, "2.1.0.0"));
            }
            NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of());
            listener.onResponse(nodesInfoResponse);
            return null;
        }).when(dataAccess).nodesInfo(any(), any());
    }

    private NodeInfo createNodeInfo(DiscoveryNode node, String version) {
        List<PluginInfo> plugins = new ArrayList<>();
        plugins
            .add(
                new PluginInfo(
                    ADCommonName.AD_PLUGIN_NAME,
                    randomAlphaOfLengthBetween(3, 10),
                    version,
                    Version.CURRENT,
                    "1.8",
                    randomAlphaOfLengthBetween(3, 10),
                    randomAlphaOfLengthBetween(3, 10),
                    ImmutableList.of(),
                    randomBoolean()
                )
            );
        List<PluginInfo> modules = new ArrayList<>();
        modules.addAll(plugins);
        PluginsAndModules pluginsAndModules = new PluginsAndModules(plugins, modules);
        return new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            node,
            settings,
            null,
            null,
            null,
            null,
            null,
            null,
            pluginsAndModules,
            null,
            null,
            null,
            null
        );
    }
}
