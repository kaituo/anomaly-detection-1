/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.cluster;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.EVENT_BRIDGE_SQS_DELIVERY_ROLE_NAME;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REMOTE_METADATA_ENDPOINT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REMOTE_METADATA_SERVICE_NAME;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.opensearch.Version;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.mock.plugin.MockReindexPlugin;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.cluster.ClusterMembershipReader;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SingleNodeCoordinatorModelHashRingIT extends ADIntegTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @AfterClass
    public static void stopAwsIdleConnectionReaper() throws Exception {
        Class<?> reaperClass;
        try {
            reaperClass = Class.forName("software.amazon.awssdk.http.apache.internal.conn.IdleConnectionReaper");
        } catch (ClassNotFoundException e) {
            return;
        }
        Object reaper = reaperClass.getMethod("getInstance").invoke(null);
        Field connectionManagersField = reaperClass.getDeclaredField("connectionManagers");
        connectionManagersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Object, Long> connectionManagers = (Map<Object, Long>) connectionManagersField.get(reaper);
        Class<?> connectionManagerClass = Class.forName("org.apache.http.conn.HttpClientConnectionManager");
        Method deregister = reaperClass.getMethod("deregisterConnectionManager", connectionManagerClass);
        for (Object connectionManager : new ArrayList<>(connectionManagers.keySet())) {
            deregister.invoke(reaper, connectionManager);
        }
    }

    @SuppressWarnings("unchecked")
    private static void resetCachedDiscoveryNodes() throws Exception {
        Field cachedNodesField = ClusterMembershipReader.class.getDeclaredField("SHARED_CACHED_DISCOVERY_NODES");
        cachedNodesField.setAccessible(true);
        AtomicReference<DiscoveryNodes> cachedNodes = (AtomicReference<DiscoveryNodes>) cachedNodesField.get(null);
        cachedNodes.set(DiscoveryNodes.builder().build());
    }

    @SuppressWarnings("unchecked")
    private static void resetHashRing(HashRing hashRing) throws Exception {
        ((Map<?, ?>) getField(hashRing, "nodeVersions")).clear();
        ((Map<?, ?>) getField(hashRing, "circles")).clear();
        ((Map<?, ?>) getField(hashRing, "circlesForRealtimeAD")).clear();
        ((Queue<Boolean>) getField(hashRing, "nodeChangeEvents")).clear();
        ((Map<?, ?>) getField(hashRing, "deferredNodeProbeStates")).clear();
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = HashRing.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.remove(MockReindexPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AD_MULTI_TENANCY_ENABLED.getKey(), true)
            .putList(TimeSeriesSettings.NODE_ROLE.getKey(), TimeSeriesSettings.COORDINATOR_ROLE, TimeSeriesSettings.MODEL_ROLE)
            .put(TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey(), "single-node-it-secret")
            .put(TimeSeriesSettings.REGION.getKey(), "us-west-2")
            .put(TimeSeriesSettings.CLOUD_MAP_NAMESPACE.getKey(), "local")
            .put(TimeSeriesSettings.CLOUD_MAP_SERVICE.getKey(), "single-node-coordinator-model-it")
            .put(TimeSeriesSettings.CLOUD_MAP_TABLE_NAME.getKey(), "single-node-coordinator-model-it")
            .put(TimeSeriesSettings.CLUSTER_MEMBERSHIP_READER_TTL.getKey(), TimeValue.timeValueHours(1))
            .put(TimeSeriesSettings.DATA_PLANE_ENDPOINT_CONTEXT_KEY.getKey(), "data-source-url")
            .put(EVENT_BRIDGE_SQS_DELIVERY_ROLE_NAME.getKey(), "SchedulerToSQSRole")
            .put(REMOTE_METADATA_ENDPOINT.getKey(), "http://127.0.0.1:1")
            .put(REMOTE_METADATA_SERVICE_NAME.getKey(), "es")
            .build();
    }

    /**
     * Test that the realtime hash ring bootstraps when a single node is coordinator and model.
     * We set the node to be both coordinator and model in nodeSettings(), and then test that
     * the realtime hash ring bootstraps.
     * @throws Exception if the test fails
     */
    public void testRealtimeHashRingBootstrapsWhenSingleNodeIsCoordinatorAndModel() throws Exception {
        HashRing hashRing = internalCluster().getInstance(HashRing.class);
        resetCachedDiscoveryNodes();
        resetHashRing(hashRing);

        assertEquals(
            "Reproduction must start with no SDK membership nodes",
            0,
            ClusterMembershipReader.getCachedDiscoveryNodes().getSize()
        );
        assertFalse(hashRing.hasRealtimeHashRing());

        hashRing.buildCirclesForRealtime();

        assertBusy(() -> {
            assertTrue("Realtime hash ring should not stay empty for a single coordinator/model node", hashRing.hasRealtimeHashRing());
            assertEquals(1, hashRing.getNodesWithSameVersion(Version.CURRENT, true).size());
            assertTrue(hashRing.getOwningNodeWithSameLocalVersionForRealtime("single-node-model").isPresent());
        });
    }
}
