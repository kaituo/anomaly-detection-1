/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.util.Timeout;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.client.RestClientProvider;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Live AOSS multi-tenant data-plane IT using one ECS task for each service role:
 * master, coordinator, and model.
 */
public class MultiTenantThreeRoleAossDataPlaneIT extends MultiTenantAossDataPlaneIT {

    private static final String MASTER_CLUSTER_PROPERTY = "tests.master.rest.cluster";
    private static final String COORDINATOR_CLUSTER_PROPERTY = "tests.rest.cluster";
    private static final String MODEL_CLUSTER_PROPERTY = "tests.model.rest.cluster";
    private static EcsThreeRoleCluster ecsCluster;
    private static String ecsRunId;
    private static RestClient coordinatorRestClient;
    private static String coordinatorRestClientEndpoint;

    @BeforeClass
    public static void startEcsThreeRoleCluster() {
        try {
            ecsCluster = EcsThreeRoleCluster.start();
            ecsRunId = ecsCluster.runId();
            ecsCluster.verifyAutoScalingConfiguration();
        } catch (Exception | AssertionError e) {
            if (ecsCluster != null) {
                try {
                    throw new AssertionError("Three-role ECS startup diagnostics:\n" + ecsCluster.diagnostics(), e);
                } finally {
                    ecsCluster.close();
                    ecsCluster = null;
                }
            }
            throw e;
        }
    }

    @AfterClass
    public static void stopEcsThreeRoleCluster() {
        RuntimeException cleanupFailure = null;
        closeCoordinatorRestClient();
        if (ecsCluster != null) {
            try {
                ecsCluster.close();
            } catch (RuntimeException e) {
                cleanupFailure = e;
            } finally {
                ecsCluster = null;
            }
        }
        if (ecsRunId != null) {
            try {
                EcsThreeRoleCluster.cleanupRun(ecsRunId);
            } catch (RuntimeException e) {
                if (cleanupFailure == null) {
                    cleanupFailure = e;
                } else {
                    cleanupFailure.addSuppressed(e);
                }
            } finally {
                ecsRunId = null;
            }
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    @Override
    @Test
    public void testApiProxyAndSqsDirectSigningUseAossCustomerDataPlane() throws Exception {
        assumeThreeRoleClusterConfigured();

        try {
            assertOnlyPluginRole(MASTER_CLUSTER_PROPERTY, TimeSeriesSettings.MASTER_ROLE);
            assertOnlyPluginRole(COORDINATOR_CLUSTER_PROPERTY, TimeSeriesSettings.COORDINATOR_ROLE);
            assertOnlyPluginRole(MODEL_CLUSTER_PROPERTY, TimeSeriesSettings.MODEL_ROLE);

            super.testApiProxyAndSqsDirectSigningUseAossCustomerDataPlane();
        } catch (Exception | AssertionError e) {
            throw new AssertionError("Three-role ECS diagnostics:\n" + ecsCluster.diagnostics(), e);
        }
    }

    @Override
    protected void afterSuccessfulSqsProcessing(AnomalyDetector detector, String tenantId, SqsClient sqsClient, Instant nextManualTick)
        throws Exception {
        AutoScalingWorkload workload = createHighCardinalityAutoScalingWorkload(sqsClient, tenantId, nextManualTick);
        workload.coordinatorClientSupplier(MultiTenantThreeRoleAossDataPlaneIT::currentCoordinatorClient);
        workload.includeDetectorForCleanup(detector);
        ecsCluster.verifyMetricDrivenAutoScaling(workload::trigger, workload::stop, tenantId);
        workload.verifyCheckpointDeletion();
    }

    @Override
    protected Response stopTenantDetector(String detectorId, String tenantId) throws IOException {
        if (ecsCluster != null) {
            return stopTenantDetector(detectorId, tenantId, currentCoordinatorClient());
        }
        return super.stopTenantDetector(detectorId, tenantId);
    }

    @Override
    protected Response deleteTenantDetector(String detectorId, String tenantId) throws IOException {
        if (ecsCluster != null) {
            return deleteTenantDetector(detectorId, tenantId, currentCoordinatorClient());
        }
        return super.deleteTenantDetector(detectorId, tenantId);
    }

    private static synchronized RestClient currentCoordinatorClient() {
        String endpoint = ecsCluster.currentCoordinatorEndpoint();
        if (coordinatorRestClient == null || endpoint.equals(coordinatorRestClientEndpoint) == false) {
            closeCoordinatorRestClient();
            coordinatorRestClient = RestClientProvider.getRestClient(endpoint, 300_000);
            coordinatorRestClientEndpoint = endpoint;
        }
        return coordinatorRestClient;
    }

    private static synchronized void closeCoordinatorRestClient() {
        if (coordinatorRestClient == null) {
            return;
        }
        try {
            coordinatorRestClient.close();
        } catch (IOException ignored) {
            // Best-effort test client cleanup.
        } finally {
            coordinatorRestClient = null;
            coordinatorRestClientEndpoint = null;
        }
    }

    @Override
    protected HashRingRevisionRef prepareModelNodeRouting(DynamoDbClient dynamoDbClient) {
        ecsCluster.waitForModelTaskRevision();
        return null;
    }

    @Override
    protected void cleanupModelNodeRouting(HashRingRevisionRef hashRingRevisionRef) {
        // The ECS fixture owns the Cloud Map registration and hash-ring cleanup.
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        Settings effectiveSettings = withLongRestTimeout(settings);
        if (isHttps()) {
            return super.buildClient(effectiveSettings, hosts);
        }

        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, effectiveSettings);
        Timeout timeout = Timeout.ofMilliseconds(300_000);
        builder.setRequestConfigCallback(conf -> {
            conf.setConnectTimeout(timeout);
            conf.setResponseTimeout(timeout);
            return conf;
        });
        builder.setStrictDeprecationMode(effectiveSettings.getAsBoolean("strictDeprecationMode", true));
        return builder.build();
    }

    @Override
    protected Settings restClientSettings() {
        return withLongRestTimeout(super.restClientSettings());
    }

    @Override
    protected Settings restAdminSettings() {
        return withLongRestTimeout(super.restAdminSettings());
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return ecsCluster != null || super.preserveClusterUponCompletion();
    }

    @Override
    protected boolean shouldWipeAllODFEIndices() {
        return ecsCluster == null && super.shouldWipeAllODFEIndices();
    }

    private Settings withLongRestTimeout(Settings settings) {
        return Settings.builder().put(settings).put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "300s").build();
    }

    private void assumeThreeRoleClusterConfigured() {
        Assume.assumeTrue("Master cluster must be configured", hasText(System.getProperty(MASTER_CLUSTER_PROPERTY)));
        Assume.assumeTrue("Coordinator cluster must be configured", hasText(System.getProperty(COORDINATOR_CLUSTER_PROPERTY)));
        Assume.assumeTrue("Model cluster must be configured", hasText(System.getProperty(MODEL_CLUSTER_PROPERTY)));
    }

    private void assertOnlyPluginRole(String clusterProperty, String expectedRole) throws Exception {
        try (RestClient roleClient = buildClient(restClientSettings(), hosts(clusterProperty))) {
            Response response = TestHelpers
                .makeRequest(roleClient, "GET", "/_nodes/settings?flat_settings=true", ImmutableMap.of(), "", null);
            String roles = readNodeRoles(response);
            assertTrue("Expected role [" + expectedRole + "] in [" + roles + "] for " + clusterProperty, roles.contains(expectedRole));
            for (String otherRole : new String[] {
                TimeSeriesSettings.MASTER_ROLE,
                TimeSeriesSettings.COORDINATOR_ROLE,
                TimeSeriesSettings.MODEL_ROLE }) {
                if (expectedRole.equals(otherRole) == false) {
                    assertFalse("Unexpected role [" + otherRole + "] in [" + roles + "] for " + clusterProperty, roles.contains(otherRole));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private String readNodeRoles(Response response) throws IOException {
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> nodes = (Map<String, Object>) responseMap.get("nodes");
        assertNotNull("Expected _nodes/settings response to contain nodes", nodes);
        assertFalse("Expected at least one node in _nodes/settings response", nodes.isEmpty());

        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        Map<String, Object> settings = (Map<String, Object>) node.get("settings");
        assertNotNull("Expected node settings", settings);

        Object flatRoles = settings.get(TimeSeriesSettings.NODE_ROLE.getKey());
        if (flatRoles != null) {
            return flatRoles.toString();
        }

        Map<String, Object> plugins = (Map<String, Object>) settings.get("plugins");
        if (plugins == null) {
            return "";
        }
        Map<String, Object> timeseries = (Map<String, Object>) plugins.get("timeseries");
        if (timeseries == null) {
            return "";
        }
        Object nestedRoles = timeseries.get("node.roles");
        return nestedRoles == null ? "" : nestedRoles.toString();
    }

    private HttpHost[] hosts(String clusterProperty) throws java.net.URISyntaxException {
        String cluster = System.getProperty(clusterProperty);
        String[] stringUrls = cluster.split(",");
        HttpHost[] hosts = new HttpHost[stringUrls.length];
        for (int i = 0; i < stringUrls.length; i++) {
            String endpoint = stringUrls[i].trim();
            hosts[i] = HttpHost.create(endpoint.contains("://") ? endpoint : "http://" + endpoint);
        }
        return hosts;
    }

    private boolean hasText(String value) {
        return value != null && value.isBlank() == false;
    }
}
