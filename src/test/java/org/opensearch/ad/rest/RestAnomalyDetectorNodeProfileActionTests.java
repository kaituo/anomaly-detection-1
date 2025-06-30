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

package org.opensearch.ad.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.transport.client.node.NodeClient;

public class RestAnomalyDetectorNodeProfileActionTests extends OpenSearchTestCase {

    public void testPrepareRequestUsesLocalNodeProfileAction() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        when(clusterService.localNode()).thenReturn(localNode);

        TestRestAnomalyDetectorNodeProfileAction action = new TestRestAnomalyDetectorNodeProfileAction(
            multiTenantSettings(),
            clusterService
        );
        NodeClient client = mock(NodeClient.class);
        RestChannel channel = mock(RestChannel.class);

        action
            .executePreparedRequest(
                createRequest(
                    Map.of(DETECTOR_ID, "detector-1", "type", ProfileName.INIT_PROGRESS.getName()),
                    tenantHeaders("tenant-a", true)
                ),
                client,
                channel
            );

        ArgumentCaptor<ProfileRequest> requestCaptor = ArgumentCaptor.forClass(ProfileRequest.class);
        verify(client).execute(eq(ADProfileAction.INSTANCE), requestCaptor.capture(), any());

        ProfileRequest profileRequest = requestCaptor.getValue();
        assertEquals("detector-1", profileRequest.getConfigId());
        assertEquals(1, profileRequest.getProfilesToBeRetrieved().size());
        assertTrue(profileRequest.getProfilesToBeRetrieved().contains(ProfileName.INIT_PROGRESS));
        assertEquals("tenant-a", profileRequest.getTenantId());
    }

    public void testPrepareRequestRequiresInternalToken() {
        ClusterService clusterService = mock(ClusterService.class);
        TestRestAnomalyDetectorNodeProfileAction action = new TestRestAnomalyDetectorNodeProfileAction(
            multiTenantSettings(),
            clusterService
        );

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> action
                .prepareRequest(createRequest(Map.of(DETECTOR_ID, "detector-1"), tenantHeaders("tenant-a", false)), mock(NodeClient.class))
        );

        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Missing or invalid internal API token", exception.getMessage());
    }

    private Settings multiTenantSettings() {
        return Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), true)
            .put(TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey(), "test-secret")
            .build();
    }

    private Map<String, List<String>> tenantHeaders(String tenantId, boolean includeInternalToken) {
        if (includeInternalToken) {
            return Map.of(CommonName.TENANT_ID_HEADER, List.of(tenantId), CommonName.INTERNAL_API_TOKEN_HEADER, List.of("test-secret"));
        }
        return Map.of(CommonName.TENANT_ID_HEADER, List.of(tenantId));
    }

    private FakeRestRequest createRequest(Map<String, String> params, Map<String, List<String>> headers) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.POST);
        builder.withPath(TimeSeriesAnalyticsPlugin.AD_BASE_INTERNAL_DETECTORS_URI + "/detector-1/_node_profile/init_progress");
        builder.withParams(params);
        builder.withHeaders(headers);
        return builder.build();
    }

    private static class TestRestAnomalyDetectorNodeProfileAction extends RestAnomalyDetectorNodeProfileAction {
        TestRestAnomalyDetectorNodeProfileAction(Settings settings, ClusterService clusterService) {
            super(settings, clusterService);
        }

        void executePreparedRequest(RestRequest request, NodeClient client, RestChannel channel) throws Exception {
            prepareRequest(request, client).accept(channel);
        }
    }
}
