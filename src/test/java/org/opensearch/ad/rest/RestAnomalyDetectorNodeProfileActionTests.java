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

import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.opensearch.Version;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.transport.client.node.NodeClient;

public class RestAnomalyDetectorNodeProfileActionTests extends OpenSearchTestCase {

    public void testPrepareRequestUsesLocalNodeProfileAction() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        when(clusterService.localNode()).thenReturn(localNode);

        TestRestAnomalyDetectorNodeProfileAction action = new TestRestAnomalyDetectorNodeProfileAction(Settings.EMPTY, clusterService);
        NodeClient client = mock(NodeClient.class);
        RestChannel channel = mock(RestChannel.class);

        action.executePreparedRequest(createRequest(Map.of(DETECTOR_ID, "detector-1", "type", ProfileName.INIT_PROGRESS.getName())), client, channel);

        ArgumentCaptor<ProfileRequest> requestCaptor = ArgumentCaptor.forClass(ProfileRequest.class);
        verify(client).execute(eq(ADProfileAction.INSTANCE), requestCaptor.capture(), any());

        ProfileRequest profileRequest = requestCaptor.getValue();
        assertEquals("detector-1", profileRequest.getConfigId());
        assertEquals(1, profileRequest.getProfilesToBeRetrieved().size());
        assertTrue(profileRequest.getProfilesToBeRetrieved().contains(ProfileName.INIT_PROGRESS));
        assertNull(profileRequest.getTenantId());
    }

    private FakeRestRequest createRequest(Map<String, String> params) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.POST);
        builder.withPath("/_plugins/_anomaly_detection/detectors/detector-1/_node_profile/init_progress");
        builder.withParams(params);
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
