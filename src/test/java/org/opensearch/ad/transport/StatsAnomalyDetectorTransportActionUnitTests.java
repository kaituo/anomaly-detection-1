/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.client.ADNodeCommunicator;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.StatsNodeResponse;
import org.opensearch.timeseries.transport.StatsNodesResponse;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.TransportUtil;
import org.opensearch.transport.TransportService;

public class StatsAnomalyDetectorTransportActionUnitTests extends OpenSearchTestCase {

    private static final String TENANT_ID = "account-id:application-id:workspace-id";

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testClusterDetectorCountSearchUsesRequestTenantContext() {
        DataAccess dataAccess = mock(DataAccess.class);
        ADDelegatingDataManagement adDataManagement = mock(ADDelegatingDataManagement.class);
        when(adDataManagement.doesConfigIndexExist()).thenReturn(true);

        StatsAnomalyDetectorTransportAction action = new StatsAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            new ADStats(Collections.emptyMap()),
            mock(ClusterService.class),
            Settings.EMPTY,
            adDataManagement,
            dataAccess,
            mock(RunContext.class),
            mock(ADNodeCommunicator.class)
        );

        StatsRequest request = new StatsRequest(TENANT_ID, "node-1");
        request.addStat(StatNames.DETECTOR_COUNT.getName());

        action.getClusterStats(mock(MultiResponsesDelegateActionListener.class), request);

        ArgumentCaptor<TenantContext> tenantContextCaptor = ArgumentCaptor.forClass(TenantContext.class);
        verify(dataAccess).search(any(SearchRequest.class), tenantContextCaptor.capture(), any(ActionListener.class));
        assertFalse(tenantContextCaptor.getValue().isSystemWide());
        assertEquals(TENANT_ID, tenantContextCaptor.getValue().getTenantId());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRedactNodeIdsUsesOpaqueResponseKeys() throws IOException {
        Map<String, Object> firstStats = Map.of(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), 12L);
        Map<String, Object> secondStats = Map.of(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), 1L);
        StatsNodesResponse rawResponse = new StatsNodesResponse(
            ClusterName.DEFAULT,
            List
                .of(
                    new StatsNodeResponse(TransportUtil.createDiscoveryNodeFromIpPort("10.0.106.175:9200"), firstStats),
                    new StatsNodeResponse(TransportUtil.createDiscoveryNodeFromIpPort("10.0.158.247:9200"), secondStats)
                ),
            Collections.emptyList()
        );

        StatsNodesResponse redactedResponse = StatsAnomalyDetectorTransportAction.redactNodeIds(rawResponse);

        assertEquals(StatsAnomalyDetectorTransportAction.PUBLIC_NODE_ID_PREFIX + "1", redactedResponse.getNodes().get(0).getNode().getId());
        assertEquals(StatsAnomalyDetectorTransportAction.PUBLIC_NODE_ID_PREFIX + "2", redactedResponse.getNodes().get(1).getNode().getId());

        Map<String, Object> responseMap = createParser(redactedResponse.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .map();
        Map<String, Object> nodes = (Map<String, Object>) responseMap.get("nodes");
        assertTrue(nodes.containsKey("node-1"));
        assertTrue(nodes.containsKey("node-2"));
        assertFalse(nodes.containsKey("10.0.106.175:9200"));
        assertFalse(nodes.containsKey("10.0.158.247:9200"));
        assertEquals(
            12L,
            ((Number) ((Map<String, Object>) nodes.get("node-1")).get(StatNames.AD_EXECUTE_REQUEST_COUNT.getName())).longValue()
        );
        assertEquals(
            1L,
            ((Number) ((Map<String, Object>) nodes.get("node-2")).get(StatNames.AD_EXECUTE_REQUEST_COUNT.getName())).longValue()
        );
    }
}
