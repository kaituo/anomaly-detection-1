/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.client.ADNodeCommunicator;
import org.opensearch.ad.rest.handler.store.ADDelegatingDataManagement;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.client.DataAccess;
import org.opensearch.timeseries.client.RunContext;
import org.opensearch.timeseries.client.TenantContext;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
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
}
