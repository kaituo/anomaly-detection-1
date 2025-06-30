/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.InternalStatNames;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.node.NodeClient;

public class RestADStatsNodesActionTests extends OpenSearchTestCase {

    public void testPrepareRequestRequiresInternalToken() {
        RestADStatsNodesAction action = new RestADStatsNodesAction(adStats(), mock(DiscoveryNodeSelector.class), multiTenantSettings());

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> action.prepareRequest(createRequest(tenantHeaders("tenant-a", false)), mock(NodeClient.class))
        );

        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Missing or invalid internal API token", exception.getMessage());
    }

    public void testPrepareRequestAcceptsInternalRoute() {
        RestADStatsNodesAction action = new RestADStatsNodesAction(adStats(), mock(DiscoveryNodeSelector.class), multiTenantSettings());

        assertNotNull(action.prepareRequest(createRequest(tenantHeaders("tenant-a", true)), mock(NodeClient.class)));
    }

    public void testPrepareRequestAcceptsInternalStats() {
        RestADStatsNodesAction action = new RestADStatsNodesAction(adStats(), mock(DiscoveryNodeSelector.class), multiTenantSettings());

        assertNotNull(
            action
                .prepareRequest(
                    createRequest(tenantHeaders("tenant-a", true), InternalStatNames.JVM_HEAP_USAGE.getName()),
                    mock(NodeClient.class)
                )
        );
    }

    private ADStats adStats() {
        return new ADStats(Map.of(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier())));
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

    private FakeRestRequest createRequest(Map<String, List<String>> headers) {
        return createRequest(headers, StatNames.AD_EXECUTE_REQUEST_COUNT.getName());
    }

    private FakeRestRequest createRequest(Map<String, List<String>> headers, String stat) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.GET);
        builder.withPath(TimeSeriesAnalyticsPlugin.AD_BASE_INTERNAL_DETECTORS_URI + "/_stats/nodes/" + stat);
        builder.withParams(Map.of("nodeId", "_all", "stat", stat));
        builder.withHeaders(new HashMap<>(headers));
        return builder.build();
    }
}
