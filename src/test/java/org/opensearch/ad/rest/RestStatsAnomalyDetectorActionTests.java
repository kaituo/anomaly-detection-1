/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.util.DiscoveryNodeSelector;
import org.opensearch.transport.client.node.NodeClient;

public class RestStatsAnomalyDetectorActionTests extends OpenSearchTestCase {

    public void testPrepareRequestRejectsNodeSpecificStatsInMultiTenantMode() {
        RestStatsAnomalyDetectorAction action = new RestStatsAnomalyDetectorAction(
            adStats(),
            mock(DiscoveryNodeSelector.class),
            multiTenantSettings()
        );

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(createRequest("10.0.106.175:9200", tenantHeaders()), mock(NodeClient.class))
        );

        assertEquals(
            "Node-specific stats requests are not supported in multi-tenant mode. Use the stats endpoint without a nodeId or with _all.",
            exception.getMessage()
        );
    }

    public void testPrepareRequestAllowsAllNodeSelectorInMultiTenantMode() {
        RestStatsAnomalyDetectorAction action = new RestStatsAnomalyDetectorAction(
            adStats(),
            mock(DiscoveryNodeSelector.class),
            multiTenantSettings()
        );

        assertNotNull(action.prepareRequest(createRequest("_all", tenantHeaders()), mock(NodeClient.class)));
    }

    public void testPrepareRequestAllowsNodeSpecificStatsWhenMultiTenancyDisabled() {
        RestStatsAnomalyDetectorAction action = new RestStatsAnomalyDetectorAction(
            adStats(),
            mock(DiscoveryNodeSelector.class),
            Settings.EMPTY
        );

        assertNotNull(action.prepareRequest(createRequest("node-id", Map.of()), mock(NodeClient.class)));
    }

    private ADStats adStats() {
        return new ADStats(Map.of(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier())));
    }

    private Settings multiTenantSettings() {
        return Settings.builder().put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), true).build();
    }

    private Map<String, List<String>> tenantHeaders() {
        return Map.of(CommonName.TENANT_ID_HEADER, List.of("tenant-a"));
    }

    private FakeRestRequest createRequest(String nodeId, Map<String, List<String>> headers) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.GET);
        builder.withPath(TimeSeriesAnalyticsPlugin.AD_BASE_URI + "/" + nodeId + "/stats/" + StatNames.AD_EXECUTE_REQUEST_COUNT.getName());
        builder.withParams(Map.of("nodeId", nodeId, "stat", StatNames.AD_EXECUTE_REQUEST_COUNT.getName()));
        builder.withHeaders(new HashMap<>(headers));
        return builder.build();
    }
}
