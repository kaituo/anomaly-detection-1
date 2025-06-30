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
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.transport.EntityADResultAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.transport.client.node.NodeClient;

public class RestEntityADResultActionTests extends OpenSearchTestCase {

    public void testPrepareRequestIncrementsExecuteRequestCountWhenStatsProvided() throws Exception {
        ADStats adStats = new ADStats(
            Map.of(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
        );
        TestRestEntityADResultAction action = new TestRestEntityADResultAction(multiTenantSettings(), adStats);
        NodeClient client = mock(NodeClient.class);
        RestChannel channel = mock(RestChannel.class);

        action
            .executePreparedRequest(
                createRequest(
                    "{\"start\":1,\"end\":2,\"tenant_id\":\"tenant-a\",\"entities\":[{\"entity\":[{\"name\":\"entity\",\"value\":\"entity-1\"}],\"value\":[1.0]}]}",
                    Map.of(DETECTOR_ID, "detector-1"),
                    tenantHeaders("tenant-a")
                ),
                client,
                channel
            );

        assertEquals(1L, adStats.getStat(StatNames.AD_EXECUTE_REQUEST_COUNT.getName()).getValue());
        verify(client).execute(eq(EntityADResultAction.INSTANCE), any(), any());
    }

    private Settings multiTenantSettings() {
        return Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_MULTI_TENANCY_ENABLED.getKey(), true)
            .put(TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey(), "test-secret")
            .build();
    }

    private Map<String, List<String>> tenantHeaders(String tenantId) {
        return Map.of(CommonName.TENANT_ID_HEADER, List.of(tenantId), CommonName.INTERNAL_API_TOKEN_HEADER, List.of("test-secret"));
    }

    private FakeRestRequest createRequest(String content, Map<String, String> params, Map<String, List<String>> headers) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.POST);
        builder.withPath(TimeSeriesAnalyticsPlugin.AD_BASE_INTERNAL_DETECTORS_URI + "/detector-1/_entity_result");
        builder.withParams(params);
        builder.withHeaders(new HashMap<>(headers));
        builder.withContent(new BytesArray(content), MediaTypeRegistry.JSON);
        return builder.build();
    }

    private static class TestRestEntityADResultAction extends RestEntityADResultAction {
        TestRestEntityADResultAction(Settings settings, ADStats adStats) {
            super(settings, adStats);
        }

        void executePreparedRequest(RestRequest request, NodeClient client, RestChannel channel) throws Exception {
            prepareRequest(request, client).accept(channel);
        }
    }
}
