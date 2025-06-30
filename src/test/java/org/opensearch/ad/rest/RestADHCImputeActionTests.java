/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ADHCImputeAction;
import org.opensearch.ad.transport.ADHCImputeRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.node.NodeClient;

public class RestADHCImputeActionTests extends OpenSearchTestCase {

    public void testPrepareRequestBuildsImputeRequestFromBody() throws Exception {
        TestRestADHCImputeAction action = new TestRestADHCImputeAction(multiTenantSettings());
        NodeClient client = mock(NodeClient.class);
        RestChannel channel = mock(RestChannel.class);

        action
            .executePreparedRequest(
                createRequest(
                    "{\"tenant_id\":\"tenant-a\",\"task_id\":null,\"data_start_millis\":123,\"data_end_millis\":456,\"ignored\":{\"foo\":\"bar\"}}",
                    Map.of(DETECTOR_ID, "detector-1"),
                    tenantHeaders("tenant-a", true)
                ),
                client,
                channel
            );

        ArgumentCaptor<ADHCImputeRequest> requestCaptor = ArgumentCaptor.forClass(ADHCImputeRequest.class);
        verify(client).execute(eq(ADHCImputeAction.INSTANCE), requestCaptor.capture(), any());

        ADHCImputeRequest imputeRequest = requestCaptor.getValue();
        assertEquals("detector-1", imputeRequest.getConfigId());
        assertEquals("tenant-a", imputeRequest.getTenantId());
        assertNull(imputeRequest.getTaskId());
        assertEquals(123L, imputeRequest.getDataStartMillis());
        assertEquals(456L, imputeRequest.getDataEndMillis());
    }

    public void testPrepareRequestRequiresInternalToken() {
        RestADHCImputeAction action = new RestADHCImputeAction(multiTenantSettings());

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"data_start_millis\":123,\"data_end_millis\":456}",
                        Map.of(DETECTOR_ID, "detector-1"),
                        tenantHeaders("tenant-a", false)
                    ),
                    mock(NodeClient.class)
                )
        );

        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Missing or invalid internal API token", exception.getMessage());
    }

    public void testPrepareRequestRequiresTenantHeader() {
        RestADHCImputeAction action = new RestADHCImputeAction(multiTenantSettings());

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"data_start_millis\":123,\"data_end_millis\":456}",
                        Map.of(DETECTOR_ID, "detector-1"),
                        internalHeadersOnly()
                    ),
                    mock(NodeClient.class)
                )
        );

        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Tenant ID header is missing or has no value", exception.getMessage());
    }

    public void testPrepareRequestRequiresDetectorId() {
        RestADHCImputeAction action = new RestADHCImputeAction(multiTenantSettings());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> action
                .prepareRequest(
                    createRequest("{\"data_start_millis\":123,\"data_end_millis\":456}", Map.of(), tenantHeaders("tenant-a", true)),
                    mock(NodeClient.class)
                )
        );

        assertEquals("Missing required parameter: " + DETECTOR_ID, exception.getMessage());
    }

    public void testPrepareRequestRejectsMismatchedTenantIds() {
        RestADHCImputeAction action = new RestADHCImputeAction(multiTenantSettings());

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"tenant_id\":\"tenant-b\",\"data_start_millis\":123,\"data_end_millis\":456}",
                        Map.of(DETECTOR_ID, "detector-1"),
                        tenantHeaders("tenant-a", true)
                    ),
                    mock(NodeClient.class)
                )
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertEquals("Tenant ID in header and body must match", exception.getMessage());
    }

    public void testPrepareRequestRejectsNonPositiveStartMillis() {
        RestADHCImputeAction action = new RestADHCImputeAction(multiTenantSettings());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"data_start_millis\":0,\"data_end_millis\":456}",
                        Map.of(DETECTOR_ID, "detector-1"),
                        tenantHeaders("tenant-a", true)
                    ),
                    mock(NodeClient.class)
                )
        );

        assertEquals("Invalid data_start_millis: 0", exception.getMessage());
    }

    public void testPrepareRequestRejectsNonPositiveEndMillis() {
        RestADHCImputeAction action = new RestADHCImputeAction(multiTenantSettings());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"data_start_millis\":123,\"data_end_millis\":0}",
                        Map.of(DETECTOR_ID, "detector-1"),
                        tenantHeaders("tenant-a", true)
                    ),
                    mock(NodeClient.class)
                )
        );

        assertEquals("Invalid data_end_millis: 0", exception.getMessage());
    }

    public void testPrepareRequestCarriesTargetNodeId() throws Exception {
        TestRestADHCImputeAction action = new TestRestADHCImputeAction(multiTenantSettings());
        NodeClient client = mock(NodeClient.class);
        RestChannel channel = mock(RestChannel.class);

        action
            .executePreparedRequest(
                createRequest(
                    "{\"tenant_id\":\"tenant-a\",\"data_start_millis\":123,\"data_end_millis\":456,\"target_node_id\":\"10.0.0.2:9200\"}",
                    Map.of(DETECTOR_ID, "detector-1"),
                    tenantHeaders("tenant-a", true)
                ),
                client,
                channel
            );

        ArgumentCaptor<ADHCImputeRequest> requestCaptor = ArgumentCaptor.forClass(ADHCImputeRequest.class);
        verify(client).execute(eq(ADHCImputeAction.INSTANCE), requestCaptor.capture(), any());

        ADHCImputeRequest imputeRequest = requestCaptor.getValue();
        assertEquals("10.0.0.2:9200", imputeRequest.getTargetNodeId());
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

    private Map<String, List<String>> internalHeadersOnly() {
        return Map.of(CommonName.INTERNAL_API_TOKEN_HEADER, List.of("test-secret"));
    }

    private FakeRestRequest createRequest(String content, Map<String, String> params, Map<String, List<String>> headers) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.POST);
        builder.withPath(TimeSeriesAnalyticsPlugin.AD_BASE_INTERNAL_DETECTORS_URI + "/detector-1/_hc_impute");
        builder.withParams(params);
        builder.withHeaders(new HashMap<>(headers));
        builder.withContent(new BytesArray(content), MediaTypeRegistry.JSON);
        return builder.build();
    }

    private static class TestRestADHCImputeAction extends RestADHCImputeAction {
        TestRestADHCImputeAction(Settings settings) {
            super(settings);
        }

        void executePreparedRequest(RestRequest request, NodeClient client, RestChannel channel) throws Exception {
            prepareRequest(request, client).accept(channel);
        }
    }
}
