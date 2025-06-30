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

import static org.mockito.Mockito.mock;
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.transport.client.node.NodeClient;

public class RestADSingleStreamResultActionTests extends OpenSearchTestCase {

    public void testPrepareRequestRequiresInternalToken() {
        RestADSingleStreamResultAction action = new RestADSingleStreamResultAction(multiTenantSettings());

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"model_id\":\"model-1\",\"start\":1,\"end\":2,\"value_list\":[1.0]}",
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
        RestADSingleStreamResultAction action = new RestADSingleStreamResultAction(multiTenantSettings());

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"model_id\":\"model-1\",\"start\":1,\"end\":2,\"value_list\":[1.0]}",
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
        RestADSingleStreamResultAction action = new RestADSingleStreamResultAction(multiTenantSettings());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"model_id\":\"model-1\",\"start\":1,\"end\":2,\"value_list\":[1.0]}",
                        Map.of(),
                        tenantHeaders("tenant-a", true)
                    ),
                    mock(NodeClient.class)
                )
        );

        assertEquals("Missing required parameter: " + DETECTOR_ID, exception.getMessage());
    }

    public void testPrepareRequestAcceptsInlineConfigJson() throws Exception {
        RestADSingleStreamResultAction action = new RestADSingleStreamResultAction(multiTenantSettings());

        assertNotNull(
            action
                .prepareRequest(
                    createRequest(
                        "{\"model_id\":\"model-1\",\"start\":1,\"end\":2,\"value_list\":[1.0],\"tenant_id\":\"tenant-a\",\"config_json\":{\"name\":\"detector\"}}",
                        Map.of(DETECTOR_ID, "detector-1"),
                        tenantHeaders("tenant-a", true)
                    ),
                    mock(NodeClient.class)
                )
        );
    }

    public void testPrepareRequestRejectsMismatchedTenantIds() {
        RestADSingleStreamResultAction action = new RestADSingleStreamResultAction(multiTenantSettings());

        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> action
                .prepareRequest(
                    createRequest(
                        "{\"model_id\":\"model-1\",\"start\":1,\"end\":2,\"value_list\":[1.0],\"tenant_id\":\"tenant-b\"}",
                        Map.of(DETECTOR_ID, "detector-1"),
                        tenantHeaders("tenant-a", true)
                    ),
                    mock(NodeClient.class)
                )
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertEquals("Tenant ID in header and body must match", exception.getMessage());
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
        builder.withPath(TimeSeriesAnalyticsPlugin.AD_BASE_INTERNAL_DETECTORS_URI + "/detector-1/_single_stream_result");
        builder.withParams(params);
        builder.withHeaders(new HashMap<>(headers));
        builder.withContent(new BytesArray(content), MediaTypeRegistry.JSON);
        return builder.build();
    }
}
