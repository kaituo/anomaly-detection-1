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

import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;
import static org.mockito.Mockito.mock;

import java.util.Map;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

public class RestADSingleStreamResultActionTests extends OpenSearchTestCase {

    public void testPrepareRequestAllowsMissingTenant() throws Exception {
        RestADSingleStreamResultAction action = new RestADSingleStreamResultAction();

        assertNotNull(action.prepareRequest(createRequest("{\"model_id\":\"model-1\",\"start\":1,\"end\":2,\"value_list\":[1.0]}"), mock(NodeClient.class)));
    }

    public void testPrepareRequestRequiresDetectorId() {
        RestADSingleStreamResultAction action = new RestADSingleStreamResultAction();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(createRequest("{\"model_id\":\"model-1\",\"start\":1,\"end\":2,\"value_list\":[1.0]}", Map.of()), mock(NodeClient.class))
        );

        assertEquals("Missing required parameter: " + DETECTOR_ID, exception.getMessage());
    }

    public void testPrepareRequestAcceptsTenantFromBody() throws Exception {
        RestADSingleStreamResultAction action = new RestADSingleStreamResultAction();

        assertNotNull(
            action.prepareRequest(
                createRequest("{\"model_id\":\"model-1\",\"start\":1,\"end\":2,\"value_list\":[1.0],\"tenant_id\":\"tenant-a\"}"),
                mock(NodeClient.class)
            )
        );
    }

    private FakeRestRequest createRequest(String content) {
        return createRequest(content, Map.of(DETECTOR_ID, "detector-1"));
    }

    private FakeRestRequest createRequest(String content, Map<String, String> params) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.POST);
        builder.withPath("/_plugins/_anomaly_detection/detectors/detector-1/_single_stream_result");
        builder.withParams(params);
        builder.withContent(new BytesArray(content), MediaTypeRegistry.JSON);
        return builder.build();
    }
}
