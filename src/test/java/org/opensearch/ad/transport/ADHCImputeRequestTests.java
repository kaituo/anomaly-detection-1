/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.Collections;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ADHCImputeRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTripPreservesConcreteNodes() throws IOException {
        DiscoveryNode firstNode = new DiscoveryNode(
            "node-1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        DiscoveryNode secondNode = new DiscoveryNode(
            "node-2",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        ADHCImputeRequest originalRequest = new ADHCImputeRequest("detector-1", "tenant-a", "task-1", 123L, 456L, firstNode, secondNode);

        ADHCImputeRequest deserializedRequest = copy(originalRequest);

        assertEquals("detector-1", deserializedRequest.getConfigId());
        assertEquals("tenant-a", deserializedRequest.getTenantId());
        assertEquals("task-1", deserializedRequest.getTaskId());
        assertEquals(123L, deserializedRequest.getDataStartMillis());
        assertEquals(456L, deserializedRequest.getDataEndMillis());
        assertNotNull(deserializedRequest.nodesIds());
        assertEquals(0, deserializedRequest.nodesIds().length);
        assertNotNull(deserializedRequest.concreteNodes());
        assertEquals(2, deserializedRequest.concreteNodes().length);
        assertEquals(firstNode, deserializedRequest.concreteNodes()[0]);
        assertEquals(secondNode, deserializedRequest.concreteNodes()[1]);
    }

    public void testSerializationRoundTripPreservesNullOptionalFields() throws IOException {
        ADHCImputeRequest originalRequest = new ADHCImputeRequest("detector-2", null, null, 789L, 101112L);

        ADHCImputeRequest deserializedRequest = copy(originalRequest);

        assertEquals("detector-2", deserializedRequest.getConfigId());
        assertNull(deserializedRequest.getTenantId());
        assertNull(deserializedRequest.getTaskId());
        assertEquals(789L, deserializedRequest.getDataStartMillis());
        assertEquals(101112L, deserializedRequest.getDataEndMillis());
        assertNotNull(deserializedRequest.nodesIds());
        assertEquals(0, deserializedRequest.nodesIds().length);
        assertNotNull(deserializedRequest.concreteNodes());
        assertEquals(0, deserializedRequest.concreteNodes().length);
    }

    private ADHCImputeRequest copy(ADHCImputeRequest request) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        return new ADHCImputeRequest(in);
    }
}
