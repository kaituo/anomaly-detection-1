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

package org.opensearch.timeseries.util;

import java.net.InetAddress;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;

public class TransportUtilTests extends OpenSearchTestCase {

    public void testCreateDiscoveryNodeFromIpUsesIpPortIdWithoutLeadingSlash() {
        DiscoveryNode node = TransportUtil.createDiscoveryNodeFromIp("127.0.0.1", 9200);

        assertNotNull(node);
        assertEquals("127.0.0.1:9200", node.getId());
    }

    public void testCreateDiscoveryNodeFromIpPortAcceptsSlashPrefixedAddress() {
        DiscoveryNode node = TransportUtil.createDiscoveryNodeFromIpPort("/127.0.0.1:9200");

        assertNotNull(node);
        assertEquals("127.0.0.1:9200", node.getId());
    }

    public void testExtractIpStripsTransportAddressSlash() throws Exception {
        TransportAddress address = new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300);

        assertEquals("127.0.0.1", TransportUtil.extractIp(address));
    }
}
