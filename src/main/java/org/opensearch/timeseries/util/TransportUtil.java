/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

 package org.opensearch.timeseries.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.transport.TransportAddress;

public class TransportUtil {
    private static final Logger LOG = LogManager.getLogger(TransportUtil.class);

    /**
     * Create a DiscoveryNode from an IP address using empty id and port 9200.
     */
    public static DiscoveryNode createDiscoveryNodeFromIp(String ipAddress) {
        try {
            TransportAddress address = new TransportAddress(InetAddress.getByName(ipAddress), 9200);
            // use address.toString() as id to avoid duplicate ids
            return new DiscoveryNode(address.toString(), address, Version.CURRENT);
        } catch (UnknownHostException e) {
            LOG.warn("Invalid IP address in task list: {}", ipAddress, e);
            return null;
        }
    }
}