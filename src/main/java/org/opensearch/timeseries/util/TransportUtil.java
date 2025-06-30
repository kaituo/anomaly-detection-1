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
     * 
     * @param ipAddress the IP address
     * @param port the port
     * @return the DiscoveryNode
     */
    public static DiscoveryNode createDiscoveryNodeFromIp(String ipAddress, int port) {
        try {
            TransportAddress address = new TransportAddress(InetAddress.getByName(ipAddress), port);
            // use ipAddress.toString() as id to avoid duplicate ids
            return new DiscoveryNode(address.toString(), address, Version.CURRENT);
        } catch (UnknownHostException e) {
            // local host might also be put into HashRing, but we only want to include the ip address from DDB
            LOG.debug("Invalid IP address in task list: {}", ipAddress, e);
            return null;
        }
    }

    /**
     * Create a DiscoveryNode from an IP address and port using empty id.
     * 
     * @param ipPort the IP address and port in the format "ip:port"
     * @return the DiscoveryNode
     */
    public static DiscoveryNode createDiscoveryNodeFromIpPort(String ipPort) {
        String[] ipPortParts = ipPort.split(":");
        if (ipPortParts.length != 2) {
            // it is possible the ecs task node id is not in the format "ip:port"
            LOG.debug("Invalid IP address and port in task list: {}", ipPort);
            return null;
        }
        String ipAddress = ipPortParts[0];
        int port = Integer.parseInt(ipPortParts[1]);
        return createDiscoveryNodeFromIp(ipAddress, port);
    }
}
