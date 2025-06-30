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
     * Extract the IP portion from a TransportAddress, stripping the leading slash
     * and port so that addresses on different ports (e.g. transport 9300 vs HTTP 9200)
     * can be compared by IP alone.
     *
     * @param address transport address whose toString() looks like {@code /10.0.1.23:9300}
     * @return IP string, e.g. {@code /10.0.1.23}, or empty string if null
     */
    public static String extractIp(TransportAddress address) {
        if (address == null) {
            return "";
        }
        // TransportAddress.toString() → "/10.0.1.23:9300"  or  "10.0.1.23:9300"
        // Split on ":" to drop the port, keep the (possibly slash-prefixed) IP.
        return address.toString().split(":")[0];
    }

    /**
     * Create a DiscoveryNode from an IP address using empty id and port 9200.
     * 
     * @param ipAddress the IP address
     * @param port the port
     * @return the DiscoveryNode like DiscoveryNode(id="/10.0.1.23:9200", address="/10.0.1.23:9200").
     *  The slash is a Java InetAddress.toString() artifact.
     *  InetAddress.getByName("10.0.1.23") creates an InetAddress whose toString() returns hostname/literal_IP.
     *  Since a raw IP string like "10.0.1.23" has no hostname, Java formats it as /10.0.1.23
     *  (empty hostname + "/" + IP). TransportAddress.toString() then appends the port, producing /10.0.1.23:9200.
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
