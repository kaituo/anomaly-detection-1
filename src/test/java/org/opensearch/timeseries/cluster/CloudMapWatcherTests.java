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

package org.opensearch.timeseries.cluster;

import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

import software.amazon.awssdk.services.servicediscovery.model.DiscoverInstancesRequest;
import software.amazon.awssdk.services.servicediscovery.model.DiscoverInstancesResponse;
import software.amazon.awssdk.services.servicediscovery.model.HealthStatusFilter;
import software.amazon.awssdk.services.servicediscovery.model.HttpInstanceSummary;

public class CloudMapWatcherTests extends OpenSearchTestCase {

    public void testExtractIpv4AddressesFiltersDeduplicatesAndSorts() {
        DiscoverInstancesResponse response = DiscoverInstancesResponse
            .builder()
            .instances(
                instance("10.0.0.2"),
                instance(""),
                instance("10.0.0.1"),
                HttpInstanceSummary.builder().attributes(Map.of()).build(),
                instance("10.0.0.2")
            )
            .build();

        assertEquals(List.of("10.0.0.1", "10.0.0.2"), CloudMapWatcher.extractIpv4Addresses(response));
    }

    public void testNextRevisionIdAdvancesPastLargestKnownRevision() {
        assertEquals(101L, CloudMapWatcher.nextRevisionId(10L, 100L, 50L));
        assertEquals(201L, CloudMapWatcher.nextRevisionId(10L, 100L, 200L));
        assertEquals(301L, CloudMapWatcher.nextRevisionId(300L, 100L, 200L));
    }

    public void testNextRevisionIdRejectsOverflow() {
        expectThrows(IllegalStateException.class, () -> CloudMapWatcher.nextRevisionId(Long.MAX_VALUE, 100L, 200L));
    }

    public void testBuildDiscoverInstancesRequestDoesNotFilterHealthByDefault() {
        DiscoverInstancesRequest request = CloudMapWatcher.buildDiscoverInstancesRequest("namespace", "service", false);

        assertEquals("namespace", request.namespaceName());
        assertEquals("service", request.serviceName());
        assertNull(request.healthStatus());
    }

    public void testBuildDiscoverInstancesRequestCanFilterHealthyInstances() {
        DiscoverInstancesRequest request = CloudMapWatcher.buildDiscoverInstancesRequest("namespace", "service", true);

        assertEquals(HealthStatusFilter.HEALTHY, request.healthStatus());
    }

    private static HttpInstanceSummary instance(String ipAddress) {
        return HttpInstanceSummary.builder().attributes(Map.of("AWS_INSTANCE_IPV4", ipAddress)).build();
    }
}
