/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;

public class TenantAwareHelperTests extends OpenSearchTestCase {

    public void testParseTenantIdReturnsApplicationIdAndDataSourceId() {
        Pair<String, String> tenantComponents = TenantAwareHelper.parseTenantId("application-1:data-source-1");

        assertEquals("application-1", tenantComponents.getKey());
        assertEquals("data-source-1", tenantComponents.getValue());
    }

    public void testParseTenantIdRejectsNullTenantId() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TenantAwareHelper.parseTenantId(null));

        assertEquals("Tenant id cannot be null", exception.getMessage());
    }

    public void testParseTenantIdRejectsInvalidTenantIdFormat() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TenantAwareHelper.parseTenantId("application-1")
        );

        assertEquals("Invalid tenant id format: application-1", exception.getMessage());
    }

    public void testDataSourceEndpointResolverResolveParsesTenantId() {
        DataSourceEndpointResolver resolver = (applicationId, dataSourceId) -> applicationId + "/" + dataSourceId;

        assertEquals("application-1/data-source-1", resolver.resolve("application-1:data-source-1"));
    }
}
