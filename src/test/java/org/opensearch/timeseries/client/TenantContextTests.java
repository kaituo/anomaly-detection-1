/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.test.OpenSearchTestCase;

public class TenantContextTests extends OpenSearchTestCase {

    public void testGetTenantComponentsParsesTenantId() {
        TenantContext tenantContext = TenantContext.user("application-1:data-source-1");

        Pair<String, String> tenantComponents = tenantContext.getTenantComponents();

        assertNotNull(tenantComponents);
        assertEquals("application-1", tenantComponents.getKey());
        assertEquals("data-source-1", tenantComponents.getValue());
    }

    public void testGetTenantComponentsReturnsNullForSystemWideContext() {
        assertNull(TenantContext.systemWide().getTenantComponents());
    }
}
