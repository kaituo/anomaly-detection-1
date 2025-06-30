/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.util.TenantAwareHelper.TenantComponents;

public class TenantContextTests extends OpenSearchTestCase {

    public void testGetTenantComponentsParsesTenantId() {
        TenantContext tenantContext = TenantContext.user("account-1:application-1:workspace-1");

        TenantComponents tenantComponents = tenantContext.getTenantComponents();

        assertNotNull(tenantComponents);
        assertEquals("account-1", tenantComponents.accountId());
        assertEquals("application-1", tenantComponents.applicationId());
        assertEquals("workspace-1", tenantComponents.workspaceId());
    }

    public void testGetTenantComponentsReturnsNullForSystemWideContext() {
        assertNull(TenantContext.systemWide().getTenantComponents());
    }
}
