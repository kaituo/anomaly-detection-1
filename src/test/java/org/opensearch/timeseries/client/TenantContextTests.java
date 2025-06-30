/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.Config;
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

    public void testUserContextFromConfigCarriesConfigId() {
        Config config = mock(Config.class);
        when(config.getId()).thenReturn("config-1");
        when(config.getTenantId()).thenReturn("account-1:application-1:workspace-1");
        when(config.getDataSourceId()).thenReturn("data-source-1");

        TenantContext tenantContext = TenantContext.user(config);

        assertEquals("config-1", tenantContext.getConfigId());
    }
}
