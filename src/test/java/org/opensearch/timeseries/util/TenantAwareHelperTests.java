/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.rest.handler.store.endpoint.DataSourceEndpointResolver;
import org.opensearch.timeseries.util.TenantAwareHelper.TenantComponents;

public class TenantAwareHelperTests extends OpenSearchTestCase {

    public void testGetTenantIDReturnsRawTenantHeader() {
        RestRequest request = requestWithHeaders(Map.of(CommonName.TENANT_ID_HEADER, List.of("account-1:application-1:workspace-1")));

        assertEquals("account-1:application-1:workspace-1", TenantAwareHelper.getTenantID(true, request));
    }

    public void testGetTenantIDIgnoresAosdHeaders() {
        Map<String, List<String>> headers = Map
            .of(
                CommonName.TENANT_ID_HEADER,
                List.of("account-1:application-1:workspace-1"),
                CommonName.AOSD_APPLICATION_ID_HEADER,
                List.of("application-1"),
                CommonName.AOSD_DATA_SOURCE_ID_HEADER,
                List.of("data-source-1")
            );
        RestRequest request = requestWithHeaders(headers);

        assertEquals("account-1:application-1:workspace-1", TenantAwareHelper.getTenantID(true, request));
    }

    public void testGetTenantIDRequiresTenantHeader() {
        RestRequest request = requestWithHeaders(Map.of(CommonName.AOSD_APPLICATION_ID_HEADER, List.of("application-1")));

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TenantAwareHelper.getTenantID(true, request)
        );

        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Tenant ID header is missing or has no value", exception.getMessage());
    }

    public void testGetTenantIDReturnsNullWhenMultiTenancyDisabled() {
        RestRequest request = requestWithHeaders(Map.of());

        assertNull(TenantAwareHelper.getTenantID(false, request));
    }

    public void testParseTenantIdReturnsAccountApplicationAndWorkspace() {
        TenantComponents tenantComponents = TenantAwareHelper.parseTenantId("account-1:application-1:workspace-1");

        assertEquals("account-1", tenantComponents.accountId());
        assertEquals("application-1", tenantComponents.applicationId());
        assertEquals("workspace-1", tenantComponents.workspaceId());
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

    public void testDataSourceEndpointResolverResolveDelegatesByApplicationAndDataSource() {
        DataSourceEndpointResolver resolver = (applicationId, dataSourceId) -> applicationId + "/" + dataSourceId;

        assertEquals("application-1/data-source-1", resolver.resolve("application-1", "data-source-1"));
    }

    private RestRequest requestWithHeaders(Map<String, List<String>> headers) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.GET);
        builder.withHeaders(new HashMap<>(headers));
        return builder.build();
    }
}
