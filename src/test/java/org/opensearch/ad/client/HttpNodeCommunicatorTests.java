/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import static org.mockito.Mockito.mock;

import org.apache.hc.core5.http.Header;
import org.opensearch.client.Request;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class HttpNodeCommunicatorTests extends OpenSearchTestCase {

    public void testApplyInternalRequestHeadersUsesConfiguredTenantRoutingHeaders() {
        Settings settings = Settings
            .builder()
            .put(TimeSeriesSettings.APPLICATION_ID_HEADER.getKey(), "x-custom-application-id")
            .put(TimeSeriesSettings.DATA_SOURCE_ID_HEADER.getKey(), "x-custom-data-source-id")
            .build();
        TestHttpNodeCommunicator communicator = new TestHttpNodeCommunicator(settings);

        Request request = new Request("GET", "/internal");
        communicator.applyHeaders(request, "application-1:data-source-1");

        assertEquals("secret", headerValue(request, CommonName.INTERNAL_API_TOKEN_HEADER));
        assertEquals("application-1:data-source-1", headerValue(request, CommonName.TENANT_ID_HEADER));
        assertEquals("application-1", headerValue(request, "x-custom-application-id"));
        assertEquals("data-source-1", headerValue(request, "x-custom-data-source-id"));
        assertNull(headerValue(request, CommonName.AOSD_APPLICATION_ID_HEADER));
        assertNull(headerValue(request, CommonName.AOSD_DATA_SOURCE_ID_HEADER));
    }

    private String headerValue(Request request, String name) {
        for (Header header : request.getOptions().getHeaders()) {
            if (name.equalsIgnoreCase(header.getName())) {
                return header.getValue();
            }
        }
        return null;
    }

    private static class TestHttpNodeCommunicator extends HttpNodeCommunicator {
        private TestHttpNodeCommunicator(Settings settings) {
            super("/rest", "/internal", "cluster", mock(HashRing.class), "secret", settings);
        }

        private void applyHeaders(Request request, String tenantId) {
            applyInternalRequestHeaders(request, tenantId);
        }

        @Override
        protected String buildProfilePath(String configId, String typeStr) {
            return "/profile";
        }
    }
}
