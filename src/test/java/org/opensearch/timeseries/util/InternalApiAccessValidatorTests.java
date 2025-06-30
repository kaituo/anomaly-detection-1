/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class InternalApiAccessValidatorTests extends OpenSearchTestCase {

    private static final Setting<Boolean> MULTI_TENANCY_ENABLED = Setting
        .boolSetting("plugins.timeseries.test.multi_tenancy.enabled", false, Setting.Property.NodeScope);

    public void testRejectsWhenMultiTenancyIsDisabled() {
        RestRequest request = requestWithHeaders(Map.of(CommonName.INTERNAL_API_TOKEN_HEADER, List.of("secret")));
        Settings settings = Settings.builder().put(MULTI_TENANCY_ENABLED.getKey(), false).build();

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> InternalApiAccessValidator.validateInternalMultiTenantRequest(request, settings, MULTI_TENANCY_ENABLED)
        );

        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("This API is only available in multi-tenancy mode", exception.getMessage());
    }

    public void testRejectsWhenSharedSecretIsMissingOrBlank() {
        RestRequest request = requestWithHeaders(Map.of(CommonName.INTERNAL_API_TOKEN_HEADER, List.of("secret")));

        IllegalStateException missingSecret = expectThrows(
            IllegalStateException.class,
            () -> InternalApiAccessValidator
                .validateInternalMultiTenantRequest(
                    request,
                    Settings.builder().put(MULTI_TENANCY_ENABLED.getKey(), true).build(),
                    MULTI_TENANCY_ENABLED
                )
        );
        assertTrue(missingSecret.getMessage().contains(TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey()));

        IllegalStateException blankSecret = expectThrows(
            IllegalStateException.class,
            () -> InternalApiAccessValidator
                .validateInternalMultiTenantRequest(
                    request,
                    Settings
                        .builder()
                        .put(MULTI_TENANCY_ENABLED.getKey(), true)
                        .put(TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey(), " ")
                        .build(),
                    MULTI_TENANCY_ENABLED
                )
        );
        assertTrue(blankSecret.getMessage().contains(TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey()));
    }

    private RestRequest requestWithHeaders(Map<String, List<String>> headers) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry());
        builder.withMethod(RestRequest.Method.POST);
        builder.withPath("/_plugins/_anomaly_detection/test");
        builder.withHeaders(new HashMap<>(headers));
        return builder.build();
    }
}
