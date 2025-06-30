/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/**
 * Validates access to internal multi-tenant HTTP APIs.
 */
public final class InternalApiAccessValidator {

    private InternalApiAccessValidator() {}

    public static void validateInternalMultiTenantRequest(
        RestRequest request,
        Settings settings,
        Setting<Boolean> multiTenancyEnabledSetting
    ) {
        if (multiTenancyEnabledSetting.get(settings) == false) {
            throw new OpenSearchStatusException("This API is only available in multi-tenancy mode", RestStatus.FORBIDDEN);
        }

        String expectedToken = TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.get(settings);
        if (expectedToken == null || expectedToken.isBlank()) {
            throw new IllegalStateException(
                TimeSeriesSettings.INTERNAL_API_SHARED_SECRET.getKey() + " must be configured for internal multi-tenant HTTP APIs"
            );
        }

        String actualToken = request.header(CommonName.INTERNAL_API_TOKEN_HEADER);
        if (expectedToken.equals(actualToken) == false) {
            throw new OpenSearchStatusException("Missing or invalid internal API token", RestStatus.FORBIDDEN);
        }
    }
}
